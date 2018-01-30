import aiohttp
import argparse
import async_timeout
import asyncio
import os
import logging
import asyncpg
from multiprocessing import Pool
from datetime import datetime
from lxml.html import fromstring
from typing import List
from collections import namedtuple

BASE = os.path.dirname(os.path.abspath(__file__))
loger = logging.getLogger(__file__)

Price = namedtuple('Price', ['date', 'open', 'high', 'low', 'close', 'volume'])
Trade = namedtuple(
    'Trade',
    [
        'insider',
        'relation',
        'last_date',
        'transaction',
        'owner_type',
        'shares_traded',
        'last_price',
        'shares_held'
    ]
)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='stockdata html parser',
        usage='parser [options]',
        description="Parse nasdaq.com for stocks' data"
    )
    parser.add_argument('-N', '--workers', type=int, help='number of workers', default=1)
    parser.add_argument(
        '-f',
        '--file',
        help="path to the file with stocks' names",
        default=os.path.join(BASE, 'tickers.txt')
    )

    return parser.parse_args()


async def fetch(session: aiohttp.ClientSession, url: str, timeout: float = 10):
    try:
        with async_timeout.timeout(timeout):
            async with session.get(url) as response:
                return await response.text()
    except TimeoutError:
        loger.error('Timeout exceeded. Resource {}'.format(url))
        raise


async def insert_stock_data(conn: asyncpg.Connection, stock: str, prices: List[Price]) -> int:
    async with conn.transaction():
        await conn.execute("insert into stock(name) values ($1)", stock)

        stock_id = await conn.fetchval("select id from stock where name=$1", stock)
        await conn.executemany(
            "insert into price values ($1, $2, $3, $4, $5, $6, $7)",
            [(stock_id, *price) for price in prices]
        )

        return stock_id


async def get_prices(session: aiohttp.ClientSession, stock: str) -> List[Price]:
    html = await fetch(session, 'http://www.nasdaq.com/symbol/{}/historical'.format(stock))
    doc = fromstring(html)

    # quotes_content_left_pnlAJAX - название контейнера, в котором хранится таблица
    div = doc.get_element_by_id('quotes_content_left_pnlAJAX', None)
    rows = div.find('table/tbody').getchildren()
    prices = []

    # Первую строку (т. е. сегодня) не берем, так как в ней могут быть не конечные данные,
    # и в выходные дни она будет просто пустой
    for row in rows[1:]:
        columns = [column.text.strip() for column in row.iterchildren()]
        prices.append(Price(
            # Здесь переходим к форме записи даты без разделителей,
            # чтобы не зависеть от локали
            date=datetime.strptime(columns[0].replace('/', ''), '%m%d%Y'),
            open=float(columns[1].replace(',', '')),
            high=float(columns[2].replace(',', '')),
            low=float(columns[3].replace(',', '')),
            close=float(columns[4].replace(',', '')),
            volume=int(columns[5].replace(',', ''))
        ))

    return prices


async def get_trades(session: aiohttp.ClientSession, url: str) -> List[Trade]:
    trades = []
    past_pages = 0
    next_page = fromstring('<a href="{}"></a>'.format(url))  # Данный сурогат использован как заглушка для while

    # Здесь решено отказаться от рекурсии, как задел на будущее, если придется обрабатывать много страниц
    while past_pages < 10 and next_page.tag == 'a':
        html = await fetch(session, next_page.get('href'))
        doc = fromstring(html)

        # Здесь реализован поиск по классу, т. к. в разметке у ближайших контейнеров к таблице нет id,
        # а к данному класу пренадлежит только таблица
        table = doc.find_class('certain-width')[0]
        # Шапку таблицы не берем
        rows = table.getchildren()[1:]

        for row in rows:
            # Используем именно text_content() потому, что в первом столбце лежит ссылка
            columns = [column.text_content().strip() for column in row.iterchildren()]
            trades.append(Trade(
                insider=columns[0],
                relation=columns[1] if columns[1] else None,
                last_date=datetime.strptime(columns[2].replace('/', ''), '%m%d%Y') if columns[2] else None,
                transaction=columns[3] if columns[3] else None,
                owner_type=columns[4] if columns[4] else None,
                shares_traded=int(columns[5].replace(',', '')),
                last_price=float(columns[6].replace(',', '')) if columns[6] else None,
                shares_held=int(columns[7].replace(',', ''))
            ))

        # quotes_content_left_lb_NextPage - либо ссылка на следующую страницу, либо <span>, если это последняя
        next_page = doc.get_element_by_id('quotes_content_left_lb_NextPage')
        past_pages += 1

    return trades


async def insert_trades(conn: asyncpg.Connection, trades: List[Trade], stock_id: int):
    async with conn.transaction():
        await conn.executemany(
            '''insert into trade(stock, insider, relation, last_date, transaction,
                  owner_type, shares_traded, last_price, shares_held)
                  values ($1, $2, $3, $4, $5, $6, $7, $8, $9)''',
            [(stock_id, *trade) for trade in trades]
        )


async def work_async(stock: str):
    async with aiohttp.ClientSession() as session:
        prices = await get_prices(session, stock)
        trades = await get_trades(session, 'http://www.nasdaq.com/symbol/{}/insider-trades'.format(stock))
        conn = await asyncpg.connect(
            host='localhost',
            user='stockdata',
            password='stockdata',
            database='stockdata',
            timeout=10
        )

        stock_id = await insert_stock_data(conn, stock, prices)
        await insert_trades(conn, trades, stock_id)
        await conn.close(timeout=10)


async def clear_db():
    # Так как нужно как-то обрабатывать ситуацию, когда парсер запускается с имеющимися в БД данными
    # решено просто удалять старые. Если оставить старые записи, то не получается бороться с дубликатами
    # в таблице trades, так как владелец теоретически мог совершить две абсолютно одинаковые финансовые
    # операции.

    conn = await asyncpg.connect(
        host='localhost',
        user='stockdata',
        password='stockdata',
        database='stockdata',
        timeout=10
    )

    async with conn.transaction():
        await conn.execute("delete from price; delete from trade; delete from stock;")


def work(stock: str):
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(work_async(stock))
    except TimeoutError:
        return


def main():
    args = get_args()

    try:
        with open(args.file, 'r') as file:
            stocks = [line.strip() for line in file.readlines() if line.strip()]
    except FileNotFoundError:
        loger.error('File not found')
        exit(1)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(clear_db())

    pool = Pool(args.workers)
    pool.map(work, stocks)


if __name__ == '__main__':
    main()