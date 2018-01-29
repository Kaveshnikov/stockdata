import aiohttp
import argparse
import async_timeout
import asyncio
import os
import logging
import asyncpg
from multiprocessing import Pool
from datetime import datetime
from lxml.html import fromstring, HtmlElement
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


async def insert_stock_data(stock: str, prices: List[Price]) -> int:
    conn = await asyncpg.connect(
        host='localhost',
        user='stockdata',
        password='stockdata',
        database='stockdata',
        timeout=30
    )

    async with conn.transaction():
        await conn.execute("insert into stock(name) values ($1)", stock)

        stock_id = await conn.fetchval("select id from stock where name=$1", stock)
        await conn.executemany(
            "insert into price values ($1, $2, $3, $4, $5, $6, $7)",
            [(stock_id, *price) for price in prices]
        )
        return stock_id


async def get_prices(stock: str) -> List[Price]:
    async with aiohttp.ClientSession() as session:
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


async def work_async(stock: str):
    prices = await get_prices(stock)
    await insert_stock_data(stock, prices)



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

    # pool = Pool(args.workers)
    # pool.map(work, stocks)
    work(stocks[0])



if __name__ == '__main__':
    main()