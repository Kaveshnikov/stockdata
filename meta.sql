CREATE TABLE stock(
  id SERIAL CONSTRAINT pk_stock PRIMARY KEY,
  name VARCHAR CONSTRAINT uq_stock_name UNIQUE
);

CREATE TABLE trade(
  id SERIAL CONSTRAINT pk_trade PRIMARY KEY,
  stock INTEGER CONSTRAINT fk_trade_stock REFERENCES stock (id),
  insider VARCHAR NOT NULL,
  relation VARCHAR,
  last_date DATE,
  transaction VARCHAR,
  owner_type VARCHAR,
  shares_traded BIGINT NOT NULL,
  last_price DECIMAL(8, 4) NOT NULL,
  shares_held BIGINT NOT NULL
);

CREATE TABLE price(
  stock INTEGER CONSTRAINT fk_price_stock REFERENCES stock (id),
  date DATE NOT NULL CONSTRAINT uq_price_date UNIQUE,
  open DECIMAL(8, 4) NOT NULL,
  high DECIMAL(8, 4) NOT NULL,
  low DECIMAL(8, 4) NOT NULL,
  close DECIMAL(8, 4) NOT NULL,
  volume BIGINT NOT NULL
);

