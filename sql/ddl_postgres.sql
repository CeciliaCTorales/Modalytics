CREATE SCHEMA IF NOT EXISTS ml;

CREATE TABLE ml.d_date (
    datekey INTEGER PRIMARY KEY,
    fulldate DATE NOT NULL,
    year INTEGER,
    month INTEGER,
    monthname TEXT,
    week INTEGER,
    day INTEGER
);

CREATE TABLE ml.d_article (
    articlekey INTEGER PRIMARY KEY,
    article_id BIGINT NOT NULL,
    product_code INTEGER,
    product_type_no INTEGER,
    product_group_name TEXT,
    graphical_appearance_no INTEGER,
    colour_group_name TEXT,
    garment_group_name TEXT
);

CREATE TABLE ml.d_customer (
    customerkey INTEGER PRIMARY KEY,
    customer_id TEXT NOT NULL,
    age INTEGER,
    postal_code TEXT
);

CREATE TABLE ml.d_channel (
    channelkey INTEGER PRIMARY KEY,
    sales_channel_id INTEGER,
    channelname TEXT
);

CREATE TABLE ml.f_transactions (
    datekey INTEGER REFERENCES ml.d_date(datekey),
    articlekey INTEGER REFERENCES ml.d_article(articlekey),
    customerkey INTEGER REFERENCES ml.d_customer(customerkey),
    channelkey INTEGER REFERENCES ml.d_channel(channelkey),
    price NUMERIC(10,2),
    quantity INTEGER
);
