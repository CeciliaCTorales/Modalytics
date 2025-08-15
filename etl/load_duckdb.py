"""
ModaLytics — Carga local a DuckDB
Genera modalytics.duckdb con el modelo estrella y datos desde ./data/sample
"""
import duckdb, pandas as pd
from pathlib import Path

DB = Path("./modalytics.duckdb").resolve()
SAMPLEDIR = Path("./data/sample").resolve()
ART = SAMPLEDIR / "articles_filtered.parquet"
CUS = SAMPLEDIR / "customers_filtered.csv"
TXN_FILES = sorted(SAMPLEDIR.glob("transactions_sample_month=*.parquet"))

con = duckdb.connect(str(DB))
con.execute("CREATE SCHEMA IF NOT EXISTS ml;")

con.execute("""CREATE TABLE IF NOT EXISTS ml.d_date (
  date_key   INTEGER PRIMARY KEY,
  full_date  DATE NOT NULL,
  year       INTEGER NOT NULL,
  month      INTEGER NOT NULL,
  month_name VARCHAR,
  week       INTEGER NOT NULL,
  day        INTEGER NOT NULL
);
""")

con.execute("""CREATE TABLE IF NOT EXISTS ml.d_channel (
  channel_key INTEGER PRIMARY KEY,
  sales_channel_id INTEGER UNIQUE NOT NULL,
  channel_name VARCHAR NOT NULL
);
""")

con.execute("""CREATE TABLE IF NOT EXISTS ml.d_article (
  article_key INTEGER PRIMARY KEY,
  article_id  BIGINT UNIQUE NOT NULL,
  product_code INTEGER,
  product_type_no INTEGER,
  product_group_name VARCHAR,
  graphical_appearance_no INTEGER,
  colour_group_name VARCHAR,
  garment_group_name VARCHAR
);
""")

con.execute("""CREATE TABLE IF NOT EXISTS ml.d_customer (
  customer_key INTEGER PRIMARY KEY,
  customer_id  VARCHAR UNIQUE NOT NULL,
  age          INTEGER,
  postal_code  VARCHAR
);
""")

con.execute("""CREATE TABLE IF NOT EXISTS ml.f_transactions (
  date_key     INTEGER NOT NULL REFERENCES ml.d_date(date_key),
  article_key  INTEGER NOT NULL REFERENCES ml.d_article(article_key),
  customer_key INTEGER NOT NULL REFERENCES ml.d_customer(customer_key),
  channel_key  INTEGER NOT NULL REFERENCES ml.d_channel(channel_key),
  price        DOUBLE NOT NULL,
  quantity     INTEGER NOT NULL
);
""")

# Load dims
con.execute("DELETE FROM ml.d_channel;")
con.execute("INSERT INTO ml.d_channel VALUES (1,1,'Store'),(2,2,'Online');")

d = pd.date_range("2020-03-01","2020-09-01",freq="D")
dfd = pd.DataFrame({
    "date_key": d.strftime("%Y%m%d").astype(int),
    "full_date": d,
    "year": d.year, "month": d.month, "month_name": d.strftime("%B"),
    "week": d.isocalendar().week.astype(int), "day": d.day
})
con.register("dfd", dfd)
con.execute("DELETE FROM ml.d_date;")
con.execute("INSERT INTO ml.d_date SELECT * FROM dfd;")

df_art = pd.read_parquet(ART)[[
    "article_id","product_code","product_type_no","product_group_name",
    "graphical_appearance_no","colour_group_name","garment_group_name"
]].drop_duplicates().reset_index(drop=True)
df_art["article_key"] = df_art.index + 1
con.register("df_art", df_art)
con.execute("DELETE FROM ml.d_article;")
con.execute("""INSERT INTO ml.d_article(article_key,article_id,product_code,product_type_no,product_group_name,
                         graphical_appearance_no,colour_group_name,garment_group_name)
SELECT article_key,article_id,product_code,product_type_no,product_group_name,
       graphical_appearance_no,colour_group_name,garment_group_name FROM df_art;
""")

df_cus = pd.read_csv(CUS, dtype={"customer_id":"string"})[[
    "customer_id","age","postal_code"
]].drop_duplicates().reset_index(drop=True)
df_cus["customer_key"] = df_cus.index + 1
con.register("df_cus", df_cus)
con.execute("DELETE FROM ml.d_customer;")
con.execute("INSERT INTO ml.d_customer(customer_key,customer_id,age,postal_code) SELECT customer_key,customer_id,age,postal_code FROM df_cus;")

con.execute("CREATE OR REPLACE TEMP TABLE map_article AS SELECT article_id, article_key FROM ml.d_article;")
con.execute("CREATE OR REPLACE TEMP TABLE map_customer AS SELECT customer_id, customer_key FROM ml.d_customer;")
con.execute("CREATE OR REPLACE TEMP TABLE map_channel  AS SELECT sales_channel_id, channel_key FROM ml.d_channel;")

con.execute("DELETE FROM ml.f_transactions;")
for f in TXN_FILES:
    con.execute(f"""        INSERT INTO ml.f_transactions(date_key,article_key,customer_key,channel_key,price,quantity)
        SELECT
          CAST(strftime(t_dat, '%Y%m%d') AS INTEGER) AS date_key,
          mA.article_key, mC.customer_key, mCh.channel_key,
          price, 1
        FROM read_parquet('{str(f).replace("'","''")}') t
        LEFT JOIN map_article mA ON t.article_id = mA.article_id
        LEFT JOIN map_customer mC ON t.customer_id = mC.customer_id
        LEFT JOIN map_channel  mCh ON t.sales_channel_id = mCh.sales_channel_id
        WHERE mA.article_key IS NOT NULL AND mC.customer_key IS NOT NULL AND mCh.channel_key IS NOT NULL;
    """)
    print("Loaded:", f.name)

print("OK: modalytics.duckdb created at", DB)
con.close()
