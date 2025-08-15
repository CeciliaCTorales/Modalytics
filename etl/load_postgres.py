"""
ModaLytics — Carga a PostgreSQL (cloud)
- Carga dimensiones (Date, Channel, Article, Customer) y hechos (Transactions)
- Usa variables de entorno (.env): DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, SSL_MODE
- Lee los archivos del sample en ./data/sample
"""
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from dotenv import load_dotenv

# ========= 1) Credenciales =========
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
SSL_MODE = os.getenv("SSL_MODE", "require")

# ========= 2) Conexión =========
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
    user=DB_USER, password=DB_PASS, sslmode=SSL_MODE
)
cur = conn.cursor()

# ========= 3) DDL =========
ddl_sql = Path("sql/ddl_postgres.sql").read_text(encoding="utf-8")

print("🗑️ Borrando tablas existentes...")
cur.execute("""
DROP TABLE IF EXISTS ml.f_transactions CASCADE;
DROP TABLE IF EXISTS ml.d_date CASCADE;
DROP TABLE IF EXISTS ml.d_article CASCADE;
DROP TABLE IF EXISTS ml.d_customer CASCADE;
DROP TABLE IF EXISTS ml.d_channel CASCADE;
""")
conn.commit()

print("📜 Creando tablas (esquema ml)...")
cur.execute(ddl_sql)
conn.commit()

# ========= 4) Cargar Dimensiones =========
sample_dir = Path("data/sample")

# 4.1 D_Channel (sin CSV)
print("📦 Cargando D_Channel (Store/Online)...")
cur.execute("INSERT INTO ml.d_channel (channelkey, sales_channel_id, channelname) VALUES (1,1,'Store'),(2,2,'Online');")
conn.commit()

# 4.2 D_Date (desde CSV generado)
print("📦 Cargando D_Date...")
d_date = pd.read_csv(sample_dir / "d_date.csv")
cols_date = ["datekey","fulldate","year","month","monthname","week","day"]
d_date = d_date[cols_date]
execute_values(
    cur,
    "INSERT INTO ml.d_date (datekey, fulldate, year, month, monthname, week, day) VALUES %s",
    list(d_date.itertuples(index=False, name=None))
)
conn.commit()

# 4.3 D_Article (desde Parquet → generar surrogate key)
print("📦 Cargando D_Article...")
art = pd.read_parquet(sample_dir / "articles_filtered.parquet")
cols_art = [
    "article_id","product_code","product_type_no","product_group_name",
    "graphical_appearance_no","colour_group_name","garment_group_name"
]
art = art[cols_art].drop_duplicates().reset_index(drop=True)
art.insert(0, "articlekey", art.index + 1)  # surrogate key

execute_values(
    cur,
    """INSERT INTO ml.d_article
       (articlekey, article_id, product_code, product_type_no, product_group_name,
        graphical_appearance_no, colour_group_name, garment_group_name)
       VALUES %s""",
    list(art.itertuples(index=False, name=None))
)
conn.commit()

# 4.4 D_Customer (desde CSV → generar surrogate key)
print("📦 Cargando D_Customer...")
cus = pd.read_csv(sample_dir / "customers_filtered.csv", dtype={"customer_id":"string"})
cols_cus = ["customer_id","age","postal_code"]
cus = cus[cols_cus].drop_duplicates().reset_index(drop=True)
cus.insert(0, "customerkey", cus.index + 1)  # surrogate key

execute_values(
    cur,
    "INSERT INTO ml.d_customer (customerkey, customer_id, age, postal_code) VALUES %s",
    list(cus.itertuples(index=False, name=None))
)
conn.commit()

# ========= 5) Mapas (business -> surrogate) =========
print("🔑 Construyendo mapas de keys...")
# Artículos
cur.execute("SELECT articlekey, article_id FROM ml.d_article;")
a_map = {row[1]: row[0] for row in cur.fetchall()}
# Clientes
cur.execute("SELECT customerkey, customer_id FROM ml.d_customer;")
c_map = {row[1]: row[0] for row in cur.fetchall()}
# Canal
cur.execute("SELECT channelkey, sales_channel_id FROM ml.d_channel;")
ch_map = {row[1]: row[0] for row in cur.fetchall()}

# ========= 6) Cargar Hechos por archivo =========
print("📦 Cargando F_Transactions (archivo por archivo)...")
txn_files = sorted(sample_dir.glob("transactions_sample_month=*.parquet"))

insert_sql = """
INSERT INTO ml.f_transactions (datekey, articlekey, customerkey, channelkey, price, quantity)
VALUES %s
"""

rows_total = 0
for p in txn_files:
    tx = pd.read_parquet(p)

    # datekey YYYYMMDD
    datekey = pd.to_datetime(tx["t_dat"]).dt.strftime("%Y%m%d").astype(int)

    # map a surrogate keys
    articlekey = tx["article_id"].map(a_map)
    customerkey = tx["customer_id"].map(c_map)
    channelkey = tx["sales_channel_id"].map(ch_map)

    df = pd.DataFrame({
        "datekey": datekey,
        "articlekey": articlekey,
        "customerkey": customerkey,
        "channelkey": channelkey,
        "price": tx["price"].astype(float),
        "quantity": 1
    }).dropna()  # si falta key, se descarta

    if not df.empty:
        execute_values(cur, insert_sql, list(df.itertuples(index=False, name=None)))
        conn.commit()
        rows_total += len(df)
        print(f"  ✓ {p.name}: {len(df):,} filas insertadas")
    else:
        print(f"  ⚠ {p.name}: 0 filas (después de mapear keys)")

print(f"✅ Hechos cargados: {rows_total:,} filas")

# ========= 7) Conteos finales =========
for tbl in ["ml.d_channel","ml.d_date","ml.d_article","ml.d_customer","ml.f_transactions"]:
    cur.execute(f"SELECT COUNT(*) FROM {tbl}")
    print(f"📊 {tbl}: {cur.fetchone()[0]:,} filas")

cur.close()
conn.close()
print("🎉 Listo")
