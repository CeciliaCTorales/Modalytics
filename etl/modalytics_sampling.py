#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ModaLytics - Hito 1: Muestreo/recorte del dataset H&M (Kaggle)
Autor/a: Cecilia Torales (proyecto ModaLytics)
Descripción:
    - Modo A: recorte por rango de fechas [--start --end]
    - Modo B: muestreo aleatorio por fracción [--frac]
    - Mantiene integridad referencial: filtra articles/customers según las transacciones retenidas.
    - Trabaja por chunks (sin cargar todo a RAM).
    - Salida en Parquet (particionado por mes) o CSV.

Ejemplos de uso:
    # Recorte por fechas (recomendado)
    python etl/modalytics_sampling.py --data-dir "./data/raw" --out-dir "./data/sample" --start 2020-03-01 --end 2020-09-01

    # Muestreo 10% estratificado por mes
    python etl/modalytics_sampling.py --data-dir "./data/raw" --out-dir "./data/sample" --frac 0.10

    # Sin parámetros → últimos 6 meses del rango detectado
    python etl/modalytics_sampling.py --data-dir "./data/raw" --out-dir "./data/sample"

Requisitos:
    pip install pandas pyarrow tqdm
"""
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **kwargs):  # tqdm opcional
        return x

# ----------------------------
# Config por defecto
# ----------------------------
DEFAULT_CHUNK = 1_000_000  # filas por chunk
TXN_FILE = "transactions_train.csv"
ART_FILE = "articles.csv"
CUS_FILE = "customers.csv"

DTYPES_TXN = {
    "article_id": "int64",
    "customer_id": "string",
    "price": "float32",
    "sales_channel_id": "int8",
}
PARSE_DATES = ["t_dat"]  # columna de fecha en transactions


def parse_args():
    p = argparse.ArgumentParser(description="ModaLytics sampler")
    p.add_argument("--data-dir", required=True, help="Carpeta con los CSV originales de H&M")
    p.add_argument("--out-dir", required=True, help="Carpeta de salida para el sample")
    # Modo A (rango de fechas)
    p.add_argument("--start", type=str, default=None, help="Fecha inicio (YYYY-MM-DD)")
    p.add_argument("--end", type=str, default=None, help="Fecha fin (YYYY-MM-DD)")
    # Modo B (muestreo aleatorio)
    p.add_argument("--frac", type=float, default=None, help="Fracción de muestreo (ej. 0.10 para 10%)")
    # Output options
    p.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="Formato de salida")
    p.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK, help="Filas por chunk (RAM control)")
    p.add_argument("--seed", type=int, default=42, help="Semilla para reproducibilidad")
    return p.parse_args()


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def month_key(dt_series: pd.Series) -> pd.Series:
    # clave de mes para estratificación/particionado
    return dt_series.dt.to_period("M").astype(str)


def scan_min_max_dates(txn_path: Path, chunk_size: int):
    min_d, max_d = None, None
    for chunk in pd.read_csv(
        txn_path,
        dtype=DTYPES_TXN,
        parse_dates=PARSE_DATES,
        chunksize=chunk_size
    ):
        cmin, cmax = chunk["t_dat"].min(), chunk["t_dat"].max()
        min_d = cmin if min_d is None or cmin < min_d else min_d
        max_d = cmax if max_d is None or cmax > max_d else max_d
    return min_d, max_d


def write_out_transactions(df: pd.DataFrame, out_dir: Path, fmt: str, first_write: bool):
    if df.empty:
        return
    if fmt == "parquet":
        # Particionar por mes para consultas rápidas
        if "__month" not in df.columns:
            df["__month"] = month_key(df["t_dat"])
        for m, sub in df.groupby("__month"):
            p = out_dir / f"transactions_sample_month={m}.parquet"
            sub.drop(columns=["__month"], errors="ignore").to_parquet(p, index=False)
    else:
        mode = "w" if first_write else "a"
        df.to_csv(out_dir / "transactions_sample.csv", index=False, mode=mode, header=first_write)


def sample_transactions(txn_path: Path, out_dir: Path, fmt: str, chunk_size: int,
                        start: str = None, end: str = None, frac: float = None, seed: int = 42):
    rng = np.random.default_rng(seed)
    kept_articles = set()
    kept_customers = set()
    first_write = True
    total_in, total_out = 0, 0

    for chunk in tqdm(pd.read_csv(
        txn_path,
        dtype=DTYPES_TXN,
        parse_dates=PARSE_DATES,
        chunksize=chunk_size
    ), desc="Procesando transacciones"):
        total_in += len(chunk)

        # Filtro por fechas
        if start or end:
            mask = pd.Series(True, index=chunk.index)
            if start:
                mask &= chunk["t_dat"] >= pd.to_datetime(start)
            if end:
                mask &= chunk["t_dat"] <= pd.to_datetime(end)
            chunk = chunk.loc[mask]

        # Muestreo estratificado simple por mes
        if frac is not None:
            chunk["__month"] = month_key(chunk["t_dat"])
            sampled = []
            for m, sub in chunk.groupby("__month"):
                keep_mask = rng.random(len(sub)) < frac
                sampled.append(sub.loc[keep_mask])
            chunk = pd.concat(sampled, axis=0, ignore_index=True) if sampled else chunk.iloc[0:0]

        if len(chunk) == 0:
            continue

        kept_articles.update(chunk["article_id"].astype("int64").tolist())
        kept_customers.update(chunk["customer_id"].astype("string").tolist())
        total_out += len(chunk)

        write_out_transactions(chunk.drop(columns=["__month"], errors="ignore"), out_dir, fmt, first_write)
        first_write = False

    return kept_articles, kept_customers, total_in, total_out


def filter_articles(articles_path: Path, out_dir: Path, fmt: str, article_ids: set):
    df_art = pd.read_csv(articles_path)
    df_art = df_art[df_art["article_id"].astype("int64").isin(article_ids)]
    if fmt == "parquet":
        df_art.to_parquet(out_dir / "articles_filtered.parquet", index=False)
    else:
        df_art.to_csv(out_dir / "articles_filtered.csv", index=False)
    return len(df_art)


def filter_customers(customers_path: Path, out_dir: Path, customer_ids: set, chunk_size: int):
    """
    Guardamos customers filtrados en CSV incremental para evitar consumir RAM.
    Si luego querés Parquet: convertir con pandas en un paso aparte.
    """
    kept = 0
    out_csv = out_dir / "customers_filtered.csv"
    first_write = True
    for chunk in tqdm(pd.read_csv(customers_path, chunksize=chunk_size), desc="Filtrando customers"):
        chunk["customer_id"] = chunk["customer_id"].astype("string")
        chunk = chunk[chunk["customer_id"].isin(customer_ids)]
        kept += len(chunk)
        if len(chunk) == 0:
            continue
        mode = "w" if first_write else "a"
        chunk.to_csv(out_csv, index=False, mode=mode, header=first_write)
        first_write = False
    return kept


def main():
    args = parse_args()
    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    ensure_dir(out_dir)

    txn_path = data_dir / TXN_FILE
    art_path = data_dir / ART_FILE
    cus_path = data_dir / CUS_FILE

    if not txn_path.exists() or not art_path.exists() or not cus_path.exists():
        raise FileNotFoundError("Faltan CSV: transactions_train.csv / articles.csv / customers.csv en --data-dir")

    # Info de rango de fechas real en transactions
    min_d, max_d = scan_min_max_dates(txn_path, args.chunk_size)
    print(f"[INFO] Rango de fechas en transactions: {min_d.date()} a {max_d.date()}")

    # Validación de modo
    if (args.start or args.end) and (args.frac is not None):
        raise ValueError("Elegí un solo modo: fechas (--start/--end) O muestreo (--frac)")

    if not (args.start or args.end or args.frac is not None):
        # Default: últimos 6 meses del rango detectado
        default_end = max_d.normalize()
        default_start = (default_end - pd.DateOffset(months=6)).normalize()
        print(f"[INFO] Sin parámetros: usaré últimos 6 meses ({default_start.date()} a {default_end.date()})")
        args.start, args.end = str(default_start.date()), str(default_end.date())

    # Ejecutar muestreo/recorte
    kept_articles, kept_customers, total_in, total_out = sample_transactions(
        txn_path, out_dir, args.format, args.chunk_size, args.start, args.end, args.frac, args.seed
    )
    print(f"[RESUMEN] Transacciones leídas: {total_in:,} | retenidas: {total_out:,}")

    # Filtrar dimensiones
    n_art = filter_articles(art_path, out_dir, args.format, kept_articles)
    n_cus = filter_customers(cus_path, out_dir, kept_customers, args.chunk_size)
    print(f"[RESUMEN] Articles retenidos: {n_art:,} | Customers retenidos: {n_cus:,}")

    print(f"[OK] Archivos generados en: {out_dir.resolve()}")
    print("- transactions_sample_month=YYYY-MM.parquet (uno por mes)  O  transactions_sample.csv")
    print("- articles_filtered.parquet/.csv")
    print("- customers_filtered.csv  (convertible a parquet con pandas)")

if __name__ == "__main__":
    main()
