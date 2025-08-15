import pandas as pd
from pathlib import Path

# 📅 Rango de fechas según tu dataset
start_date = "2018-09-20"
end_date = "2020-09-22"

# 📂 Carpeta de salida
output_dir = Path("data/sample")
output_dir.mkdir(parents=True, exist_ok=True)

# 🔹 Generar dataframe de calendario
dates = pd.date_range(start=start_date, end=end_date)
df = pd.DataFrame({
    "datekey": dates.strftime("%Y%m%d").astype(int),  # YYYYMMDD como número
    "fulldate": dates.strftime("%Y-%m-%d"),
    "year": dates.year,
    "month": dates.month,
    "monthname": dates.strftime("%B"),
    "week": dates.isocalendar().week,
    "day": dates.day
})

# 💾 Guardar a CSV
df.to_csv(output_dir / "d_date.csv", index=False, encoding="utf-8")

print(f"✅ Archivo creado: {output_dir / 'd_date.csv'}")
print(f"📊 {len(df)} filas generadas")
