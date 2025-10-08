import pandas as pd
import time
import os

# === 1. Читаємо CSV і міряємо час ===
start = time.time()
df_csv = pd.read_csv("data/transactions_data.csv")
csv_time = time.time() - start

# amount може зчитатися як str → приводимо до числового
df_csv["amount"] = pd.to_numeric(df_csv["amount"], errors="coerce")

# === 2. Зберігаємо у Parquet зі стисненням ===
df_csv.to_parquet("data/transactions_data.parquet", engine="pyarrow", compression="snappy")

# === 3. Читаємо Parquet і міряємо час ===
start = time.time()
df_parquet = pd.read_parquet("data/transactions_data.parquet", engine="pyarrow")
parquet_time = time.time() - start

# === 4. Досліджуємо розмір файлів ===
csv_size = os.path.getsize("data/transactions_data.csv") / (1024 * 1024)     # MB
parquet_size = os.path.getsize("data/transactions_data.parquet") / (1024 * 1024)

# === 5. Простий запит для порівняння швидкості ===
start = time.time()
_ = df_csv[df_csv["amount"] > 1000]
csv_query_time = time.time() - start

start = time.time()
_ = df_parquet[df_parquet["amount"] > 1000]
parquet_query_time = time.time() - start

# === 6. Виводимо результати ===
print(f"CSV size: {csv_size:.2f} MB")
print(f"Parquet size: {parquet_size:.2f} MB")

print(f"CSV read: {csv_time:.3f} sec")
print(f"Parquet read: {parquet_time:.3f} sec")

print(f"CSV query (>1000): {csv_query_time:.6f} sec")
print(f"Parquet query (>1000): {parquet_query_time:.6f} sec")
