import pandas as pd

# Припустимо, у вас є три CSV
tables = {
    "cards_data": "data/cards_data.csv",
    "transactions_data": "data/transactions_data.csv",
    "users_data": "data/users_data.csv"
}

for name, path in tables.items():
    df = pd.read_csv(path)
    df.to_parquet(f"data/{name}.parquet", engine="pyarrow", index=False)
