import pandas as pd

url = (
    "https://www.bankofcanada.ca/valet/observations/V39063/json"
)

df = pd.read_json(url)

print(df.head())
print(df.tail())
