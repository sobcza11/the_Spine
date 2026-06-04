import os
import requests

key = os.getenv("TIINGO_API_KEY")

print("KEY FOUND:", bool(key))
print("KEY PREFIX:", key[:8])

r = requests.get(
    "https://api.tiingo.com/account",
    headers={"Content-Type": "application/json"},
    params={"token": key},
    timeout=30
)

print("STATUS:", r.status_code)
print(r.text[:500])