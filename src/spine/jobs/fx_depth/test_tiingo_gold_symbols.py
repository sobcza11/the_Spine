import os
import requests

API_KEY = os.getenv("TIINGO_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing TIINGO_API_KEY environment variable.")

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Token {API_KEY}"
}

candidates = ["xauusd", "XAUUSD", "xau/usd", "XAU/USD"]

for symbol in candidates:
    url = f"https://api.tiingo.com/tiingo/fx/{symbol}/prices"
    params = {
        "startDate": "2026-01-01",
        "resampleFreq": "1day"
    }

    r = requests.get(url, headers=headers, params=params, timeout=30)

    print("\nSYMBOL:", symbol)
    print("STATUS:", r.status_code)
    print("TEXT:", r.text[:500])