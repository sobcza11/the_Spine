$ErrorActionPreference = "Stop"

Set-Location "C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine"

.\.venv\Scripts\Activate.ps1

python .\src\spine\jobs\macro\build_rates_yield_family.py

mkdir .\data\serving\rates -ErrorAction SilentlyContinue

Copy-Item .\data\macro\serving\rates\rates_us_yield_family.json .\data\serving\rates\rates_us_yield_family.json -Force
Copy-Item .\data\macro\serving\rates\rates_us_yield_family_latest.json .\data\serving\rates\rates_us_yield_family_latest.json -Force

python .\src\spine\jobs\r2\upload_rates_yield_family_to_r2.py

Write-Host "PASS | RATES daily refresh complete"
