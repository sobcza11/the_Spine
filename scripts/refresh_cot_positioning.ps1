cd "C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine"

$env:PYTHONPATH = "C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\src"

python .\src\spine\cot\orchestration\run_cot_weekly_refresh_v1.py

if ($LASTEXITCODE -ne 0) {
  throw "COT upstream refresh failed. Stop before publishing stale data."
}

python .\src\spine\state_engine\build_cot_positioning_serving.py

if ($LASTEXITCODE -ne 0) {
  throw "COT serving build failed. Stop before publishing."
}

wrangler r2 object put pub-73703eeb21994303b8b98f8cbcf6dbca/spine_us/serving/cflow/cot_positioning_serving.json `
  --file .\data\serving\cflow\cot_positioning_serving.json `
  --remote