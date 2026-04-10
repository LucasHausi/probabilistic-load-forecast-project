# probabilistic-load-forecast


# uv install for cpu 
uv sync --extra cpu --group dev

# uv install for cuda 12.8 
uv sync --extra cu128 --group dev

docker compose up -d db
uv run alembic upgrade head
uv run pytest
uv run uvicorn apps.api.main:app --reload
uv run streamlit run apps/ui/Home.py


# Daily import forecast commands
plf weather import-forecast --start 2026-03-27T00:00:00Z --end 2026-03-28T00:00:00Z --variable t2m --area-code AT
plf weather import-forecast --start 2026-03-27T00:00:00Z --end 2026-03-28T00:00:00Z --variable ssrd --area-code AT
plf weather import-forecast --start 2026-03-27T00:00:00Z --end 2026-03-28T00:00:00Z --variable tp --area-code AT
plf weather import-forecast --start 2026-03-27T00:00:00Z --end 2026-03-28T00:00:00Z --variable u10 --area-code AT
plf weather import-forecast --start 2026-03-27T00:00:00Z --end 2026-03-28T00:00:00Z --variable v10 --area-code AT

# Windows daily automation for all forecast variables
PowerShell -ExecutionPolicy Bypass -File .\scripts\import_weather_forecast_daily.ps1

# Optional parameters
PowerShell -ExecutionPolicy Bypass -File .\scripts\import_weather_forecast_daily.ps1 -AreaCode AT -ForecastDays 2

# Task Scheduler example
Program/script: powershell.exe
Add arguments: -ExecutionPolicy Bypass -File "C:\path\to\probabilistic-load-forecast-project\scripts\import_weather_forecast_daily.ps1"
Start in: C:\path\to\probabilistic-load-forecast-project

# Load import command
plf load import --start 2026-03-26T00:00:00Z --end 2026-03-28T00:00:00Z

# Windows daily automation for load import
PowerShell -ExecutionPolicy Bypass -File .\scripts\import_load_daily.ps1

# Optional parameters
PowerShell -ExecutionPolicy Bypass -File .\scripts\import_load_daily.ps1 -LookbackDays 2

# Task Scheduler example
Program/script: powershell.exe
Add arguments: -ExecutionPolicy Bypass -File "C:\path\to\probabilistic-load-forecast-project\scripts\import_load_daily.ps1"
Start in: C:\path\to\probabilistic-load-forecast-project
