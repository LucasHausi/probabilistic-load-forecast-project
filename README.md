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
plf weather import-forecast --start 2026-03-27T00:00:00Z --end 2026-03-28T00:00:00Z --variable to --area-code AT
plf weather import-forecast --start 2026-03-27T00:00:00Z --end 2026-03-28T00:00:00Z --variable u10 --area-code AT
plf weather import-forecast --start 2026-03-27T00:00:00Z --end 2026-03-28T00:00:00Z --variable v10 --area-code AT

# Load import command
plf load import --start 2026-03-26T00:00:00Z --end 2026-03-28T00:00:00Z