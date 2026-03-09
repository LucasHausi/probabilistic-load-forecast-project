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