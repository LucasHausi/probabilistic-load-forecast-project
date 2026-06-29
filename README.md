# Interpretable Probabilistic Electricity Load Forecasting

Day-ahead electricity-load forecasting for the Austrian bidding zone, combining deterministic and Bayesian sequence models with uncertainty diagnostics and SHAP-based explanations.

[Thesis (PDF)](thesis/MA_Hausensteiner.pdf) · [Thesis source](thesis/MA_Hausensteiner.tex) · [Quick start](#quick-start) · [Research results](#research-results)

## Overview

This repository contains the research code and deployment prototype for the master's thesis **“Interpretable Probabilistic Electricity Load Forecasting Using Bayesian LSTM and Explainable AI Methods in Renewable Dominated Power Grids.”** It studies 24-hour-ahead load forecasting at 15-minute resolution for Austria.

The project brings together:

- **Open energy and weather data:** ENTSO-E actual load, ERA5-Land reanalysis, and ECMWF forecasts.
- **Three forecasting approaches:** Prophet, a deterministic LSTM, and a Bayesian LSTM.
- **Probabilistic evaluation:** prediction intervals, PIT histograms, PICP, and MPIW.
- **Forecast explanations:** global, day-part, and timestep-level SHAP analyses, including attribution uncertainty for the Bayesian model.
- **An interactive prototype:** FastAPI data access and a Streamlit interface for forecasts, uncertainty bands, and explanations.

The repository is a research prototype, not an operational TSO forecasting system. In particular, the Bayesian prediction intervals require further calibration before they should be used as operational risk estimates.

## Research setup

| Dimension | Configuration |
| --- | --- |
| Forecast target | Austrian bidding-zone load (`10YAT-APG------L`) |
| Resolution | 15 minutes |
| Forecast horizon | 96 steps / 24 hours |
| LSTM lookback | 672 steps / 7 days |
| Load source | ENTSO-E Transparency Platform |
| Historical weather | ERA5-Land via the Copernicus Climate Data Store |
| Forecast weather | ECMWF Open Data |
| Weather variables | 2 m temperature, solar radiation, precipitation, and 10 m u/v wind components |
| Models | Prophet, deterministic LSTM, Bayesian LSTM |
| Explainability | SHAP at global, day-part, and individual-timestep levels |

Weather grids are spatially aggregated to Austria. Accumulated variables are converted into rates, hourly weather is aligned to the 15-minute load series, and forecast covariates are shifted to match the day-ahead task. The selected model feature set combines recent load, basic calendar indicators, and future weather covariates.

## Architecture

```mermaid
flowchart LR
    ENTSOE[ENTSO-E load] --> INGEST[CLI and provider adapters]
    ERA5[ERA5-Land] --> INGEST
    ECMWF[ECMWF forecasts] --> INGEST
    INGEST --> PG[(PostgreSQL)]
    PG --> API[FastAPI]
    API --> UI[Streamlit prototype]

    NB[EDA and modelling notebooks] --> MLFLOW[(MLflow model registry)]
    NB --> ARTIFACTS[Prepared arrays and scalers]
    MLFLOW --> UI
    ARTIFACTS --> UI
    UI --> OUTPUT[Point forecasts, intervals, and SHAP explanations]
```

The Python package follows a ports-and-adapters structure: domain types are independent of infrastructure, application services implement the use cases, and adapters handle ENTSO-E, CDS, ECMWF, and PostgreSQL integration.

## Quick start

### Prerequisites

- Python `>=3.11,<3.13`
- [uv](https://docs.astral.sh/uv/)
- Docker with Compose
- An ENTSO-E API token for load imports
- Copernicus CDS API credentials for ERA5-Land imports

### 1. Install the environment

For CPU execution:

```bash
uv sync --extra cpu --group dev
```

For an NVIDIA system using the configured CUDA 12.8 PyTorch index:

```bash
uv sync --extra cu128 --group dev
```

### 2. Configure credentials

Create a `.env` file in the repository root:

```dotenv
PG_DSN=postgresql://postgres:secret@localhost:5432/plf
ENTSOE_BASE_URL=YOUR_ENTSOE_API_BASE_URL
ENTSOE_SECURITY_TOKEN=YOUR_ENTSOE_SECURITY_TOKEN
CDSAPI_URL=YOUR_CDS_API_URL
CDSAPI_KEY=YOUR_CDS_API_KEY
```

The `.env` file is ignored by Git. ECMWF Open Data imports do not require credentials.

### 3. Start PostgreSQL

```bash
docker compose up -d db
```

Repository adapters create their tables on the first import; there is no separate migration step. Populate load data and all five weather variables before opening the forecast page because the latest-common-timestamp query depends on all six time series.

### 4. Import data

Import Austrian load data from ENTSO-E:

```bash
uv run plf load import \
  --start 2026-03-26T00:00:00Z \
  --end 2026-03-28T00:00:00Z
```

Download ERA5-Land data, calculate Austrian country averages, and store them in PostgreSQL:

```bash
uv run plf weather fetch-store \
  --start 2025-01-01T00:00:00Z \
  --end 2025-02-01T00:00:00Z
```

Import one ECMWF forecast variable:

```bash
uv run plf weather import-forecast \
  --start 2026-03-27T00:00:00Z \
  --end 2026-03-28T00:00:00Z \
  --variable t2m \
  --area-code AT
```

Repeat the forecast import for `t2m`, `ssrd`, `tp`, `u10`, and `v10`. On Windows, the included scripts automate the daily imports:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\scripts\import_load_daily.ps1
PowerShell -ExecutionPolicy Bypass -File .\scripts\import_weather_forecast_daily.ps1
```

Both scripts accept optional parameters; use PowerShell's `Get-Help` or inspect the parameter blocks at the top of each script.

### 5. Run the API and UI

Start the API:

```bash
uv run uvicorn apps.api.main:app --reload
```

The API is available at `http://localhost:8000`, with interactive OpenAPI documentation at `http://localhost:8000/docs`.

In a second terminal, start Streamlit:

```bash
uv run streamlit run apps/ui/Home.py
```

The load-data and weather-data pages only require PostgreSQL and the API. The forecast page has the additional model-artifact requirements described below.

## Forecast prototype prerequisites

The Streamlit forecast page loads these registered models from MLflow:

- `Prophet`
- `VanillaLSTM`
- `BayesianLSTM`

It also expects preprocessing artifacts under:

```text
data/processed/ml_data/fs_06_load_calendar_future_weather/
├── X_train.npy
├── meta.json
└── scalers/
    ├── X_scaler.pkl
    └── y_scaler.pkl
```

The Prophet explanation additionally reads `data/processed/data_combined.parquet`. These generated files and MLflow artifacts are intentionally not committed.

Start the local tracking server before using the forecast page:

```bash
uv run mlflow server \
  --backend-store-uri sqlite:///mlflow.db \
  --default-artifact-root ./mlartifacts \
  --host 127.0.0.1 \
  --port 5000
```

Run the preparation and modelling notebooks to generate the files and model runs, then register the selected model versions under the names above. The UI resolves the latest registered version of each model.

## CLI reference

All timestamps should be ISO 8601 values. Naive timestamps are interpreted as UTC, but explicit offsets or a trailing `Z` are recommended.

| Command | Purpose |
| --- | --- |
| `plf load import` | Fetch and store ENTSO-E actual load |
| `plf load get` | Read stored load for a bidding-zone EIC code |
| `plf weather fetch-store` | Download ERA5-Land data, aggregate it, and store it |
| `plf weather store-averages` | Aggregate already-downloaded ERA5 files and store them |
| `plf weather get-db` | Read one stored country-average weather series |
| `plf weather import-forecast` | Fetch and store an ECMWF forecast variable |

Inspect exact arguments with, for example, `uv run plf weather import-forecast --help`.

## API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/load-data` | Load observations for `start`, `end`, and `eic_code` |
| `GET` | `/weather-data` | Weather observations for `start`, `end`, `variable`, and `area_code` |
| `GET` | `/latest-common-timestamp` | Latest timestamp shared by load and all weather series |

## Research workflow

The notebooks document the experimental workflow and are intentionally kept separate from the service package:

1. **Data exploration and preparation**
   - [`notebooks/eda/data_preperation.ipynb`](notebooks/eda/data_preperation.ipynb)
   - [`notebooks/eda/feature_engineering.ipynb`](notebooks/eda/feature_engineering.ipynb)
   - [`notebooks/eda/load_forecast_eda.ipynb`](notebooks/eda/load_forecast_eda.ipynb)
2. **Model training**
   - [`notebooks/modelling/prophet.ipynb`](notebooks/modelling/prophet.ipynb)
   - [`notebooks/modelling/lstm.ipynb`](notebooks/modelling/lstm.ipynb)
   - [`notebooks/modelling/bayesian_lstm.ipynb`](notebooks/modelling/bayesian_lstm.ipynb)
3. **Evaluation and interpretation**
   - [`notebooks/performance/model_comparison.ipynb`](notebooks/performance/model_comparison.ipynb)
   - [`notebooks/performance/model_shap.ipynb`](notebooks/performance/model_shap.ipynb)
   - [`notebooks/performance/profiling.ipynb`](notebooks/performance/profiling.ipynb)

The notebooks are stateful research artifacts. Run them in the order above and review their path/configuration cells before executing long training jobs.

## Research results

Held-out test performance reported in the thesis:

| Model | RMSE (MW) | MAE (MW) | MAPE |
| --- | ---: | ---: | ---: |
| Deterministic LSTM | **341.09** | **218.39** | **3.39%** |
| Bayesian LSTM (hidden size 64) | 353.59 | 250.21 | 3.79% |
| Prophet | 521.33 | 418.77 | 6.57% |

The main findings are:

- Recent load is the dominant predictor; calendar information, especially the weekday indicator, is the strongest secondary signal.
- Future weather covariates improve the forecast, while historical weather alone is less useful in this setup.
- The deterministic LSTM provides the best point forecasts; midday is the most difficult regime for all three models.
- The Bayesian LSTM produces predictive distributions, but calibration varies by model size. The selected hidden-size-64 model achieves raw 95% PICP of `0.9465` with a comparatively wide MPIW of `1318.81 MW`.
- SHAP rankings remain broadly stable across stochastic Bayesian forward passes, although attribution uncertainty changes by feature and time-of-day regime.

These figures describe the thesis experiment and should not be interpreted as a benchmark against official Austrian TSO forecasts.

## Repository layout

```text
.
├── apps/
│   ├── api/                  # FastAPI read API
│   └── ui/                   # Streamlit exploration and forecast prototype
├── notebooks/
│   ├── eda/                  # Data preparation and feature engineering
│   ├── modelling/            # Prophet, LSTM, and Bayesian LSTM training
│   └── performance/          # Comparison, SHAP, and profiling
├── scripts/                  # Windows daily import automation
├── src/probabilistic_load_forecast/
│   ├── adapters/             # ENTSO-E, CDS, ECMWF, and PostgreSQL
│   ├── application/          # Use cases and service ports
│   └── domain/               # Forecasting domain models and validation
├── tests/                    # Unit and PostgreSQL integration tests
└── thesis/                   # Thesis source, bibliography, figures, and PDF
```

## Development

Run the test suite with PostgreSQL available at the configured `PG_DSN`:

```bash
uv run pytest
```

Run only tests that do not require PostgreSQL:

```bash
uv run pytest --ignore=tests/adapters/db
```

Run the configured pre-commit checks:

```bash
uv run pre-commit run --all-files
```

## Thesis and license

The full methodology, research questions, calibration analysis, limitations, and discussion are available in the [thesis PDF](thesis/MA_Hausensteiner.pdf). The LaTeX source and bibliography are included under [`thesis/`](thesis/).

Author: **Lucas Hausensteiner**

Degree programme: **Data Science, FH Technikum Wien**

This project is licensed under the [GNU General Public License v3.0](LICENSE).
