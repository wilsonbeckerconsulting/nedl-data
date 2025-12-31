# NEDL ETL Pipeline

Prefect-based ETL pipeline that extracts ownership data from Cherre, transforms it into a dimensional model, and loads it to Supabase.

## Architecture

```
Cherre API
    ↓
┌─────────────────────────────────────┐
│  raw.cherre_*                       │  ← Append-only landing zone
└────────────────┬────────────────────┘
                 │
      ┌──────────┴──────────┐
      ↓                     ↓
┌───────────┐        ┌────────────────┐
│ app.*     │        │ analytics.*    │
│ (current  │        │ (dimensional,  │
│  state)   │        │  SCD Type 2)   │
└───────────┘        └────────────────┘
```

## Setup

```bash
# Clone the repo
git clone https://github.com/your-org/nedl-data.git
cd nedl-data

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies and pre-commit hooks
make setup
```

## Development

```bash
# Lint (check only)
make lint

# Format (auto-fix)
make format

# Run tests
make test

# Full CI check
make ci
```

Pre-commit hooks run automatically on every commit (ruff lint + format).

## Environment Variables

Required secrets (set in GitHub Actions or `.env` for local dev):

| Variable | Description |
|----------|-------------|
| `CHERRE_API_KEY` | Cherre GraphQL API key |
| `CHERRE_API_URL` | Cherre GraphQL endpoint |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Supabase service role key |
| `PREFECT_API_KEY` | Prefect Cloud API key |
| `PREFECT_API_URL` | Prefect Cloud workspace URL |

## Flows

| Flow | Schedule | Description |
|------|----------|-------------|
| `extract-cherre` | Daily 5am UTC | Cherre → raw schema |
| `transform-analytics` | After extract | raw → analytics schema |
| `transform-app` | After extract | raw → app schema |

## Project Structure

```
src/
├── config.py           # Settings from env vars
├── raw/                # Extract: Cherre → raw schema
├── app/                # Transform: raw → app schema  
├── analytics/          # Transform: raw → analytics schema
├── validation/         # Data quality checks
└── flows/              # Prefect flow orchestrators
```

## CI/CD

- **CI** (`.github/workflows/ci.yml`): Lint, typecheck, test on every PR
- **Daily ETL** (`.github/workflows/daily_etl.yml`): Scheduled ETL runs

