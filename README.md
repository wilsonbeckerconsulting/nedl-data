# NEDL Data Pipeline

Prefect-based ETL pipeline that extracts multifamily ownership data from Cherre, transforms it into a dimensional model, validates data quality, and loads it to Supabase.

## Architecture

```
Cherre GraphQL API
        ↓
┌───────────────────────────────────────────────────────────┐
│  raw schema (append-only, JSONB)                          │
│  ├── cherre_transactions                                  │
│  ├── cherre_properties                                    │
│  ├── cherre_grantors                                      │
│  └── cherre_grantees                                      │
└─────────────────────────┬─────────────────────────────────┘
                          │
           ┌──────────────┴──────────────┐
           ↓                             ↓
┌─────────────────────┐       ┌─────────────────────────────┐
│  app schema         │       │  analytics schema           │
│  (current state,    │       │  (dimensional model,        │
│   API-optimized)    │       │   SCD Type 2, historical)   │
│                     │       │  ├── dim_property           │
│  TODO               │       │  ├── dim_entity             │
│                     │       │  ├── dim_entity_identifier  │
└─────────────────────┘       │  ├── fact_transaction       │
                              │  ├── bridge_transaction_party│
                              │  └── bridge_property_owner  │
                              └─────────────────────────────┘
```

## Setup

```bash
# Clone the repo
git clone https://github.com/nedl-ai/nedl-data.git
cd nedl-data

# Create virtual environment (Python 3.11+)
python -m venv venv
source venv/bin/activate

# Install dependencies and pre-commit hooks
make setup

# Connect to Prefect Cloud
prefect cloud login
```

## Development

```bash
# Lint (check only)
make lint

# Format (auto-fix)
make format

# Type check
make typecheck

# Run tests
make test

# Full CI check (lint + typecheck + test)
make ci
```

Pre-commit hooks run automatically on every commit (Ruff lint + format).

## Environment Variables

Required secrets (set in GitHub Actions Secrets):

| Variable | Description |
|----------|-------------|
| `CHERRE_API_KEY` | Cherre GraphQL API key |
| `CHERRE_API_URL` | Cherre GraphQL endpoint |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Supabase service role key |
| `PREFECT_API_KEY` | Prefect Cloud API key |
| `PREFECT_API_URL` | Prefect Cloud workspace URL |

For local development, create a `.env` file (git-ignored):

```bash
export CHERRE_API_KEY="your-key"
export CHERRE_API_URL="https://graphql.cherre.com/graphql"
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_KEY="your-service-key"
```

## Flows

| Flow | File | Description |
|------|------|-------------|
| `extract-cherre` | `src/flows/extract.py` | Cherre API → raw schema |
| `transform-analytics` | `src/flows/transform_analytics.py` | raw → analytics schema (dimensional model) |
| `validate-analytics` | `src/flows/validate.py` | DQ checks, emits `nedl.dq.failure` events |

### Running Flows Locally

```bash
# Extract yesterday's data
python src/flows/extract.py

# Extract specific date range
python src/flows/extract.py --start-date 2025-01-01 --end-date 2025-01-31

# Transform to analytics
python src/flows/transform_analytics.py

# Run DQ validation
python src/flows/validate.py
```

## Project Structure

```
nedl-data/
├── src/
│   ├── config.py              # Pydantic settings from env vars
│   ├── db.py                  # Supabase client helpers
│   ├── protocols.py           # Module contracts (typing.Protocol)
│   │
│   ├── raw/                   # Extract: Cherre → raw schema
│   │   ├── cherre_client.py   # GraphQL client with retry logic
│   │   ├── cherre_transactions.py
│   │   ├── cherre_properties.py
│   │   ├── cherre_grantors.py
│   │   └── cherre_grantees.py
│   │
│   ├── analytics/             # Transform: raw → analytics schema
│   │   ├── dim_property.py    # SCD Type 2 property dimension
│   │   ├── dim_entity.py      # Canonical entity dimension
│   │   └── fact_transaction.py
│   │
│   ├── app/                   # Transform: raw → app schema (TODO)
│   │
│   ├── validation/            # Data quality checks
│   │   └── data_quality.py    # Comprehensive DQ report
│   │
│   └── flows/                 # Prefect flow orchestrators
│       ├── extract.py
│       ├── transform_analytics.py
│       └── validate.py
│
├── tests/
│   ├── test_smoke.py          # Import and settings tests
│   └── test_module_contracts.py  # Module structure validation
│
├── .github/workflows/
│   ├── ci.yml                 # Lint, typecheck, test on PRs
│   └── daily_etl.yml          # Scheduled ETL runs
│
├── pyproject.toml             # Project config, dependencies
├── requirements.txt           # Runtime dependencies
├── requirements-dev.txt       # Dev dependencies (ruff, pyright, pytest)
├── Makefile                   # Dev commands
├── prefect.yaml               # Prefect deployment config
└── .pre-commit-config.yaml    # Pre-commit hooks (Ruff)
```

## Database Schemas

### Raw Schema (append-only landing zone)

```sql
CREATE TABLE raw.cherre_transactions (
    id BIGSERIAL PRIMARY KEY,
    recorder_id TEXT,
    data JSONB NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);
```

All raw tables follow this pattern: indexed ID column + full record as JSONB.

### Analytics Schema (dimensional model)

- **dim_property**: SCD Type 2 with `valid_from`, `valid_to`, `is_current`
- **dim_entity**: Canonical owner entities with fuzzy-matched identifiers
- **fact_transaction**: Deed/recording events with FK to property and parties
- **bridge_transaction_party**: M:N between transactions and entities
- **bridge_property_owner**: Current ownership relationships

## Alerting

DQ failures emit custom Prefect events (`nedl.dq.failure`). Set up a Prefect Automation:

1. Go to **Automations** in Prefect Cloud
2. Create new automation with trigger:
   - Event: `nedl.dq.failure`
   - Resource: `nedl-data.*`
3. Add action: Send notification (Slack, email, etc.)

## CI/CD

- **CI** (`.github/workflows/ci.yml`): Runs on every PR/push to main
  - Ruff lint + format check
  - Pyright type checking
  - Pytest tests

- **Daily ETL** (`.github/workflows/daily_etl.yml`): Scheduled runs
  - Extract → Transform → Validate pipeline
  - Triggered daily at 5am UTC
