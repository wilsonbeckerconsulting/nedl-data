# Developer Playbook

Complete guide to get up and running with the NEDL Data Pipeline.

## Prerequisites

Before you start, you need:

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.11+ | `brew install python@3.11` |
| Git | any | `brew install git` |
| Make | any | Comes with Xcode Command Line Tools |

You also need access to:
- **Cherre** — GraphQL API credentials (ask team lead)
- **Supabase** — Project access (ask team lead)
- **Prefect Cloud** — Workspace access (ask team lead)
- **GitHub** — Write access to this repo

---

## 1. Clone & Setup

```bash
# Clone the repo
git clone https://github.com/nedl-ai/nedl-data.git
cd nedl-data

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies + pre-commit hooks
make setup
```

This installs:
- Runtime dependencies (prefect, supabase, requests, etc.)
- Dev dependencies (ruff, pyright, pytest)
- Pre-commit hooks (auto-format on commit)

---

## 2. Configure Credentials

Create a `.env` file in the project root:

```bash
# .env (git-ignored, never commit this)
CHERRE_API_KEY="your-cherre-api-key"
CHERRE_API_URL="https://graphql.cherre.com/graphql"
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_SERVICE_KEY="your-supabase-service-key"
```

**Where to get these:**

| Credential | Where |
|------------|-------|
| `CHERRE_API_KEY` | Ask team lead or check 1Password |
| `SUPABASE_URL` | Supabase Dashboard → Settings → API |
| `SUPABASE_SERVICE_KEY` | Supabase Dashboard → Settings → API → service_role |

---

## 3. Connect to Prefect Cloud

```bash
prefect cloud login
```

This opens a browser to authenticate. Select the **nedl** workspace.

---

## 4. Verify Setup

```bash
# Run tests
make test

# Test full pipeline with a small date range
make pipeline START=2025-12-29 END=2025-12-30
```

---

## 5. Run Locally

All local runs write to the `dev` schema (not prod). Tables are prefixed with their source schema to avoid collisions (e.g., `dev.raw_cherre_transactions`, `dev.analytics_dim_property`).

```bash
# Full pipeline: extract → transform → validate
make pipeline
make pipeline START=2025-01-01
make pipeline START=2025-01-01 END=2025-01-31

# Or run steps individually:
make extract                  # Extract yesterday's data
make extract START=2025-01-01 # Extract from date
make transform                # Transform raw → analytics
make validate                 # Run DQ validation

# Full backfill (chunks by month)
make backfill START=2024-01 END=2024-12
```

---

## 6. Development Workflow

### Making Changes

1. Create a branch: `git checkout -b feature/my-feature`
2. Make changes
3. Commit (pre-commit hooks auto-format)
4. Push: `git push origin feature/my-feature`
5. Open PR → CI runs automatically

### CI Checks

Every PR runs:
- `ruff check` — Linting
- `ruff format --check` — Formatting
- `pyright` — Type checking
- `pytest` — Tests

Fix locally before pushing:
```bash
make ci
```

### Adding a New Raw Table

1. Create `src/raw/cherre_newtable.py`
2. Define `TABLE_NAME = "raw.cherre_newtable"`
3. Implement `sync()` function
4. Add to `src/flows/extract.py`
5. Create Supabase table (see below)

### Adding a New Analytics Table

1. Create `src/analytics/new_dim.py`
2. Define `TABLE_NAME`, `SOURCE_TABLES`, `build()`
3. Add to `src/flows/transform_analytics.py`
4. Create Supabase table (see below)

---

## 7. Database Setup

### Supabase Schemas

The project uses 3 schemas:

| Schema | Purpose |
|--------|---------|
| `dev` | Local development (your machine) |
| `raw` | Production raw data (append-only JSONB) |
| `analytics` | Production dimensional model |

### Exposing Custom Schemas

By default, Supabase only exposes `public`. To use `raw`, `analytics`, `dev`:

1. Go to **Supabase Dashboard** → **Settings** → **API**
2. Under **Exposed schemas**, add: `raw, analytics, dev`
3. Save

Or run SQL:
```sql
ALTER ROLE authenticator SET pgrst.db_schemas = 'public, raw, analytics, dev';
NOTIFY pgrst, 'reload config';
```

### Creating Tables

#### Dev Schema (local development)

```sql
CREATE SCHEMA IF NOT EXISTS dev;

-- Raw tables (prefixed with raw_)
CREATE TABLE dev.raw_cherre_transactions (
    id BIGSERIAL PRIMARY KEY,
    recorder_id TEXT,
    data JSONB NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE dev.raw_cherre_grantors (
    id BIGSERIAL PRIMARY KEY,
    recorder_id TEXT,
    data JSONB NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE dev.raw_cherre_grantees (
    id BIGSERIAL PRIMARY KEY,
    recorder_id TEXT,
    data JSONB NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE dev.raw_cherre_properties (
    id BIGSERIAL PRIMARY KEY,
    tax_assessor_id TEXT,
    data JSONB NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);

-- Analytics tables (prefixed with analytics_)
CREATE TABLE dev.analytics_dim_property (
    id BIGSERIAL PRIMARY KEY,
    property_key INTEGER UNIQUE NOT NULL,
    tax_assessor_id TEXT,
    assessor_parcel_number TEXT,
    property_address TEXT,
    property_city TEXT,
    property_state TEXT,
    property_zip TEXT,
    property_county TEXT,
    property_use_code TEXT,
    land_use_code TEXT,
    year_built INTEGER,
    building_sqft INTEGER,
    land_sqft INTEGER,
    units_count INTEGER,
    assessed_value NUMERIC,
    market_value NUMERIC,
    latitude NUMERIC,
    longitude NUMERIC,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    source_system TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE dev.analytics_dim_entity (
    id BIGSERIAL PRIMARY KEY,
    entity_key INTEGER UNIQUE NOT NULL,
    canonical_entity_id TEXT,
    canonical_entity_name TEXT,
    entity_type TEXT,
    state TEXT,
    confidence_score INTEGER,
    occurrences_count INTEGER,
    is_resolved BOOLEAN DEFAULT TRUE,
    resolution_method TEXT,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    source_system TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE dev.analytics_fact_transaction (
    id BIGSERIAL PRIMARY KEY,
    transaction_key INTEGER UNIQUE NOT NULL,
    recorder_id TEXT UNIQUE,
    property_key INTEGER,
    transaction_date DATE,
    instrument_date DATE,
    document_number TEXT,
    document_type_code TEXT,
    document_amount NUMERIC,
    transfer_tax_amount NUMERIC,
    transaction_category TEXT,
    is_sale BOOLEAN DEFAULT FALSE,
    is_arms_length BOOLEAN DEFAULT FALSE,
    is_inter_family BOOLEAN DEFAULT FALSE,
    is_foreclosure BOOLEAN DEFAULT FALSE,
    is_quit_claim BOOLEAN DEFAULT FALSE,
    is_new_construction BOOLEAN DEFAULT FALSE,
    is_resale BOOLEAN DEFAULT FALSE,
    grantor_count INTEGER,
    grantee_count INTEGER,
    property_address TEXT,
    property_city TEXT,
    property_state TEXT,
    property_zip TEXT,
    source_system TEXT,
    cherre_ingest_datetime TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Permissions
GRANT ALL ON ALL TABLES IN SCHEMA dev TO service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA dev TO service_role;
```

#### Production Schemas (raw + analytics)

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE raw.cherre_transactions (
    id BIGSERIAL PRIMARY KEY,
    recorder_id TEXT,
    data JSONB NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);
-- (same pattern for other tables, without prefix)
```

---

## 8. Production Deployment

Production runs via **GitHub Actions**:

### Daily ETL (Scheduled)

Runs automatically at 5am UTC daily. Or trigger manually:

1. Go to **Actions** → **Daily ETL**
2. Click **Run workflow**
3. Optionally enter start/end dates
4. Click **Run**

### GitHub Secrets Required

These must be set in **GitHub → Settings → Secrets**:

| Secret | Description |
|--------|-------------|
| `CHERRE_API_KEY` | Cherre API key |
| `CHERRE_API_URL` | Cherre endpoint |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Supabase service key |
| `PREFECT_API_KEY` | Prefect Cloud API key |
| `PREFECT_API_URL` | Prefect workspace URL |

---

## 9. Monitoring & Alerting

### Prefect Cloud

All flow runs (local and prod) appear in [Prefect Cloud](https://app.prefect.cloud):
- View run history
- See logs
- Check task status

### DQ Alerts

DQ failures emit `nedl.dq.failure` events. To get notified:

1. Go to **Prefect Cloud** → **Automations**
2. Create new automation:
   - Trigger: Event `nedl.dq.failure`
   - Action: Slack/Email notification

---

## 10. Troubleshooting

### "401 Unauthorized" from Cherre
- Check `.env` has correct `CHERRE_API_KEY`
- Clear pycache: `make clean` (settings are cached)
- Try: `make extract START=2025-12-29 END=2025-12-30`

### "401 Unauthorized" from Prefect
- Run `prefect cloud login` to re-authenticate

### "Schema must be public" from Supabase
- Expose `raw`, `analytics`, `dev` schemas in Supabase API settings

### Tests failing locally but passing in CI
- Ensure you're using Python 3.11: `python --version`
- Ensure ruff version matches: `ruff --version` (should be 0.8.6)

---

## Quick Reference

```bash
# Setup
make setup                    # First-time install

# Full pipeline
make pipeline                 # Extract → Transform → Validate
make pipeline START=2025-01-01

# Individual steps
make extract                  # Extract yesterday
make extract START=2025-01-01 # Extract from date
make transform                # Raw → Analytics
make validate                 # DQ checks
make backfill START=2024-01 END=2024-12

# Quality
make lint                     # Check code style
make format                   # Auto-fix formatting
make typecheck                # Type checking
make test                     # Run tests
make ci                       # Full CI check
make clean                    # Clear caches
```

