# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **dbt analytics engineering project** running on **Databricks with Unity Catalog**. It demonstrates data transformation patterns using TPC-H sample data and exchange rate data.

- **dbt version**: 2.0.0
- **Data platform**: Databricks (catalog: `sa_emea_uc_demo`)
- **File format**: Delta (with Iceberg support available)

## Common Commands

```bash
# Run all models
dbtf run

# Run a specific model and its upstream dependencies
dbtf run --select +model_name

# Run all models in a folder
dbtf run --select staging.tpch

# Run tests
dbtf test

# Test a specific model
dbtf test --select model_name

# Generate and serve documentation
dbtf docs generate
dbtf docs serve

# Check source freshness
dbtf source freshness

# Install packages
dbtf deps

# Clean compiled artifacts
dbt clean
```

## Architecture

The project follows the **staging → intermediate → marts** pattern:

```
models/
├── staging/          # Raw data transformation (views, protected access)
│   ├── tpch/         # TPC-H dataset: customers, orders, parts, suppliers, etc.
│   └── open_exchange_rate/  # Currency exchange rates
├── marts/
│   ├── intermediate/ # Business logic transformations
│   │   └── exchange_rates/  # Currency conversion logic
│   └── core/         # Analytics-ready facts and dimensions
│       ├── dim_customers.sql
│       ├── dim_parts.sql
│       ├── dim_suppliers.sql
│       ├── fct_orders.sql
│       └── fct_order_items.sql
```

**Key conventions:**
- Staging models: `stg_<source>_<entity>.sql` (materialized as views)
- Mart models: `dim_` or `fct_` prefix (materialized as tables)
- YAML files: `_<folder>.yml` pattern for model definitions

## Packages

The project uses these dbt packages (defined in `packages.yml`):
- **dbt_utils**: Utility macros for common transformations
- **codegen**: Code generation helpers
- **dbt_expectations**: Data quality testing (Great Expectations-style)

## Data Access

- Models grant SELECT to "Partner Engineers" group
- Staging models use `protected` access level
- Schema usage granted via `on-run-start` hook

## Groups

Models are organized into ownership groups:
- `data-engineering`: Staging layer
- `analytics`: Marts layer

## On-Run-End Macros

The project uses two on-run-end macros to centralize and historize dbt execution metadata:

### `centralize_test_failures`
- **File**: `macros/centralise_failures.sql`
- **Purpose**: Captures test failures from `--store-failures` runs
- **Tables created**:
  - `test_failure_central`: Current run's test failures (replaced each run)
  - `test_failure_history`: Historical test failures (non-dev environments only)
- **Fields**: `test_name`, `model_name`, `test_failures_json`, `_timestamp`

### `centralize_run_results`
- **File**: `macros/centralise_run_results.sql`
- **Purpose**: Captures execution metadata for seeds, models, and snapshots
- **Tables created**:
  - `run_results_central`: Current run's execution results (replaced each run)
  - `run_results_history`: Historical execution results (non-dev environments only)
- **Fields**: `database_relation_name`, `model_name`, `execution_status`, `execution_time`, `rows_affected`, `_timestamp`

### Environment Behavior
- **Dev**: Only creates `*_central` tables
- **Non-dev** (`DBT_CLOUD_ENVIRONMENT_TYPE != 'dev'`): Also historizes to `*_history` tables with surrogate keys

### Testing Macros Locally
```bash
# Test with dev environment (no historization)
dbt build -s incremental_seed +customer_tier

# Test with prod environment (includes historization)
export DBT_CLOUD_ENVIRONMENT_TYPE=prod
dbt build -s incremental_seed +customer_tier
```
