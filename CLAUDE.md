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
