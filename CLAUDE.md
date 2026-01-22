# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a dbt project (`databricks_demo`) for Databricks that transforms TPCH benchmark data and Open Exchange Rate data into analytics-ready dimensional models. It uses Unity Catalog and targets Databricks SQL Warehouses.

## Common Commands

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run specific model
dbt run --select model_name

# Run with downstream dependencies
dbt run --select model_name+

# Run with upstream dependencies
dbt run --select +model_name

# Run tests
dbt test

# Test specific model
dbt test --select model_name

# Run unit tests only
dbt test --select test_type:unit

# Check source freshness
dbt source freshness

# Generate documentation
dbt docs generate
dbt docs serve

# Compile SQL without running
dbt compile --select model_name
```

## Architecture

### Data Flow
```
Sources (TPCH, Exchange Rates)
    ↓
Staging Models (stg_*)     [views, protected access]
    ↓
Intermediate Models (int_*, order_items, part_suppliers, customer_tier)
    ↓
Marts (dim_*, fct_*)       [tables, public-facing]
```

### Source Data
- **tpch**: Standard TPCH benchmark data from `samples.tpch` (customers, orders, line items, parts, suppliers, nations, regions)
- **tpch_now**: Orders table from `sa_dev.tpch_now` with freshness monitoring
- **exchange_rates**: Currency data from `sa_dev.raw_open_exchange_rates` via Fivetran

### Model Organization
- `models/staging/tpch/` - TPCH source transformations (views)
- `models/staging/open_exchange_rate/` - Exchange rate transformations (views)
- `models/marts/intermediate/` - Business logic calculations (order_items, part_suppliers, customer_tier)
- `models/marts/intermediate/exchange_rates/` - Exchange rate processing
- `models/marts/core/` - Final dimensional models (dim_customers, dim_parts, dim_suppliers, fct_orders, fct_order_items)

### Key Configurations
- **File format**: Delta tables with merge incremental strategy
- **Materialization**: Staging = views, Marts = tables
- **Groups**: `data-engineering` (staging), `analytics` (marts)
- **Access control**: Grants to "Partner Engineers" role
- **Unity Catalog**: Persists docs as column/relation comments

### Packages Used
- `dbt_utils` - SQL utilities
- `codegen` - Code generation helpers
- `dbt_expectations` - Data quality tests (e.g., cross-table aggregation comparisons)

### Testing Patterns
- Schema tests in YAML files (`data_tests` property)
- Singular tests in `tests/` directory
- Generic custom tests in `tests/generic/` (e.g., `is_even`)
- Unit tests defined in YAML (`unit_tests` block)
- Cross-model validation using `dbt_expectations.expect_table_aggregation_to_equal_other_table`

### Documentation
- Doc blocks in `docs/` directory organized by layer (staging, marts)
- Referenced using `{{ doc("block_name") }}` syntax in YAML descriptions