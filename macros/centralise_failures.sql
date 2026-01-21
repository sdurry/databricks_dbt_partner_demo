{% macro centralize_test_failures(results) %}
  {# --add "{{ centralize_test_failures(results) }}" to an on-run-end: block in dbt_project.yml #}
  {# --run with dbt build --store-failures. The next v.1.0.X release of dbt will include post run hooks for dbt test! #}
  {%- set test_results = [] -%}
  {%- for result in results -%}
    {%- if result.node.resource_type == 'test' and result.status != 'skipped' and (
          result.node.config.get('store_failures') or flags.STORE_FAILURES
      )
    -%}
      {%- do test_results.append(result) -%}
    {%- endif -%}
  {%- endfor -%}


  {%- set central_tbl -%} {{target.database}}.`{{ target.schema }}`.`test_failure_central` {%- endset -%}
  {%- set history_tbl -%} {{target.database}}.`{{ target.schema }}`.`test_failure_history` {%- endset -%}

  {% if test_results | length > 0 %} 

    {{ log("Centralizing test failures in " + central_tbl, info = true) if execute }}
    {% do run_query("create schema if not exists " ~ target.database ~ ".`" ~ target.schema ~ "`") %}

    {%- set central_tbl_query -%}
    create or replace table {{ central_tbl }} as (

    {% for result in test_results %}

      select
        '{{ result.node.name }}' as test_name,
        '{{ result.node.unique_id }}' as model_name,
        to_json(struct(*)) as test_failures_json,
        current_timestamp as _timestamp

      from {{ result.node.relation_name }}

      {{ "union all" if not loop.last }}

    {% endfor %}

    )
    {%- endset -%}

    {% do run_query(central_tbl_query) %}
    {{ log("Finished inserting test failures in " + central_tbl, info = true) if execute }}
  {# only run centralization in higher environments #}
    {{ log("Checking the DBT_CLOUD_ENVIRONMENT_TYPE: " + env_var('DBT_CLOUD_ENVIRONMENT_TYPE'), info = true) if execute }}
  {% if env_var('DBT_CLOUD_ENVIRONMENT_TYPE', 'dev') != 'dev' %}
      {{ log("Historizing test failures in " + history_tbl, info = true) if execute }}
      {%- set history_create_query -%}
      create table if not exists {{ history_tbl }} as (
        select
          {{ dbt_utils.generate_surrogate_key(["test_name", "test_failures_json", "_timestamp"]) }} as sk_id,
          *
        from {{ central_tbl }}
        where false
      )
      {%- endset -%}
      {% do run_query(history_create_query) %}

      {%- set history_insert_query -%}
      insert into {{ history_tbl }}
        select
         {{ dbt_utils.generate_surrogate_key(["test_name", "test_failures_json", "_timestamp"]) }} as sk_id,
         *
        from {{ central_tbl }}
      {%- endset -%}
      {% do run_query(history_insert_query) %}
      {{ log("Finished inserting new test failures in " + history_tbl, info = true) if execute }}
    {% endif %}

  {% endif %}

{% endmacro %}
