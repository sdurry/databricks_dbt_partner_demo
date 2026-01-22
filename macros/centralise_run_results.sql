{% macro centralize_run_results(results) %}
  {# --add "{{ centralize_run_results(results) }}" to an on-run-end: block in dbt_project.yml #}
  {%- set run_results = [] -%}
  {%- for result in results -%}
    {%- if result.node.resource_type in ['seed', 'model', 'snapshot'] -%}
      {%- do run_results.append(result) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set central_tbl -%} {{target.database}}.`{{ target.schema }}`.`run_results_central` {%- endset -%}
  {%- set history_tbl -%} {{target.database}}.`{{ target.schema }}`.`run_results_history` {%- endset -%}

  {% if run_results | length > 0 %}

    {{ log("Centralizing run results in " + central_tbl, info = true) if execute }}
    {% do run_query("create schema if not exists " ~ target.database ~ ".`" ~ target.schema ~ "`") %}

    {%- set central_tbl_query -%}
    create or replace table {{ central_tbl }} as (

    {% for result in run_results %}

      select
        {{ "'" ~ result.node.relation_name ~ "'" if result.node.relation_name else 'null' }} as database_relation_name,
        '{{ result.node.unique_id }}' as model_name,
        '{{ result.status }}' as execution_status,
        {{ result.execution_time }} as execution_time,
        {{ result.adapter_response.get('rows_affected') if result.adapter_response.get('rows_affected') is not none else 'null' }} as rows_affected,
        current_timestamp as _timestamp

      {{ "union all" if not loop.last }}

    {% endfor %}

    )
    {%- endset -%}

    {% do run_query(central_tbl_query) %}
    {{ log("Finished inserting run results in " + central_tbl, info = true) if execute }}

    {# only run historization in higher environments #}
    {% if env_var('DBT_CLOUD_ENVIRONMENT_TYPE', 'dev') != 'dev' %}
      {{ log("Historizing run results in " + history_tbl, info = true) if execute }}
      {%- set history_create_query -%}
      create table if not exists {{ history_tbl }} as (
        select
          {{ dbt_utils.generate_surrogate_key(["database_relation_name", "model_name", "_timestamp"]) }} as sk_id,
          *
        from {{ central_tbl }}
        where false
      )
      {%- endset -%}
      {% do run_query(history_create_query) %}

      {%- set history_insert_query -%}
      insert into {{ history_tbl }}
        select
          {{ dbt_utils.generate_surrogate_key(["database_relation_name", "model_name", "_timestamp"]) }} as sk_id,
          *
        from {{ central_tbl }}
      {%- endset -%}
      {% do run_query(history_insert_query) %}
      {{ log("Finished inserting new run results in " + history_tbl, info = true) if execute }}
    {% endif %}

  {% endif %}

{% endmacro %}
