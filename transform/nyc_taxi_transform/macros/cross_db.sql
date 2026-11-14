{#
  The only two date functions whose names differ between Athena (Trino) and DuckDB.
  dbt picks the implementation for the active adapter: duckdb__* locally,
  default__* (Trino syntax) on Athena.
#}

{% macro iso_day_of_week(column) %}
    {{ return(adapter.dispatch('iso_day_of_week', 'nyc_taxi_transform')(column)) }}
{% endmacro %}

{% macro default__iso_day_of_week(column) %}day_of_week({{ column }}){% endmacro %}

{% macro duckdb__iso_day_of_week(column) %}isodow({{ column }}){% endmacro %}


{% macro year_month(column) %}
    {{ return(adapter.dispatch('year_month', 'nyc_taxi_transform')(column)) }}
{% endmacro %}

{% macro default__year_month(column) %}date_format({{ column }}, '%Y-%m'){% endmacro %}

{% macro duckdb__year_month(column) %}strftime({{ column }}, '%Y-%m'){% endmacro %}
