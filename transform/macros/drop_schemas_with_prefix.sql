{% macro drop_schemas_with_prefix(database_name=none, prefix=none) %}

{#
  This macro drops all schemas in a specified Snowflake database that
  start with a given prefix. It is useful for cleaning up personal
  development and PR schemas. The variables `database_name` and `prefix`
  that are needed for this macro are set in `dbt_project.yml`.

  Arguments:
    - database_name (string, optional): The name of the database where schemas should be dropped.
                                        Defaults to `target.database` if not provided.
    - prefix (string, optional): The prefix that schemas must start with to be considered for dropping.
                                 Defaults to `target.schema` if not provided.
                                 A safety check ensures this default prefix starts with 'DBT_'.
  Invocation examples:
    - Cleanup for your database and dbt schema (uses target.database and target.schema, with safety check):
      dbt run-operation drop_schemas_with_prefix
    - Cleanup for a specific database and/or prefix:
      dbt run-operation drop_schemas_with_prefix --args '{database_name: ANALYTICS_DEV, prefix: DBT_CLOUD_PR...}'
#}

{# Ensure the macro only runs during execution and not parsing #}
{% if execute %}
  {# Determine the effective database name #}
  {% set effective_database_name = database_name %}
  {% if effective_database_name is none %}
    {% set effective_database_name = target.database.upper() %}
  {% endif %}

  {# Determine the effective prefix #}
  {% set effective_prefix = prefix %}
  {% if effective_prefix is none %}
    {% set effective_prefix = target.schema.upper() %}
    {# Safety check to ensure prefix starts with 'DBT_' #}
    {% if not effective_prefix.startswith('DBT_') %}
      {{ exceptions.raise_compiler_error(
        "Error: when 'prefix' is not explicitly provided, it defaults to your target schema (" ~ effective_prefix ~ ")."
        "This default is only allowed however if your target schema starts with 'DBT_' "
        "Please either provide an explicit 'prefix' argument (e.g. --args '{prefix: DBT_...}') or ensure your target schema is prefixed with 'DBT_'."
      )}}
    {% endif %}
  {% endif %}

{{ log("Running schema cleanup for database: " ~ effective_database_name ~ " with prefix: " ~ effective_prefix, info=True) }}

{#
  Query Snowflake to get a list of matching schemas.
#}
{% set get_schemas %}
  SHOW TERSE SCHEMAS IN DATABASE "{{ effective_database_name }}" STARTS WITH '{{ effective_prefix }}'
{% endset %}

{#
  Run the query and store the list.
  dbt's [`run_query` docs](https://docs.getdbt.com/reference/dbt-jinja-functions/run_query).
#}
{% set schemas_to_drop = run_query(get_schemas) %}

{#
  Iterate over each row in the results.
  The schema name is the second column (index 1 or [1]).
#}

{% for row in schemas_to_drop %}
  {% set schema_name = row[1] %}

  {{ log("Dropping schema: " ~ schema_name ~ " in database: " ~ effective_database_name, info=True) }}

  {# Use the adapter.drop_schema function #}
  {% set schema_relation = api.Relation.create(database=effective_database_name, schema=schema_name) %}
  {% do adapter.drop_schema(schema_relation) %}
  {% endfor %}

{# Check if any matching schemas were found. #}
{% if schemas_to_drop | length == 0 %}
  {{ log("No schemas found matching the prefix: " ~ effective_prefix ~ " in database: " ~ effective_database_name ~ ".", info=True) }}
{% else %}
  {{ log("Finished cleanup. Dropped " ~ schemas_to_drop | length ~ " schemas.", info=True) }}
{% endif %}

{% else %}
{# Print a message during parsing phase. #}
{{ log("This macro is being parsed. ", info=True) }}
{% endif %}

{% endmacro %}
