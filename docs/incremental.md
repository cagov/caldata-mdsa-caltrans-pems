# Incremental models

Since this project involves larger datasets,
dbt [incremental models](https://docs.getdbt.com/docs/build/incremental-models)
are extremely important to save on time and compute costs.
However, they are also more complex to reason about and manage.

## Utility macros

### `get_snowflake_refresh_warehouse()`

TODO

### `make_model_incremental()`

TODO

## Incremental models in CI

TODO

## Full refreshes

!!! note
    This involves changing data in production. It should be done with care!

Any time you change the schema (i.e., the columns or data types) of an incremental model,
this triggers the need to perform a "full refresh" of that model.
This is because the incremental logic typically involves merging the new data with the old data.
With a change in the schema, there can be empty columns and hard-to-debug data quality issues,
or the merge will just fail.
To avoid this, we need to rebuild the production incremental tables from scratch.

Right now, full refreshes are a fairly manual process.

### 1. Create a production target

First, you need to include a new `target` in your `~/.dbt/profiles.yml` file for the production environment.
Here is an example of a `profiles.yml` with both a development and a production target:

```yml
caltrans_pems:
  target: dev
  outputs:
    prd:
      type: snowflake
      account: ngb13288
      user: ian.rose@innovation.ca.gov
      authenticator: externalbrowser
      role: TRANSFORMER_PRD
      database: TRANSFORM_PRD
      warehouse: TRANSFORMING_XS_PRD
      schema: ANALYTICS
      threads: 8
    dev:
      type: snowflake
      account: ngb13288
      user: ian.rose@innovation.ca.gov
      authenticator: externalbrowser
      role: TRANSFORMER_DEV
      database: TRANSFORM_DEV
      warehouse: TRANSFORMING_XS_DEV
      schema: DBT_IROSE
      threads: 8
```

### 2. Use the dbt command line interface to perform the refresh

We next need to run the changed model and all of its downstream dependencies in full refresh mode.
Here is an example of the command line invocation:

```bash
dbt build --target prd --full-refresh --select /path/to/the/model.sql+
```

A few notes about this command:

1. Because we are fully rebuilding large tables with this command, it might take some time.
1. `--target prd` selects the production environment as opposed to the default development one.
    It should be used sparingly, since it changes the data in prod!
1. `--full-refresh` triggers the full refresh. Models that use the `get_snowflake_refresh_warehouse()`
    macro will select a larger Snowflake virtual warehouse when this flag is selected,
    since doing the full refresh typically involves processing much more data.
1. We use the `+` selector to indicate that every downstream model of the one with the schema change
    should also get rebuilt.
