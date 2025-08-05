# Incremental models

Since this project involves larger datasets,
dbt [incremental models](https://docs.getdbt.com/docs/build/incremental-models)
are extremely important to save on time and compute costs.
However, they are also more complex to reason about and manage.
Here we document some guidelines and best practices for working with
incremental models in this project.

## Incremental models in CI

Because incremental models tend to be larger, we usually don't want to commit to a full rebuild of them in CI.
For this reason, we create [zero-copy clones](https://docs.snowflake.com/en/user-guide/tables-storage-considerations#label-cloning-tables)
of incremental models *before* running CI, allowing them to be built incrementally:

```bash
dbt clone --select state:modified+,config.materialized:incremental,state:old
```

Cloning of incremental models before building them *can* result in difficult-to-debug
CI issues if there are schema changes to the model.
There are no hard-and-fast rules or guidelines for resolving these issues when they arise,
it takes careful thought and possibly tweaking the
[`on_schema_change`](https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change)
config parameter.

## Backfills/refreshes of incremental models

!!! note
    This involves changing data in production. It should be done with care!

"Backfilling" refers to re-running data loading and transformation tasks on historical data.
You might need to do this for a few reasons:

1. You've identified missing or corrupted data from some incident.
1. You've implemented some bugfix or enhancement to your data processing logic,
    and want it to be reflected in historical data.
1. You refactor or rename parts of your data models.

Any time one of the above happens, you should backfill the data in your incremental models.
This is because the incremental logic typically involves merging the new data with the old data:
any significant changes can result in (at best) empty data or (at worst) hard-to-debug data quality issues.
Or, if the changes are big enough, the merge will just fail.

Currently, backfills of incremental models are a fairly manual process.

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
      user: <your-login-name>
      authenticator: externalbrowser
      role: TRANSFORMER_PRD
      database: TRANSFORM_PRD
      warehouse: TRANSFORMING_XS_PRD
      schema: ANALYTICS
      threads: 8
    dev:
      type: snowflake
      account: ngb13288
      user: <your-login-name>
      authenticator: externalbrowser
      role: TRANSFORMER_DEV
      database: TRANSFORM_DEV
      warehouse: TRANSFORMING_XS_DEV
      schema: <your-dev-schema>
      threads: 8
```

### 2. Use the dbt command line interface to perform the backfill

Most (but not all) of the incremental models in this project use dbt's
[microbatch](https://docs.getdbt.com/docs/build/incremental-microbatch) strategy.
This breaks up incremental builds into batches of fixed size (we use day-sized batches).
Under normal operations, microbatch builds fill in the most recent data,
but they can also be used to backfill specific date ranges for the table using the
`--event-time-start` and `--event-time-end` command line arguments.

!!! note "Why the `--full-refresh` flag?"
    Not every model in the project uses microbatch logic. For instance, the models
    computing regression coefficients use a different incremental logic.
    However, these other models have complex inter-dependencies with microbatch incremental models,
    which makes it very difficult to know if a targeted backfill of a microbatch model
    will affect the non-microbatch incremental model.

    For this reason, we recommend that you *also* include the `--full-refresh` flag in any backfill operations.
    This triggers a full rebuild of the non-microbatch incremental models any time a backfill is happening,
    regardless of whether the backfill covers the full project date range.
    This ensures that the non-microbatch incremental models are are always in sync with the microbatch ones.

    The `--full-refresh` flag also triggers larger compute resources in the `get_snowflake_refresh_warehouse()`
    macro (see [below](#utility-macros) for more information).

One significant pitfall of backfilling PeMS data is in the imputation logic.
The data imputation models fill gaps or unreliable vehicle detector station data
with [imputed](https://en.wikipedia.org/wiki/Imputation_(statistics)) values
based on *historical* behavior of the detectors and their neighbors.
Since this relies on the historical data being present,
it's important that the imputation-related models and anything that depends on them
be run *after* any data backfilling is complete,
otherwise the imputation may be relying on missing or bad data.
An indicator that a model depends on historical data is if it uses an
[`asof` join](https://docs.snowflake.com/en/sql-reference/constructs/asof-join).

To make historical backfills easier, we've taken the following approach:

1. All models that directly rely on data history being complete are grouped together in `intermediate/imputation`.
1. We've provided a series of [dbt selectors](https://docs.getdbt.com/reference/node-selection/yaml-selectors)
    to make it easier to break the backfill into steps:

    1. `prep`: data cleaning, preparation, and diagnostics.
        These models do not depend on any history, and should be fully backfilled before
        moving on to the next selector.
    1. `imputation`: data imputation. These models rely on historical data being complete.
    1. `performance`: performance metrics and data marts. Most of these rely on imputation
        being complete.

A recipe for backfilling production data would then look like:

```bash
# Run the data preparation stage. This could be broken into multiple
# steps to cover large date ranges.
uv run dbt build \
    --target prd \
    --full-refresh \
    --event-time-start '<backfill start date>' \
    --event-time-end '<backfill end date>' \
    --selector prep

# Run the impuation stage. This should be done after the "prep"
# stage is fully backfilled.
uv run dbt build \
    --target prd \
    --full-refresh \
    --event-time-start '<backfill start date>' \
    --event-time-end '<backfill end date>' \
    --selector imputation

# Build the performance metrics and marts
uv run dbt build \
    --target prd \
    --full-refresh \
    --event-time-start '<backfill start date>' \
    --event-time-end '<backfill end date>' \
    --selector performance
```

A few notes about this recipe:

1. Because we are typically rebuilding large tables with this command, it might take some time.
1. `--target prd` selects the production environment as opposed to the default development one.
    It should be used sparingly, since it changes the data in prod!
    You can test your backfills in the development environment using the `dbt clone` trick that CI uses.
1. `--full-refresh` triggers the full refresh of non-microbatch incremental models.
    Models that use the `get_snowflake_refresh_warehouse()`
    macro will select a larger Snowflake virtual warehouse when this flag is selected,
    since doing the full refresh typically involves processing much more data.
1. For *extremely* large backfills, you can consider breaking it up into multiple steps by, e.g.,
    selecting one year at a time using the `--event-time-start` and `--event-time-end` arguments.

## Utility macros

### `get_snowflake_refresh_warehouse()`

Backfills and full refreshes of incremental models can process significantly more data than a normal incremental build.
For those cases, it is nice to be able to use a larger Snowflake warehouse to cut down on build times.

This project contains a macro called `get_snowflake_refresh_warehous()` which can be used
when configuring the `snowflake_warehouse` parameter in incremental models.
This macro dispatches to a larger warehouse when the `--full-refresh` flag is set in
the dbt build, and dispatches to a smaller warehouse when it is not set.
The macro takes two (optional) arguments, `big` and `small`.
The `small` argument determines which size warehouse should be used in the incremental context,
and the `big` argument determines which size warehouse should be used in the backfill/refresh context.
