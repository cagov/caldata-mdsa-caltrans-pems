# Repository setup

## Install dependencies

### 1. Set up a Python virtual environment

Much of the software in this project is written in Python.
It is usually worthwhile to install Python packages into a virtual environment,
which allows them to be isolated from those in other projects which might have different version constraints.

One popular solution for managing Python environments is [Anaconda/Miniconda](https://docs.conda.io/en/latest/miniconda.html).
Another option is to use [`pyenv`](https://github.com/pyenv/pyenv).
Pyenv is lighter weight, but is Python-only, whereas conda allows you to install packages from other language ecosystems.

Here are instructions for setting up a Python environment using Miniconda:

1. Follow the installation instructions for installing [Miniconda](https://docs.conda.io/en/latest/miniconda.html#system-requirements).
1. Create a new environment called `dev`:
   ```bash
   conda create -n infra -c conda-forge python=3.10 poetry
   ```
1. Activate the `dev` environment:
   ```bash
   conda activate dev
   ```

### 2. Install Python dependencies

Python dependencies are specified using [`poetry`](https://python-poetry.org/).

To install them, open a terminal and enter:

```bash
poetry install
```

Any time the dependencies change, you can re-run the above command to update them.

## Configure Snowflake

In order to use Snowflake (as well as the terraform validators for the Snowflake configuration)
you should set some default local environment variables in your environment.
This will depend on your operating system and shell. For Linux and Mac OS systems,
as well as users of Windows subsystem for Linux (WSL) it's often set in
`~/.zshrc`, `~/.bashrc`, or `~/.bash_profile`.

If you use zsh or bash, open your shell configuration file, and add the following lines:

**Default Transformer role**

```bash
export SNOWFLAKE_ACCOUNT=<account-locator>
export SNOWFLAKE_USER=<your-username>
export SNOWFLAKE_PASSWORD=<your-password>
export SNOWFLAKE_ROLE=TRANSFORMER_DEV
export SNOWFLAKE_WAREHOUSE=TRANSFORMING_XS_DEV
```

This will enable you to perform transforming activities which is needed for dbt.
Open a new terminal and verify that the environment variables are set.

**Switch to Loader role**

```bash
export SNOWFLAKE_ACCOUNT=<account-locator>
export SNOWFLAKE_USER=<your-username>
export SNOWFLAKE_PASSWORD=<your-password>
export SNOWFLAKE_ROLE=LOADER_DEV
export SNOWFLAKE_WAREHOUSE=LOADING_XS_DEV
```

This will enable you to perform loading activities and is needed to which is needed for Airflow or Fivetran.
Again, open a new terminal and verify that the environment variables are set.

## Configure dbt

The connection information for our data warehouses will,
in general, live outside of this repository.
This is because connection information is both user-specific usually sensitive,
so should not be checked into version control.
In order to run this project locally, you will need to provide this information
in a YAML file located (by default) in `~/.dbt/profiles.yml`.

Instructions for writing a `profiles.yml` are documented
[here](https://docs.getdbt.com/docs/get-started/connection-profiles),
as well as specific instructions for
[Snowflake](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup)
and [BigQuery](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup).

You can verify that your `profiles.yml` is configured properly by running

```bash
dbt debug
```

from the dbt project root directory (`transform`).

### Snowflake project

A minimal version of a `profiles.yml` for dbt development with is:

```yml
caltrans_pems:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <account-locator>
      user: <your-username>
      password: <your-password>
      role: TRANSFORMER_DEV
      database: TRANSFORM_DEV
      warehouse: TRANSFORMING_XS_DEV
      schema: DBT_<your-name>   # Test schema for development
      threads: 4
```



## Installing `pre-commit` hooks

This project uses [pre-commit](https://pre-commit.com/) to lint, format,
and generally enforce code quality. These checks are run on every commit,
as well as in CI.

To set up your pre-commit environment locally run

```bash
pre-commit install
```

The next time you make a commit, the pre-commit hooks will run on the contents of your commit
(the first time may be a bit slow as there is some additional setup).

You can verify that the pre-commit hooks are working properly by running

```bash
pre-commit run --all-files
```
to test every file in the repository against the checks.

Some of the checks lint our dbt models,
so having the dbt project configured is a requirement to run them,
even if you don't intend to use those packages.
