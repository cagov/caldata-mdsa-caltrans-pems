# Repository setup

These are instructions for individual contributors to set up the repository locally.

## Install dependencies

We use `uv` to manage our Python virtual environments.
If you have not yet installed it on your system,
you can follow the instructions for it [here](https://docs.astral.sh/uv/getting-started/installation/).
Most of the ODI team uses [Homebrew](https://brew.sh) to install the package.
We do not recommend installing `uv` using `pip`: as a tool for managing Python environments,
it makes sense for it to live outside of a particular Python distribution.

### 2. Install Python dependencies

If you prefix your commands with `uv run` (e.g. `uv run dbt build`),
then `uv` will automatically make sure that the appropriate dependencies are installed before invoking the command.

However, if you want to explicitly ensure that all of the dependencies are installed in the virtual environment, run
```bash
uv sync
```
in the root of the repository.

Once the dependencies are installed, you can also "activate" the virtual environment
(similar to how conda virtual environments are activated) by running
```bash
source .venv/bin/activate
```
from the repository root.
With the environment activated, you no longer have to prefix commands with `uv run`.

Which approach to take is largely a matter of personal preference:

- Using the `uv run` prefix is more reliable, as dependencies are *always* resolved before executing.
- Using `source .venv/bin/activate` involves less typing.

### 3. Install dbt dependencies

dbt comes with some core libraries, but others can be added on.
This command installs those extra libraries that are needed for this repo.
It will have to be re-run each time the repo starts relying on a new dbt library.

To install dbt dependencies, open a terminal and enter:

```bash
uv run dbt deps --project-dir transform
```

## Configure Snowflake

In order to use Snowflake (as well as the terraform validators for the Snowflake configuration)
you should set some default local environment variables in your environment.
This will depend on your operating system and shell. For Linux and Mac OS systems,
as well as users of Windows subsystem for Linux (WSL) it's often set in
`~/.zshrc`, `~/.bashrc`, or `~/.bash_profile`.

If you use zsh or bash, open your shell configuration file, and add the following lines:

**Default Transformer role**

```bash
# Legacy account identifier
export SNOWFLAKE_ACCOUNT=<account-locator>
# The preferred account identifier is to use name of the account prefixed by its organization (e.g. myorg-account123)
# Supporting snowflake documentation - https://docs.snowflake.com/en/user-guide/admin-account-identifier
export SNOWFLAKE_ACCOUNT=<org_name>-<account_name> # format is organization-account
export SNOWFLAKE_USER=<your-username>
export SNOWFLAKE_PASSWORD=<your-password>
export SNOWFLAKE_ROLE=TRANSFORMER_DEV
export SNOWFLAKE_WAREHOUSE=TRANSFORMING_XS_DEV
```

This will enable you to perform transforming activities which is needed for dbt.
Open a new terminal and verify that the environment variables are set.

**Switch to Loader role**

```bash
# Legacy account identifier
export SNOWFLAKE_ACCOUNT=<account-locator>
# The preferred account identifier is to use name of the account prefixed by its organization (e.g. myorg-account123)
# Supporting snowflake documentation - https://docs.snowflake.com/en/user-guide/admin-account-identifier
export SNOWFLAKE_ACCOUNT=<org_name>-<account_name> # format is organization-account
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
in a YAML file located (by default) in `~/.dbt/profiles.yml` (where `~` indicates your home directory).
To see the absolute path of where dbt is looking for your `profiles.yml`,
run `dbt debug` and inspect the output.

Instructions for writing a `profiles.yml` are documented
[here](https://docs.getdbt.com/docs/get-started/connection-profiles),
as well as specific instructions for
[Snowflake](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup).

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
      authenticator: username_password_mfa
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
