# Writing Documentation

Documentation for this project is built using [mkdocs](https://www.mkdocs.org/)
with the [material theme](https://squidfunk.github.io/mkdocs-material/)
and hosted using [GitHub Pages](https://pages.github.com/).
The documentation source files are in the `docs/` directory
and are authored using [markdown](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax).

## Local Developments

To write documentation for this project, make sure that the repository is [set up](./setup.md).
You should then be able to start a local server for the docs:

```bash
mkdocs serve
```

Then open a web browser to [http://localhost:8000](http://localhost:8000) to view the built docs.
Any edits you make to the markdown sources should be automatically picked up,
and the page should automatically rebuild and refresh.

## Deployment

Deployment of the docs for this repository is done automatically upon merging to `main`
using the `docs` GitHub Action.

Built documentation is pushed to the `gh-pages` branch of the repository,
and can be viewed by navigating to the GitHub pages URL for the project.
