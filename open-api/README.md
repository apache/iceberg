# Open API spec

The `rest-catalog-open-api.yaml` defines the REST catalog interface.

## Lint

To make sure that the open-api definition is valid, you can run the `lint` command:

```sh
make install
make lint
```

## Generate Python code

When reviewing changes in the spec, it helps to see what kind of code is being generated from the spec. We generate Python code to make this process easier. Before committing, make sure to run:

```sh
make install
make generate
```

To update `rest-catalog-open-api.py` to the latest version.
