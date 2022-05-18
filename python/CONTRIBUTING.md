# Contributing to the Iceberg Python libary

## Linting

We rely on `pre-commit` to apply autoformatting and linting:

```bash
pip install -e ".[dev,arrow]"
make lint
```

By default, it only runs on the files known by git.

Pre-commit will automatically fix the violations such as import orders, formatting etc. Pylint errors you need to fix yourself.

In contrast to the name, it doesn't run on the git pre-commit hook by default. If this is something that you like, you can set this up by running `pre-commit install`.

## Testing

For Python, we use pytest in combination with coverage to maintain 90% code coverage

```bash
pip install -e ".[dev,arrow]"
make test
```
