<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Contributing to the Iceberg Python library

For the development, we use poetry for packing and dependency management. You can install this using:

```bash
pip install poetry
```

If you have an older version of pip and virtualenv you need to update these:
```bash
pip install --upgrade virtualenv pip
```

To get started, you can run `make install`, which will install poetry and it will install all the dependencies of the Iceberg library. This will also install the development dependencies. If you don't want to do this, you need to install using `poetry install --no-dev`.

If you want to install the library on the host, you can simply run `pip3 install -e .`. If you wish to use a virtual environment, you can run `poetry shell`. Poetry will open up a virtual environment with all the dependencies set.

To set up IDEA with Poetry ([also on Loom](https://www.loom.com/share/6d36464d45f244729d91003e7f671fd2)):

- Open up the Python project in IntelliJ
- Make sure that you're on a lastest master (that includes Poetry)
- Go to File -> Project Structure (⌘;)
- Go to Platform Settings -> SDKs
- Click the + sign -> Add Python SDK
- Select Poetry Environment from the left hand side bar and hit OK
- It can take some time to download all the dependencies based on your internet
- Go to Project Settings -> Project
- Select the Poetry SDK from the SDK dropdown, and click OK

For IDEA ≤2021 you need to install the [Poetry integration as a plugin](https://plugins.jetbrains.com/plugin/14307-poetry/).

Now you're set using Poetry, and all the tests will run in Poetry, and you'll have syntax highlighting in the pyproject.toml to indicate stale dependencies.

## Linting

We rely on `pre-commit` to apply autoformatting and linting:

```bash
make lint
```

Pre-commit will automatically fix the violations such as import orders, formatting etc. Pylint errors you need to fix yourself.

In contrast to the name suggest, it doesn't run the checks on the commit. If this is something that you like, you can set this up by running `pre-commit install`.

You can bump the integrations to the latest version using `pre-commit autoupdate`. This will check if there is a newer version of `{black,mypy,isort,...}` and update the yaml.

## Testing

For Python, we use pytest in combination with coverage to maintain 90% code coverage.

```bash
make test
```

By default, `make test` only runs tests that are suitable for local development, with no additional infrastructure.
You can run additional tests if you are within an environment that meets the additional requirements.

Make Command        | Description                            | Requirements                                                                                                                                                   |
--------------------|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
make test           | Run standard tests                     | No infrastructure requirements, simply run `make install` first to install poetry and the required test dependencies                                           |
make test-s3        | Run S3 based tests                     | An S3 protocol compatibility object store running at http://localhost:9000 with an `aws-access-key-id` of `admin` and an `aws-secret-access-key` of `password` |
make test-all       | Run all tests                          | All requirements for other `make test-*` commands                                                                                                              |

The Python CI that runs automatically for PRs and merges uses the `make test-all` command.
