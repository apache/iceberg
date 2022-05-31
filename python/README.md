<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Iceberg Python

py-iceberg is a python library for programmatic access to iceberg table metadata as well as to table data in iceberg format.
It is an implementation of [iceberg table spec](https://iceberg.apache.org/spec/) in Python.

## Getting Started

py-iceberg is currently in development, for development and testing purposes the best way to install the library is to perform the following steps:

```
git clone https://github.com/apache/iceberg.git
cd iceberg/python
pip install -e .
```

Development is made easy using [Poetry](https://python-poetry.org/docs/#installation).

## Development

Poetry provides virtual environments for development:

```bash
poetry shell
poetry install -E pyarrow
pytest
```

For more information, please refer to the [Manage environments](https://python-poetry.org/docs/managing-environments/) section of Poetry.

## Testing

Testing is done using Poetry:

#### `s3` Tests

A subset of tests are decorated using `@pytest.mark.s3` and require an S3 protocol compliant object storage to use for testing. For the python CI, a local MinIO container is used. To run these tests, you can provide a pass-through argument to tox (using `--`) that will be passed to the `pytest` command.

```
tox -- -m "s3"
```

Additional arguments can be passed to configure the object storage.

| Argument                | Default           | Description                                         |
|-------------------------|-------------------|-----------------------------------------------------|
| --endpoint-url          | http://minio:9000 | The S3 endpoint URL for tests marked as s3          |
| --aws-access-key-id     | admin             | The AWS access key ID for tests marked as s3        |
| --aws-secret-access-key | password          | The AWS secret access key ID for tests marked as s3 |

For example, to run `s3` tests against an object store running locally on port 4000 with an access key ID of `foo` and an secret access key as `bar`, you would use the following command.

```
tox -- -m "s3" --endpoint-url=http://localhost:4000 --aws-access-key-id=foo --aws-secret-access-key=bar
```

## Solution for `InterpreterNotFound` Errors

Currently, tests run against python `3.7.12`, `3.8.12`, and `3.9.10`. It's recommended to install and manage multiple interpreters using [pyenv](https://github.com/pyenv/pyenv).
```
poetry install -E pyarrow
poetry run pytest
```

## Get in Touch
- [Iceberg community](https://iceberg.apache.org/community/)
