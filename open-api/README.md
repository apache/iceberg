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
