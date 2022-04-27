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

## Testing

Testing is done using tox. The config can be found in `tox.ini` within the python directory of the iceberg project.

```
tox
```

## Solution for `InterpreterNotFound` Errors

Currently, tests run against python `3.7.12`, `3.8.12`, and `3.9.10`. It's recommended to install and manage multiple interpreters using [pyenv](https://github.com/pyenv/pyenv).
```
pyenv install 3.7.12
pyenv install 3.8.12
pyenv install 3.9.10
```

Once all three versions are installed, you can set an application-specific pyenv environment by running the following in the python directory.
```
pyenv local 3.7.12 3.8.12 3.9.10
```

## Get in Touch
- [Iceberg community](https://iceberg.apache.org/community/)
