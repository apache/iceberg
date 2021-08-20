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

## Apache Iceberg Site

This directory contains the source for the Iceberg site.

* Site structure is maintained in mkdocs.yml
* Pages are maintained in markdown in the `docs/` folder
* Links use bare page names: `[link text](target-page)`

### Installation

The site is built using mkdocs. To install mkdocs and the theme, run:

```
pip install -r requirements.txt
```

### Local Changes

To see changes locally before committing, use mkdocs to run a local server from this directory.

```
mkdocs serve
```

To see changes in Javadoc, run:

```
./gradlew refreshJavadoc
```

### Publishing

After site changes are committed, you can publish the site with this command:

```
./gradlew deploySite
```

This assumes that the Apache remote is named `apache` and will push to the
`asf-site` branch. You can specify the name of a different remote by appending
`-Premote.name=<remote-name>` to the `./gradlew deploySite` command.
