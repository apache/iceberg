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

# Contributing

In this document, you will find some guidelines on contributing to Apache Iceberg. Please keep in mind that none of these are hard rules and they're meant as a collection of helpful suggestions to make contributing as seamless of an experience as possible.

If you are thinking of contributing but first would like to discuss the change you wish to make, we welcome you to head over to the [Community](https://iceberg.apache.org/community/) page on the official Iceberg documentation site to find a number of ways to connect with the community, including slack and our mailing lists. Of course, always feel free to just open a [new issue](https://github.com/apache/iceberg/issues/new) in the GitHub repo.

## Pull Request Process

Pull requests are the preferred mechanism for contributing to Iceberg. PRs will be automatically labeled based on the content by our github-actions labeling bot. Although it is not required, if the PR is related to an issue, it's helpful to prefix your PR title with the issue number surrounded by brackets, for example `[1234] <PR title>`. Another common practice is to prefix the PR title with `[WIP]` if it's a PR that you opened for visibility and may not necessarily be ready for review or merging.

## Styling

For Java styling, check out the section [Setting up IDE and Code Style](https://iceberg.apache.org/community/#setting-up-ide-and-code-style) from the documentation site. For Python, please use the tox command `tox -e format` to apply autoformatting to the project.

## Building the Project Locally

Please refer to the [Building](https://github.com/apache/iceberg#building) section of the main readme for instructions on how to build iceberg locally.
