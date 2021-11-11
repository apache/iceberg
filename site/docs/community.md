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

# Welcome!

Apache Iceberg tracks issues in GitHub and prefers to receive contributions as pull requests.

Community discussions happen primarily on the dev mailing list, on apache-iceberg Slack workspace, and on specific GitHub issues.


## Contributing

The Iceberg Project is hosted on Github at <https://github.com/apache/iceberg>.

The Iceberg community prefers to receive contributions as [Github pull requests][github-pr-docs].

* [View open pull requests][iceberg-prs]
* [Learn about pull requests][github-pr-docs]

[iceberg-prs]: https://github.com/apache/iceberg/pulls
[github-pr-docs]: https://help.github.com/articles/about-pull-requests/


## Issues

Issues are tracked in GitHub:

* [View open issues][open-issues]
* [Open a new issue][new-issue]

[open-issues]: https://github.com/apache/iceberg/issues
[new-issue]: https://github.com/apache/iceberg/issues/new

## Slack

We use the [Apache Iceberg workspace](https://apache-iceberg.slack.com/) on Slack. To be invited, follow [this invite link](https://join.slack.com/t/apache-iceberg/shared_invite/zt-tlv0zjz6-jGJEkHfb1~heMCJA3Uycrg).

Please note that this link may occasionally break when Slack does an upgrade. If you encounter problems using it, please let us know by sending an email to <dev@iceberg.apache.org>.

## Mailing Lists

Iceberg has four mailing lists:

* **Developers**: <dev@iceberg.apache.org> -- used for community discussions
    - [Subscribe](mailto:dev-subscribe@iceberg.apache.org)
    - [Unsubscribe](mailto:dev-unsubscribe@iceberg.apache.org)
    - [Archive](https://lists.apache.org/list.html?dev@iceberg.apache.org)
* **Commits**: <commits@iceberg.apache.org> -- distributes commit notifications
    - [Subscribe](mailto:commits-subscribe@iceberg.apache.org)
    - [Unsubscribe](mailto:commits-unsubscribe@iceberg.apache.org)
    - [Archive](https://lists.apache.org/list.html?commits@iceberg.apache.org)
* **Issues**: <issues@iceberg.apache.org> -- Github issue tracking
    - [Subscribe](mailto:issues-subscribe@iceberg.apache.org)
    - [Unsubscribe](mailto:issues-unsubscribe@iceberg.apache.org)
    - [Archive](https://lists.apache.org/list.html?issues@iceberg.apache.org)
* **Private**: <private@iceberg.apache.org> -- private list for the PMC to discuss sensitive issues related to the health of the project
    - [Archive](https://lists.apache.org/list.html?private@iceberg.apache.org)


## Setting up IDE and Code Style

### Configuring Code Formatter for IntelliJ IDEA

In the **Settings/Preferences** dialog go to **Editor > Code Style > Java**. Click on the gear wheel and select **Import Scheme** to import IntelliJ IDEA XML code style settings.
Point to [intellij-java-palantir-style.xml](https://github.com/apache/iceberg/blob/master/.baseline/idea/intellij-java-palantir-style.xml) and hit **OK** (you might need to enable **Show Hidden Files and Directories** in the dialog). The code itself can then be formatted via **Code > Reformat Code**.

See also the IntelliJ [Code Style docs](https://www.jetbrains.com/help/idea/copying-code-style-settings.html) and [Reformat Code docs](https://www.jetbrains.com/help/idea/reformat-and-rearrange-code.html) for additional details.

## Running Benchmarks
Some PRs/changesets might require running benchmarks to determine whether they are affecting the baseline performance. Currently there is 
no "push a single button to get a performance comparison" solution available, therefore one has to run JMH performance tests on their local machine and
post the results on the PR.

See [Benchmarks](benchmarks.md) for a summary of available benchmarks and how to run them.
