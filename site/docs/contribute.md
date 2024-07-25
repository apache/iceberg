---
title: "Contribute"
---
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
# Contributing

In this page, you will find some guidelines on contributing to Apache Iceberg. Please keep in mind that none of
these are hard rules and they're meant as a collection of helpful suggestions to make contributing as seamless of an
experience as possible.

If you are thinking of contributing but first would like to discuss the change you wish to make, we welcome you to
head over to the [Community](community.md) page on the official Iceberg documentation site
to find a number of ways to connect with the community, including slack and our mailing lists. Of course, always feel
free to just open a [new issue](https://github.com/apache/iceberg/issues/new) in the GitHub repo. You can also check the following for a [good first issue](https://github.com/apache/iceberg/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).

The Iceberg Project is hosted on GitHub at <https://github.com/apache/iceberg>.

## Pull Request Process

The Iceberg community prefers to receive contributions as [Github pull requests][github-pr-docs].

[View open pull requests][iceberg-prs]


[iceberg-prs]: https://github.com/apache/iceberg/pulls
[github-pr-docs]: https://help.github.com/articles/about-pull-requests/

* PRs are automatically labeled based on the content by our github-actions labeling action
* It's helpful to include a prefix in the summary that provides context to PR reviewers, such as `Build:`, `Docs:`, `Spark:`, `Flink:`, `Core:`, `API:`
* If a PR is related to an issue, adding `Closes #1234` in the PR description will automatically close the issue and helps keep the project clean
* If a PR is posted for visibility and isn't necessarily ready for review or merging, be sure to convert the PR to a draft

### Merging Pull Requests

Most pull requests can be merged once a committer is satisfied with the code in the PR. For larger changes or additions to public
APIs committers will wait at least 24 hours before merging to ensure there is no additional feedback from members of the community. As in
all ASF governed projects committers are expected to act in the best interest of the project. If a committer feels there might be a conflict
of interest with a pull request they review, they are encouraged to ask for another committer to look at the pull request.

There are several exceptions to this process:

* Large changes or functional changes to a specification must go through the an [Iceberg improvement proposal](#apache-iceberg-improvement-proposals) before any code can be merged.
* Changes to files under the `format` directory and `open-api/rest-catalog*` are considered specification changes. Unless already covered under an Iceberg improvement proposal, specification changes require their own vote (e.g. bug fixes or specification clarifications). The vote follows the ASF [code modification](https://www.apache.org/foundation/voting.html#votes-on-code-modification) model with three positive PMC votes required and no lazy consensus modifier. Grammar, spelling and minor formatting fixes are exempted from this rule.

## Apache Iceberg Improvement Proposals

### What is an improvement proposal?

An improvement proposal is a major change to Apache Iceberg that may require changes to an existing specification, creation
of a new specification, or significant additions/changes to any of the existing Iceberg implementations.  Changes that are large in
scope need to be considered carefully and incorporate feedback from many community stakeholders.

### What should a proposal include?

1. A GitHub issue created using the `Apache Iceberg Improvement Proposal` template
2. A document including the following:
    * Motivation for the change 
    * Implementation proposal 
    * Breaking changes/incompatibilities 
    * Alternatives considered
3. A discussion thread initiated in the dev list with the Subject: '[DISCUSS] <proposal title\>'

### Who can submit a proposal?

Anyone can submit a proposal, but be considerate and submit only if you plan on contributing to the implementation.

### Where can I find current proposals?

Current proposals are tracked in GitHub issues with the label [Proposal][iceberg-proposals]

### How are proposals adopted?

Once general consensus has been reached, a vote should be raised on the dev list.  The vote follows the ASF 
[code modification][apache-vote] model with three positive PMC votes required and no lazy consensus modifier.
The voting process should be held in good faith to reinforce and affirm the agreed upon proposal, not to 
settle disagreements or to force a decision.

[iceberg-proposals]: https://github.com/apache/iceberg/issues?q=is%3Aissue+is%3Aopen+label%3Aproposal+
[apache-vote]: https://www.apache.org/foundation/voting.html#apache-voting-process

## Building the Project Locally

Iceberg is built using Gradle with Java 8, 11, or 17.

* To invoke a build and run tests: `./gradlew build`
* To skip tests: `./gradlew build -x test -x integrationTest`
* To fix code style: `./gradlew spotlessApply`
* To build particular Spark/Flink Versions: `./gradlew build -DsparkVersions=3.4,3.5 -DflinkVersions=1.14`

Iceberg table support is organized in library modules:

* `iceberg-common` contains utility classes used in other modules
* `iceberg-api` contains the public Iceberg API
* `iceberg-core` contains implementations of the Iceberg API and support for Avro data files, **this is what processing engines should depend on**
* `iceberg-parquet` is an optional module for working with tables backed by Parquet files
* `iceberg-arrow` is an optional module for reading Parquet into Arrow memory
* `iceberg-orc` is an optional module for working with tables backed by ORC files
* `iceberg-hive-metastore` is an implementation of Iceberg tables backed by the Hive metastore Thrift client
* `iceberg-data` is an optional module for working with tables directly from JVM applications

This project Iceberg also has modules for adding Iceberg support to processing engines:

* `iceberg-spark` is an implementation of Spark's Datasource V2 API for Iceberg with submodules for each spark versions (use runtime jars for a shaded version)
* `iceberg-flink` contains classes for integrating with Apache Flink (use iceberg-flink-runtime for a shaded version)
* `iceberg-mr` contains an InputFormat and other classes for integrating with Apache Hive
* `iceberg-pig` is an implementation of Pig's LoadFunc API for Iceberg

## Setting up IDE and Code Style

### Configuring Code Formatter for Eclipse/IntelliJ

Follow the instructions for [Eclipse](https://github.com/google/google-java-format#eclipse) or
[IntelliJ](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides) to install the **google-java-format** plugin (note the required manual actions for IntelliJ).


## Semantic Versioning

Apache Iceberg leverages [semantic versioning](https://semver.org/#semantic-versioning-200) to ensure compatibility
for developers and users of the iceberg libraries as APIs and implementations evolve.
The requirements and guarantees provided depend on the subproject as described below:

### Major Version Deprecations Required

__Modules__
`iceberg-api`

The API subproject is the main interface for developers and users of the Iceberg API and therefore has the strongest
guarantees.
Evolution of the interfaces in this subproject are enforced by [Revapi](https://revapi.org/) and require
explicit acknowledgement of API changes.

All public interfaces and classes require one major version for deprecation cycle.
Any backward incompatible changes should be annotated as `@Deprecated` and removed for the next major release.
Backward compatible changes are allowed within major versions.

### Minor Version Deprecations Required

__Modules__
`iceberg-common`
`iceberg-core`
`iceberg-data`
`iceberg-orc`
`iceberg-parquet`

Changes to public interfaces and classes in the subprojects listed above require a deprecation cycle of one minor
release.

These projects contain common and internal code used by other projects and can evolve within a major release.
Minor release deprecation will provide other subprojects and external projects notice and opportunity to transition
to new implementations.

### Minor Version Deprecations Discretionary

__modules__ (All modules not referenced above)

Other modules are less likely to be extended directly and modifications should make a good faith effort to follow a
minor version deprecation cycle.

If there are significant structural or design changes that result in deprecations
being difficult to orchestrate, it is up to the committers to decide if deprecation is necessary.

## Deprecation Notices

All interfaces, classes, and methods targeted for deprecation must include the following:

1. `@Deprecated` annotation on the appropriate element
2. `@depreceted` javadoc comment including: the version for removal, the appropriate alternative for usage
3. Replacement of existing code paths that use the deprecated behavior

Example:

```java
  /**
   * Set the sequence number for this manifest entry.
   *
   * @param sequenceNumber a sequence number
   * @deprecated since 1.0.0, will be removed in 1.1.0; use dataSequenceNumber() instead.
   */
  @Deprecated
  void sequenceNumber(long sequenceNumber);
```

## Adding new functionality without breaking APIs
When adding new functionality, make sure to avoid breaking existing APIs, especially within the scope of the API modules that are being checked by [Revapi](https://revapi.org/).

Assume adding a `createBranch(String name)` method to the `ManageSnapshots` API.

The most straight-forward way would be to add the below code:

```java
public interface ManageSnapshots extends PendingUpdate<Snapshot> {
  // existing code...

  // adding this method introduces an API-breaking change
  ManageSnapshots createBranch(String name);
}
```

And then add the implementation:

```java
public class SnapshotManager implements ManageSnapshots {
  // existing code...

  @Override
  public ManageSnapshots createBranch(String name, long snapshotId) {
    updateSnapshotReferencesOperation().createBranch(name, snapshotId);
    return this;
  }
}
```

### Checking for API breakages

Running `./gradlew revapi` will flag this as an API-breaking change:

```
./gradlew revapi
> Task :iceberg-api:revapi FAILED
> Task :iceberg-api:showDeprecationRulesOnRevApiFailure FAILED

1: Task failed with an exception.
-----------
* What went wrong:
Execution failed for task ':iceberg-api:revapi'.
> There were Java public API/ABI breaks reported by revapi:

  java.method.addedToInterface: Method was added to an interface.

  old: <none>
  new: method org.apache.iceberg.ManageSnapshots org.apache.iceberg.ManageSnapshots::createBranch(java.lang.String)

  SOURCE: BREAKING, BINARY: NON_BREAKING, SEMANTIC: POTENTIALLY_BREAKING

  From old archive: <none>
  From new archive: iceberg-api-1.4.0-SNAPSHOT.jar

  If this is an acceptable break that will not harm your users, you can ignore it in future runs like so for:

    * Just this break:
        ./gradlew :iceberg-api:revapiAcceptBreak --justification "{why this break is ok}" \
          --code "java.method.addedToInterface" \
          --new "method org.apache.iceberg.ManageSnapshots org.apache.iceberg.ManageSnapshots::createBranch(java.lang.String)"
    * All breaks in this project:
        ./gradlew :iceberg-api:revapiAcceptAllBreaks --justification "{why this break is ok}"
    * All breaks in all projects:
        ./gradlew revapiAcceptAllBreaks --justification "{why this break is ok}"
  ----------------------------------------------------------------------------------------------------

```

### Adding a default implementation

To avoid breaking the API, add a default implementation that throws an `UnsupportedOperationException`:`

```java
public interface ManageSnapshots extends PendingUpdate<Snapshot> {
  // existing code...

  // introduces new code without breaking the API
  default ManageSnapshots createBranch(String name) {
    throw new UnsupportedOperationException(this.getClass().getName() + " doesn't implement createBranch(String)");
  }
}
```

## Iceberg Code Contribution Guidelines

### Style

Java code adheres to the [Google style](https://google.github.io/styleguide/javaguide.html), which will be verified via `./gradlew spotlessCheck` during builds.
In order to automatically fix Java code style issues, please use `./gradlew spotlessApply`.

**NOTE**: The **google-java-format** plugin will always use the latest version of the **google-java-format**. However, `spotless` itself is configured to use **google-java-format** 1.7
since that version is compatible with JDK 8. When formatting the code in the IDE, there is a slight chance that it will produce slightly different results. In such a case please run `./gradlew spotlessApply`
as CI will check the style against **google-java-format** 1.7.

### Copyright

Each file must include the Apache license information as a header.

```
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
```

### Configuring Copyright for IntelliJ IDEA

Every file needs to include the Apache license as a header. This can be automated in IntelliJ by
adding a Copyright profile:

1. In the **Settings/Preferences** dialog go to **Editor → Copyright → Copyright Profiles**.
2. Add a new profile and name it **Apache**.
3. Add the following text as the license text:

   ```
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
   ```
4. Go to **Editor → Copyright** and choose the **Apache** profile as the default profile for this
   project.
5. Click **Apply**.

### Java style guidelines

#### Method naming

1. Make method names as short as possible, while being clear. Omit needless words.
2. Avoid `get` in method names, unless an object must be a Java bean.
     * In most cases, replace `get` with a more specific verb that describes what is happening in the method, like `find` or `fetch`.
     * If there isn't a more specific verb or the method is a getter, omit `get` because it isn't helpful to readers and makes method names longer.
3. Where possible, use words and conjugations that form correct sentences in English when read
     * For example, `Transform.preservesOrder()` reads correctly in an if statement: `if (transform.preservesOrder()) { ... }`

#### Boolean arguments

Avoid boolean arguments to methods that are not `private` to avoid confusing invocations like `sendMessage(false)`. It is better to create two methods with names and behavior, even if both are implemented by one internal method.

```java
  // prefer exposing suppressFailure in method names
  public void sendMessageIgnoreFailure() {
    sendMessageInternal(true);
  }

  public void sendMessage() {
    sendMessageInternal(false);
  }

  private void sendMessageInternal(boolean suppressFailure) {
    ...
  }
```

When passing boolean arguments to existing or external methods, use inline comments to help the reader understand actions without an IDE.

```java
  // BAD: it is not clear what false controls
  dropTable(identifier, false);

  // GOOD: these uses of dropTable are clear to the reader
  dropTable(identifier, true /* purge data */);
  dropTable(identifier, purge);
```

#### Config naming

1. Use `-` to link words in one concept
    * For example, preferred convection `access-key-id` rather than `access.key.id`
2. Use `.` to create a hierarchy of config groups
    * For example, `s3` in `s3.access-key-id`, `s3.secret-access-key`

## Testing

### AssertJ

Prefer using [AssertJ](https://github.com/assertj/assertj) assertions as those provide a rich and intuitive set of strongly-typed assertions.
Checks can be expressed in a fluent way and [AssertJ](https://github.com/assertj/assertj) provides rich context when assertions fail.
Additionally, [AssertJ](https://github.com/assertj/assertj) has powerful testing capabilities on collections and exceptions.
Please refer to the [usage guide](https://assertj.github.io/doc/#assertj-core-assertions-guide) for additional examples.

```java
// bad: will only say true != false when check fails
assertTrue(x instanceof Xyz);

// better: will show type of x when check fails
assertThat(x).isInstanceOf(Xyz.class);

// bad: will only say true != false when check fails
assertTrue(catalog.listNamespaces().containsAll(expected));

// better: will show content of expected and of catalog.listNamespaces() if check fails
assertThat(catalog.listNamespaces()).containsAll(expected);
```
```java
// ok
assertNotNull(metadataFileLocations);
assertEquals(metadataFileLocations.size(), 4);

// better: will show the content of metadataFileLocations if check fails
assertThat(metadataFileLocations).isNotNull().hasSize(4);

// or
assertThat(metadataFileLocations).isNotNull().hasSameSizeAs(expected).hasSize(4);
```
```java
// if any key doesn't exist, it won't show the content of the map
assertThat(map.get("key1")).isEqualTo("value1");
assertThat(map.get("key2")).isNotNull();
assertThat(map.get("key3")).startsWith("3.5");

// better: all checks can be combined and the content of the map will be shown if any check fails
assertThat(map)
    .containsEntry("key1", "value1")
    .containsKey("key2")
    .hasEntrySatisfying("key3", v -> assertThat(v).startsWith("3.5"));
```

```java
// bad
try {
    catalog.createNamespace(deniedNamespace);
    Assert.fail("this should fail");
} catch (Exception e) {
    assertEquals(AccessDeniedException.class, e.getClass());
    assertEquals("User 'testUser' has no permission to create namespace", e.getMessage());
}

// better
assertThatThrownBy(() -> catalog.createNamespace(deniedNamespace))
    .isInstanceOf(AccessDeniedException.class)
    .hasMessage("User 'testUser' has no permission to create namespace");
```
Checks on exceptions should always make sure to assert that a particular exception message has occurred.


### Awaitility

Avoid using `Thread.sleep()` in tests as it leads to long test durations and flaky behavior if a condition takes slightly longer than expected.

```java
deleteTablesAsync();
Thread.sleep(3000L);
assertThat(tables()).isEmpty();
```

A better alternative is using [Awaitility](https://github.com/awaitility/awaitility) to make sure `tables()` are eventually empty. The below example will run the check
with a default polling interval of **100 millis**:

```java
deleteTablesAsync();
Awaitility.await("Tables were not deleted")
    .atMost(5, TimeUnit.SECONDS)
    .untilAsserted(() -> assertThat(tables()).isEmpty());
```

Please refer to the [usage guide](https://github.com/awaitility/awaitility/wiki/Usage) of [Awaitility](https://github.com/awaitility/awaitility) for more usage examples.


### JUnit4 / JUnit5

Iceberg currently uses a mix of JUnit4 (`org.junit` imports) and JUnit5 (`org.junit.jupiter.api` imports) tests. To allow an easier migration to JUnit5 in the future, new test classes
that are being added to the codebase should be written purely in JUnit5 where possible.


## Running Benchmarks
Some PRs/changesets might require running benchmarks to determine whether they are affecting the baseline performance. Currently there is 
no "push a single button to get a performance comparison" solution available, therefore one has to run JMH performance tests on their local machine and
post the results on the PR.

See [Benchmarks](benchmarks.md) for a summary of available benchmarks and how to run them.
