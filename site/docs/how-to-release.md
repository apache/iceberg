---
title: "How To Release"
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

## Introduction

This page walks you through the release process of the Iceberg project. [Here](https://www.apache.org/legal/release-policy.html) you can read about the release process in general for an Apache project.

Decisions about releases are made by three groups:

* Release Manager: Does the work of creating the release, signing it, counting [votes](#voting), announcing the release and so on. Requires the assistance of a committer for some steps.
* The community: Performs the discussion of whether it is the right time to create a release and what that release should contain. The community can also cast non-binding votes on the release.
* PMC: Gives binding votes on the release.

This page describes the procedures that the release manager and voting PMC members take during the release process.

## Setup

To create a release candidate, you will need:

* Apache LDAP credentials for Nexus and SVN
* A [GPG key for signing](https://www.apache.org/dev/release-signing#generate), published in [KEYS](https://dist.apache.org/repos/dist/dev/iceberg/KEYS)

If you have not published your GPG key yet, you must publish it before sending the vote email by doing:

```shell
svn co https://dist.apache.org/repos/dist/dev/iceberg icebergsvn
cd icebergsvn
echo "" >> KEYS # append a newline
gpg --list-sigs <YOUR KEY ID HERE> >> KEYS # append signatures
gpg --armor --export <YOUR KEY ID HERE> >> KEYS # append public key block
svn commit -m "add key for <YOUR NAME HERE>"
```

### Nexus access

Nexus credentials are configured in your personal `~/.gradle/gradle.properties` file using `mavenUser` and `mavenPassword`:

```
mavenUser=yourApacheID
mavenPassword=SomePassword
```

### PGP signing

The release scripts use the command-line `gpg` utility so that signing can use the gpg-agent and does not require writing your private key's passphrase to a configuration file.

To configure gradle to sign convenience binary artifacts, add the following settings to `~/.gradle/gradle.properties`:

```
signing.gnupg.keyName=Your Name (CODE SIGNING KEY)
```

To use `gpg` instead of `gpg2`, also set `signing.gnupg.executable=gpg`

For more information, see the Gradle [signing documentation](https://docs.gradle.org/current/userguide/signing_plugin.html#sec:signatory_credentials).

### Apache repository

The release should be executed against `https://github.com/apache/iceberg.git` instead of any fork.
Set it as remote with name `apache` for release if it is not already set up.

## Creating a release candidate

### Initiate a discussion about the release with the community

This step can be useful to gather ongoing patches that the community thinks should be in the upcoming release.

The communication can be started via a [DISCUSS] mail on the dev@ channel and the desired tickets can be added to the github milestone of the next release.

Note, creating a milestone in github requires a committer. However, a non-committer can assign tasks to a milestone if added to the list of collaborators in [.asf.yaml](https://github.com/apache/iceberg/blob/master/.asf.yaml)

The release status is discussed during each community sync meeting. Release manager should join the meeting to report status and discuss any release blocker.

### Build the source release

To create the source release artifacts, run the `source-release.sh` script with the release version and release candidate number:

```bash
dev/source-release.sh -v 0.13.0 -r 0 -k <YOUR KEY ID HERE>
```

Example console output:

```text
Preparing source for apache-iceberg-0.13.0-rc1
Adding version.txt and tagging release...
[master ca8bb7d0] Add version.txt for release 0.13.0
 1 file changed, 1 insertion(+)
 create mode 100644 version.txt
Pushing apache-iceberg-0.13.0-rc1 to origin...
Enumerating objects: 5, done.
Counting objects: 100% (5/5), done.
Delta compression using up to 12 threads
Compressing objects: 100% (3/3), done.
Writing objects: 100% (4/4), 433 bytes | 433.00 KiB/s, done.
Total 4 (delta 1), reused 0 (delta 0)
remote: Resolving deltas: 100% (1/1), completed with 1 local object.
To https://github.com/apache/iceberg.git
 * [new tag]           apache-iceberg-0.13.0-rc1 -> apache-iceberg-0.13.0-rc1
Creating tarball  using commit ca8bb7d0821f35bbcfa79a39841be8fb630ac3e5
Signing the tarball...
Checking out Iceberg RC subversion repo...
Checked out revision 52260.
Adding tarball to the Iceberg distribution Subversion repo...
A         tmp/apache-iceberg-0.13.0-rc1
A         tmp/apache-iceberg-0.13.0-rc1/apache-iceberg-0.13.0.tar.gz.asc
A  (bin)  tmp/apache-iceberg-0.13.0-rc1/apache-iceberg-0.13.0.tar.gz
A         tmp/apache-iceberg-0.13.0-rc1/apache-iceberg-0.13.0.tar.gz.sha512
Adding         tmp/apache-iceberg-0.13.0-rc1
Adding  (bin)  tmp/apache-iceberg-0.13.0-rc1/apache-iceberg-0.13.0.tar.gz
Adding         tmp/apache-iceberg-0.13.0-rc1/apache-iceberg-0.13.0.tar.gz.asc
Adding         tmp/apache-iceberg-0.13.0-rc1/apache-iceberg-0.13.0.tar.gz.sha512
Transmitting file data ...done
Committing transaction...
Committed revision 52261.
Creating release-announcement-email.txt...
Success! The release candidate is available here:
  https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-0.13.0-rc1

Commit SHA1: ca8bb7d0821f35bbcfa79a39841be8fb630ac3e5

We have generated a release announcement email for you here:
/Users/jackye/iceberg/release-announcement-email.txt

Please note that you must update the Nexus repository URL
contained in the mail before sending it out.
```

The source release script will create a candidate tag based on the HEAD revision in git and will prepare the release tarball, signature, and checksum files. It will also upload the source artifacts to SVN.

Note the commit SHA1 and candidate location because those will be added to the vote thread.

Once the source release is ready, use it to stage convenience binary artifacts in Nexus.

### Build and stage convenience binaries

Convenience binaries are created using the source release tarball from in the last step.

Untar the source release and go into the release directory:

```bash
tar xzf apache-iceberg-0.13.0.tar.gz
cd apache-iceberg-0.13.0
```

To build and publish the convenience binaries, run the `dev/stage-binaries.sh` script. This will push to a release staging repository.

Disable gradle parallelism by setting `org.gradle.parallel=false` in `gradle.properties`.

```
dev/stage-binaries.sh
```

Next, you need to close the staging repository:

1. Go to [Nexus](https://repository.apache.org/) and log in
2. In the menu on the left, choose "Staging Repositories"
3. Select the Iceberg repository
   * If multiple staging repositories are created after running the script, verify that gradle parallelism is disabled and try again.
4. At the top, select "Close" and follow the instructions
   * In the comment field use "Apache Iceberg &lt;version&gt; RC&lt;num&gt;"

### Start a VOTE thread

The last step for a candidate is to create a VOTE thread on the dev mailing list.
The email template is already generated in `release-announcement-email.txt` with some details filled.

Example title subject:

```text
[VOTE] Release Apache Iceberg <VERSION> RC<NUM>
```

Example content:

```text
Hi everyone,

I propose the following RC to be released as official Apache Iceberg <VERSION> release.

The commit id is <SHA1>
* This corresponds to the tag: apache-iceberg-<VERSION>-rc<NUM>
* https://github.com/apache/iceberg/commits/apache-iceberg-<VERSION>-rc<NUM>
* https://github.com/apache/iceberg/tree/<SHA1>

The release tarball, signature, and checksums are here:
* https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-<VERSION>-rc<NUM>/

You can find the KEYS file here:
* https://dist.apache.org/repos/dist/dev/iceberg/KEYS

Convenience binary artifacts are staged in Nexus. The Maven repository URL is:
* https://repository.apache.org/content/repositories/orgapacheiceberg-<ID>/

This release includes important changes that I should have summarized here, but I'm lazy.

Please download, verify, and test.

Please vote in the next 72 hours. (Weekends excluded)

[ ] +1 Release this as Apache Iceberg <VERSION>
[ ] +0
[ ] -1 Do not release this because...

Only PMC members have binding votes, but other community members are encouraged to cast
non-binding votes. This vote will pass if there are 3 binding +1 votes and more binding
+1 votes than -1 votes.
```

When a candidate is passed or rejected, reply with the voting result:

```text
Subject: [RESULT][VOTE] Release Apache Iceberg <VERSION> RC<NUM>
```

```text
Thanks everyone who participated in the vote for Release Apache Iceberg <VERSION> RC<NUM>.

The vote result is:

+1: 3 (binding), 5 (non-binding)
+0: 0 (binding), 0 (non-binding)
-1: 0 (binding), 0 (non-binding)

Therefore, the release candidate is passed/rejected.
```


### Finishing the release

After the release vote has passed, you need to release the last candidate's artifacts.

But note that releasing the artifacts should happen around the same time the new docs are released
so make sure the [documentation changes](#documentation-release)
are prepared when going through the below steps.

#### Publishing the release

First, copy the source release directory to releases:

```bash
svn mv https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-<VERSION>-rcN https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-<VERSION> -m "Iceberg: Add release <VERSION>"
```

!!! Note
    The above step requires PMC privileges to execute.

Next, add a release tag to the git repository based on the passing candidate tag:

```bash
git tag -am 'Release Apache Iceberg <VERSION>' apache-iceberg-<VERSION> apache-iceberg-<VERSION>-rcN
```

Then release the candidate repository in [Nexus](https://repository.apache.org/#stagingRepositories).

#### Announcing the release

To announce the release, wait until Maven central has mirrored the Apache binaries, then update the Iceberg site and send an announcement email:

```text
[ANNOUNCE] Apache Iceberg release <VERSION>
```
```text
I'm pleased to announce the release of Apache Iceberg <VERSION>!

Apache Iceberg is an open table format for huge analytic datasets. Iceberg
delivers high query performance for tables with tens of petabytes of data,
along with atomic commits, concurrent writes, and SQL-compatible table
evolution.

This release can be downloaded from: https://www.apache.org/dyn/closer.cgi/iceberg/<TARBALL NAME WITHOUT .tar.gz>/<TARBALL NAME>

Java artifacts are available from Maven Central.

Thanks to everyone for contributing!
```

#### Update revapi

Create a PR in the `iceberg` repo to make revapi run on the new release. For an example see [this PR](https://github.com/apache/iceberg/pull/6275).

#### Update GitHub

- Create a PR in the `iceberg` repo to add the new version to the github issue template. For an example see [this PR](https://github.com/apache/iceberg/pull/6287).
- Draft [a new release to update Github](https://github.com/apache/iceberg/releases/new) to show the latest release. A changelog can be generated automatically using Github.

### Documentation Release

Documentation needs to be updated as a part of an Iceberg release after a release candidate is passed.
The commands described below assume you are in a directory containing a local clone of the `iceberg-docs`
repository and `iceberg` repository. Adjust the commands accordingly if it is not the case. Note that all
changes in `iceberg` need to happen against the `master` branch and changes in `iceberg-docs` need to happen
against the `main` branch. 

#### Common documentation update

1. To start the release process, run the following steps in the `iceberg-docs` repository to copy docs over:
```shell
cp -r ../iceberg/format/* ../iceberg-docs/landing-page/content/common/
```
2. Change into the `iceberg-docs` repository and create a branch.
```shell
cd ../iceberg-docs
git checkout -b <BRANCH NAME>
```
3. Commit, push, and open a PR against the `iceberg-docs` repo (`<BRANCH NAME>` -> `main`)

#### Versioned documentation update

Once the common docs changes have been merged into `main`, the next step is to update the versioned docs.

1. In the `iceberg-docs` repository, cut a new branch using the version number as the branch name
```shell
cd ../iceberg-docs
git checkout -b <VERSION>
git push --set-upstream apache <VERSION>
```
2. Copy the versioned docs from the `iceberg` repo into the `iceberg-docs` repo
```shell
rm -rf ../iceberg-docs/docs/content
cp -r ../iceberg/docs ../iceberg-docs/docs/content
```
3. Commit the changes and open a PR against the `<VERSION>` branch in the `iceberg-docs` repo

#### Javadoc update

In the `iceberg` repository, generate the javadoc for your release and copy it to the `javadoc` folder in `iceberg-docs` repo:
```shell
cd ../iceberg
./gradlew refreshJavadoc
rm -rf ../iceberg-docs/javadoc
cp -r site/docs/javadoc/<VERSION NUMBER> ../iceberg-docs/javadoc
```

This resulted changes in `iceberg-docs` should be approved in a separate PR.

#### Update the latest branch

Since `main` is currently the same as the version branch, one needs to rebase `latest` branch against `main`:

```shell
git checkout latest
git rebase main
git push apache latest
```

#### Set latest version in iceberg-docs repo

The last step is to update the `main` branch in `iceberg-docs` to set the latest version.
A PR needs to be published in the `iceberg-docs` repository with the following changes:
1. Update variable `latestVersions.iceberg` to the new release version in `landing-page/config.toml`
2. Update variable `latestVersions.iceberg` to the new release version and 
`versions.nessie` to the version of `org.projectnessie.nessie:*` from [versions.props](https://github.com/apache/iceberg/blob/master/versions.props) in `docs/config.toml`
3. Update list `versions` with the new release in `landing-page/config.toml`
4. Update list `versions` with the new release in `docs/config.toml`
5. Mark the current latest release notes to past releases under `landing-page/content/common/release-notes.md`
6. Add release notes for the new release version in `landing-page/content/common/release-notes.md`

# How to Verify a Release

Each Apache Iceberg release is validated by the community by holding a vote. A community release manager
will prepare a release candidate and call a vote on the Iceberg
[dev list](community.md#mailing-lists).
To validate the release candidate, community members will test it out in their downstream projects and environments.
It's recommended to report the Java, Scala, Spark, Flink and Hive versions you have tested against when you vote.

In addition to testing in downstream projects, community members also check the release's signatures, checksums, and
license documentation.

## Validating a source release candidate

Release announcements include links to the following:

- **A source tarball**
- **A signature (.asc)**
- **A checksum (.sha512)**
- **KEYS file**
- **GitHub change comparison**

After downloading the source tarball, signature, checksum, and KEYS file, here are instructions on how to
verify signatures, checksums, and documentation.

### Verifying Signatures

First, import the keys.
```bash
curl https://dist.apache.org/repos/dist/dev/iceberg/KEYS -o KEYS
gpg --import KEYS
```

Next, verify the `.asc` file.
```bash
gpg --verify apache-iceberg-{{ icebergVersion }}.tar.gz.asc
```

### Verifying Checksums

```bash
shasum -a 512 --check apache-iceberg-{{ icebergVersion }}.tar.gz.sha512
```

### Verifying License Documentation

Untar the archive and change into the source directory.
```bash
tar xzf apache-iceberg-{{ icebergVersion }}.tar.gz
cd apache-iceberg-{{ icebergVersion }}
```

Run RAT checks to validate license headers.
```bash
dev/check-license
```

### Verifying Build and Test

To verify that the release candidate builds properly, run the following command.
```bash
./gradlew build
```

## Testing release binaries

Release announcements will also include a maven repository location. You can use this
location to test downstream dependencies by adding it to your maven or gradle build.

To use the release in your maven build, add the following to your `POM` or `settings.xml`:
```xml
...
  <repositories>
    <repository>
      <id>iceberg-release-candidate</id>
      <name>Iceberg Release Candidate</name>
      <url>${MAVEN_URL}</url>
    </repository>
  </repositories>
...
```

To use the release in your gradle build, add the following to your `build.gradle`:
```groovy
repositories {
    mavenCentral()
    maven {
        url "${MAVEN_URL}"
    }
}
```

!!! Note
    Replace `${MAVEN_URL}` with the URL provided in the release announcement

### Verifying with Spark

To verify using spark, start a `spark-shell` with a command like the following command (use the appropriate
spark-runtime jar for the Spark installation):
```bash
spark-shell \
    --conf spark.jars.repositories=${MAVEN_URL} \
    --packages org.apache.iceberg:iceberg-spark3-runtime:{{ icebergVersion }} \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=${LOCAL_WAREHOUSE_PATH} \
    --conf spark.sql.catalog.local.default-namespace=default \
    --conf spark.sql.defaultCatalog=local
```

### Verifying with Flink

To verify using Flink, start a Flink SQL Client with the following command:
```bash
wget ${MAVEN_URL}/iceberg-flink-runtime/{{ icebergVersion }}/iceberg-flink-runtime-{{ icebergVersion }}.jar

sql-client.sh embedded \
    -j iceberg-flink-runtime-{{ icebergVersion }}.jar \
    -j ${FLINK_CONNECTOR_PACKAGE}-${HIVE_VERSION}_${SCALA_VERSION}-${FLINK_VERSION}.jar \
    shell
```

## Voting

Votes are cast by replying to the release candidate announcement email on the dev mailing list
with either `+1`, `0`, or `-1`.

> [ ] +1 Release this as Apache Iceberg {{ icebergVersion }}
[ ] +0
[ ] -1 Do not release this because...

In addition to your vote, it's customary to specify if your vote is binding or non-binding. Only members
of the Project Management Committee have formally binding votes. If you're unsure, you can specify that your
vote is non-binding. To read more about voting in the Apache framework, checkout the
[Voting](https://www.apache.org/foundation/voting.html) information page on the Apache foundation's website.
