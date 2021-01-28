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

## Setup

To create a release candidate, you will need:

* Apache LDAP credentals for Nexus and SVN
* A [GPG key for signing](https://www.apache.org/dev/release-signing#generate), published in [KEYS](https://dist.apache.org/repos/dist/dev/iceberg/KEYS)

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

## Creating a release candidate

### Build the source release

To create the source release artifacts, run the `source-release.sh` script with the release version and release candidate number:

```bash
dev/source-release.sh 0.8.1 0
```
```
Preparing source for apache-iceberg-0.8.1-rc0
...
Success! The release candidate is available here:
  https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-0.8.1-rc0/

Commit SHA1: 4b4716c76559b3cdf3487e6b60ab52950241989b
```

The source release script will create a candidate tag based on the HEAD revision in git and will prepare the release tarball, signature, and checksum files. It will also upload the source artifacts to SVN.

Note the commit SHA1 and candidate location because those will be added to the vote thread.

Once the source release is ready, use it to stage convenience binary artifacts in Nexus.

### Build and stage convenience binaries

Convenience binaries are created using the source release tarball from in the last step.

Untar the source release and go into the release directory:

```bash
tar xzf apache-iceberg-0.8.1.tar.gz
cd apache-iceberg-0.8.1
```

To build and publish the convenience binaries, run the `dev/stage-binaries.sh` script. This will push to a release staging repository.

```
dev/stage-binaries.sh
```

Next, you need to close the staging repository:

1. Go to [Nexus](https://repository.apache.org/) and log in
2. In the menu on the left, choose "Staging Repositories"
3. Select the Iceberg repository
4. At the top, select "Close" and follow the instructions
    * In the comment field use "Apache Iceberg &lt;version&gt; RC&lt;num&gt;"

### Start a VOTE thread

The last step for a candidate is to create a VOTE thread on the dev mailing list.

```text
Subject: [VOTE] Release Apache Iceberg <VERSION> RC<NUM>
```
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

Please vote in the next 72 hours.

[ ] +1 Release this as Apache Iceberg <VERSION>
[ ] +0
[ ] -1 Do not release this because...
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

First, copy the source release directory to releases:

```bash
mkdir iceberg
cd iceberg
svn co https://dist.apache.org/repos/dist/dev/iceberg candidates
svn co https://dist.apache.org/repos/dist/release/iceberg releases
cp -r candidates/apache-iceberg-<VERSION>-rcN/ releases/apache-iceberg-<VERSION>
cd releases
svn add apache-iceberg-<VERSION>
svn ci -m 'Iceberg: Add release <VERSION>'
```

Next, add a release tag to the git repository based on the passing candidate tag:

```bash
git tag -am 'Release Apache Iceberg <VERSION>' apache-iceberg-<VERSION> apache-iceberg-<VERSION>-rcN
```

Then release the candidate repository in [Nexus](https://repository.apache.org/#stagingRepositories).

To announce the release, wait until Maven central has mirrored the Apache binaries, then update the Iceberg site and send an announcement email:

```text
[ANNOUNCE] Apache Iceberg release <VERSION>
```
```text
I'm please to announce the release of Apache Iceberg <VERSION>!

Apache Iceberg is an open table format for huge analytic datasets. Iceberg
delivers high query performance for tables with tens of petabytes of data,
along with atomic commits, concurrent writes, and SQL-compatible table
evolution.

This release can be downloaded from: https://www.apache.org/dyn/closer.cgi/iceberg/<TARBALL NAME WITHOUT .tar.gz>/<TARBALL NAME>

Java artifacts are available from Maven Central.

Thanks to everyone for contributing!
```
