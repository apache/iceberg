
## Setup

To create a release candidate, you will need:

* Apache LDAP credentals for Nexus and SVN
* A [GPG key for signing](https://www.apache.org/dev/release-signing#generate), published in [KEYS](https://dist.apache.org/repos/dist/dev/incubator/iceberg/KEYS)

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
dev/source-release.sh 0.7.0 0
```
```
Preparing source for apache-iceberg-0.7.0-incubating-rc0
...
Success! The release candidate is available here:
  https://dist.apache.org/repos/dist/dev/incubator/iceberg/apache-iceberg-0.7.0-incubating-rc0/

Commit SHA1: 4b4716c76559b3cdf3487e6b60ab52950241989b
```

The source release script will create a candidate tag based on the HEAD revision in git and will prepare the release tarball, signature, and checksum files. It will also upload the source artifacts to SVN.

Note the commit SHA1 and candidate location because those will be added to the vote thread.

Once the source release is ready, use it to stage convenience binary artifacts in Nexus.

### Build and stage convenience binaries

Convenience binaries are created using the source release tarball from in the last step.

Untar the source release and go into the release directory:

```bash
tar xzf apache-iceberg-0.7.0-incubating.tar.gz
cd apache-iceberg-0.7.0-incubating
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
    * In the comment field use "Apache Iceberg (incubating) &lt;version&gt; RC&lt;num&gt;"

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
* https://github.com/apache/incubator-iceberg/commits/apache-iceberg-<VERSION>-rc<NUM>
* https://github.com/apache/incubator-iceberg/tree/<SHA1>

The release tarball, signature, and checksums are here:
* https://dist.apache.org/repos/dist/dev/incubator/iceberg/apache-iceberg-<VERSION>-rc<NUM>/

You can find the KEYS file here:
* https://dist.apache.org/repos/dist/dev/incubator/iceberg/KEYS

Convenience binary artifacts are staged in Nexus. The Maven repository URL is:
* https://repository.apache.org/content/repositories/orgapacheiceberg-<ID>/

This release includes important changes that I should have summarized here, but I'm lazy.

Please download, verify, and test.

Please vote in the next 72 hours.

[ ] +1 Release this as Apache Parquet <VERSION>
[ ] +0
[ ] -1 Do not release this because...
```
