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

# How to release

The guide to release PyIceberg.

The first step is to publish a release candidate (RC) and publish it to the public for testing and validation. Once the vote has passed on the RC, the RC turns into the new release.

## Running a release candidate

Make sure that the version is correct in `pyproject.toml` and `pyiceberg/__init__.py`. Correct means that it reflects the version that you want to release.

### Setting the tag

First set the tag on the commit:

```bash
export RC=rc1
export VERSION=0.1.0${RC}
export VERSION_WITHOUT_RC=${VERSION/rc?/}
export VERSION_BRANCH=${VERSION_WITHOUT_RC//./-}
export GIT_TAG=pyiceberg-${VERSION}

git tag -s ${GIT_TAG} -m "PyIceberg ${VERSION}"
git push apache ${GIT_TAG}

export GIT_TAG_REF=$(git show-ref ${GIT_TAG})
export GIT_TAG_HASH=${GIT_TAG_REF:0:40}
export LAST_COMMIT_ID=$(git rev-list ${GIT_TAG} 2> /dev/null | head -n 1)
```

The `-s` option will sign the commit. If you don't have a key yet, you can find the instructions [here](http://www.apache.org/dev/openpgp.html#key-gen-generate-key). To install gpg on a M1 based Mac, a couple of additional steps are required: https://gist.github.com/phortuin/cf24b1cca3258720c71ad42977e1ba57

### Upload to Apache SVN

Both the source distribution (`sdist`) and the binary distributions (`wheels`) need to be published for the RC. The wheels are convenient to avoid having people to install compilers locally. The downside is that each architecture requires its own wheel. [use `cibuildwheel`](https://github.com/pypa/cibuildwheel) runs in Github actions to create a wheel for each of the architectures.

Before committing the files to the Apache SVN artifact distribution SVN hashes need to be generated, and those need to be signed with gpg to make sure that they are authentic.

Go to [Github Actions and run the `Python release` action](https://github.com/apache/iceberg/actions/workflows/python-release.yml). **Set the version to master, since we cannot modify the source**. Download the zip, and sign the files:

```bash
for name in $(ls release-master/pyiceberg-*.whl)
do
    gpg --yes --armor --local-user fokko@apache.org --output "${name}.asc" --detach-sig "${name}"
    shasum -a 512 "${name}.asc" > "${name}.asc.sha512"
done
```

Now we can upload the files

```bash
export SVN_TMP_DIR=/tmp/iceberg-${VERSION_BRANCH}/
svn checkout https://dist.apache.org/repos/dist/dev/iceberg $SVN_TMP_DIR

export SVN_TMP_DIR_VERSIONED=${SVN_TMP_DIR}pyiceberg-$VERSION/
mkdir -p $SVN_TMP_DIR_VERSIONED
cp artifact/* $SVN_TMP_DIR_VERSIONED
svn add $SVN_TMP_DIR_VERSIONED
svn ci -m "PyIceberg ${VERSION}" ${SVN_TMP_DIR_VERSIONED}
```

### Upload to PyPi

Go to Github Actions and run the `Python release` action. Set the version of the release candidate as the input: `0.1.0rc1`. Download the zip and unzip it locally.

Next step is to upload them to pypi. Please keep in mind that this **won't** bump the version for everyone that hasn't pinned their version, since it is set to a RC [pre-release and those are ignored](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#pre-release-versioning).

```bash
twine upload -s release-0.1.0-rc1/*
```

Final step is to generate the email to the dev mail list:

```bash
cat << EOF > release-announcement-email.txt
To: dev@iceberg.apache.org
Subject: [VOTE] Release Apache PyIceberg $VERSION_WITHOUT_RC
Hi Everyone,

I propose that we release the following RC as the official PyIceberg $VERSION_WITHOUT_RC release.

The commit ID is $LAST_COMMIT_ID

* This corresponds to the tag: $GIT_TAG ($GIT_TAG_HASH)
* https://github.com/apache/iceberg/releases/tag/$GIT_TAG
* https://github.com/apache/iceberg/tree/$LAST_COMMIT_ID

The release tarball, signature, and checksums are here:

* https://dist.apache.org/repos/dist/dev/iceberg/pyiceberg-$VERSION/

You can find the KEYS file here:

* https://dist.apache.org/repos/dist/dev/iceberg/KEYS

Convenience binary artifacts are staged on pypi:

https://pypi.org/project/pyiceberg/$VERSION/

And can be installed using: pip3 install pyiceberg==$VERSION

Please download, verify, and test.

Please vote in the next 72 hours.
[ ] +1 Release this as PyIceberg $VERSION_WITHOUT_RC
[ ] +0
[ ] -1 Do not release this because...
EOF

cat release-announcement-email.txt
```

## Vote has passed

Once the vote has been passed, the latest version can be pushed to PyPi. Check out the Apache SVN and make sure to publish the right version with `twine`:

```bash
svn checkout https://dist.apache.org/repos/dist/dev/iceberg /tmp/
twine upload -s /tmp/iceberg/pyiceberg-0.1.0rc1
```

Send out an announcement on the dev mail list:

```
To: dev@iceberg.apache.org
Subject: [ANNOUNCE] Apache PyIceberg release <VERSION>

I'm pleased to announce the release of Apache PyIceberg <VERSION>!

Apache Iceberg is an open table format for huge analytic datasets. Iceberg
delivers high query performance for tables with tens of petabytes of data,
along with atomic commits, concurrent writes, and SQL-compatible table
evolution.

This Python release can be downloaded from: https://pypi.org/project/pyiceberg/<VERSION>/

Thanks to everyone for contributing!
```

## Release the docs

A committer triggers the [`Python Docs` Github Actions](https://github.com/apache/iceberg/actions/workflows/python-ci-docs.yml) through the UI by selecting the branch that just has been released. This will publish the new docs.
