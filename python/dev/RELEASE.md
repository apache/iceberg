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

The guide to release the Python package.

First we're going to release a release candidate (RC) and publish it to the public to test. Once the vote has passed on the RC, we can release the new version.

## Running a release candidate

Make sure that you're on the version that you want to release.

```bash
export RC=rc0
export VERSION=0.0.1.${RC}
export VERSION_WITHOUT_RC=${VERSION/rc?/}
export VERSION_BRANCH=${VERSION_WITHOUT_RC//./-}

git checkout -b apache-iceberg-python-${VERSION_BRANCH}

git tag -s ${VERSION} -m "Apache Iceberg Python ${VERSION}"

export GIT_TAG=$(git show-ref ${VERSION})
export GIT_TAG_HASH=${GIT_TAG:0:40}
export LAST_COMMIT_ID=$(git rev-list ${VERSION} 2> /dev/null | head -n 1)
```

The `-s` option will sign the commit. If you don't have a key yet, you can find the instructions [here](http://www.apache.org/dev/openpgp.html#key-gen-generate-key). To install gpg on a M1 based Mac, a couple of additional steps are required: https://gist.github.com/phortuin/cf24b1cca3258720c71ad42977e1ba57

Next we'll create a source distribution (`sdist`) which will generate a `.tar.gz` with all the source files.

```bash
# Update the version
poetry version ${VERSION}

git diff pyiceberg/__init__.py
git add pyiceberg/__init__.py
git commit -s -m "Set to version ${VERSION}"
```

Now we can stage the version in pypi and upload the files to the Apache SVN.

Next we're going to build the artifacts:

```bash
rm -rf dist/
poetry build
```

This will create two artifacts:

```
Building apache-iceberg (0.1.0)
  - Building sdist
  - Built apache-iceberg-0.1.0.tar.gz
  - Building wheel
  - Built apache_iceberg-0.1.0-py3-none-any.whl
```

The `sdist` contains the source which can be used for checking licenses, and the wheel is a compiled version for quick installation.

Next, we can upload them to pypi. Please keep in mind that this **won't** bump the version for everyone that hasn't pinned their version, since it is RC [pre-release and those are ignored](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#pre-release-versioning).

```
twine upload dist/*
```

Before committing the files to the Apache SVN artifact distribution SVN, we need to generate hashes, and we need t o sign them using gpg:

```bash
for name in "apache_iceberg-${VERSION}-py3-none-any.whl" "apache-iceberg-${VERSION}.tar.gz"
do
    gpg --yes --armor --local-user fokko@apache.org --output "dist/${name}.asc" --detach-sig "dist/${name}"
    shasum -a 512 "dist/${name}" > "dist/${name}.sha512"
done
```

Next, we'll clone the Apache SVN, copy and commit the files:

```bash
export SVN_TMP_DIR=/tmp/iceberg-${VERSION_BRANCH}/
svn checkout https://dist.apache.org/repos/dist/dev/iceberg $SVN_TMP_DIR

export SVN_TMP_DIR_VERSIONED=${SVN_TMP_DIR}apache-iceberg-$VERSION/
mkdir -p $SVN_TMP_DIR_VERSIONED
cp dist/* $SVN_TMP_DIR_VERSIONED
svn add $SVN_TMP_DIR_VERSIONED
svn ci -m "Apache Iceberg ${VERSION}" ${SVN_TMP_DIR_VERSIONED}
```

Finally, we can generate the email what we'll send to the mail list:

```bash
cat << EOF > release-announcement-email.txt
To: dev@iceberg.apache.org
Subject: [VOTE] Release Apache Iceberg Python Client $VERSION
Hi Everyone,

I propose that we release the following RC as the official Apache Iceberg Python Client $VERSION release.

The commit ID is $LAST_COMMIT_ID

* This corresponds to the tag: $GIT_TAG_HASH
* https://github.com/apache/iceberg/commits/apache-iceberg-python-$VERSION_BRANCH
* https://github.com/apache/iceberg/tree/$GIT_TAG_HASH

The release tarball, signature, and checksums are here:

* https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-python-$VERSION/

You can find the KEYS file here:

* https://dist.apache.org/repos/dist/dev/iceberg/KEYS

Convenience binary artifacts are staged on pypi:

https://pypi.org/project/apache-iceberg/$VERSION/

And can be installed using: pip install apache-iceberg==$VERSION

Please download, verify, and test.
Please vote in the next 72 hours.
[ ] +1 Release this as Apache Iceberg Python Client $VERSION
[ ] +0
[ ] -1 Do not release this because...
EOF

cat release-announcement-email.txt
```
