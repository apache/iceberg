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

First we're going to release a release candidate (RC) and publish it to the public for testing and validation. Once the vote has passed on the RC, we can release the new version.

## Running a release candidate

Make sure that you're on the version that you want to release. And that the version is correct in `pyproject.toml` and `pyiceberg/__init__.py`. Correct means that it reflects the version that you want to release, and doesn't contain any additional modifiers, such as `dev0`.

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

Next we'll create a source distribution (`sdist`) which will generate a `.tar.gz` with all the source files. So we can upload the files to the Apache SVN.

```
poetry build
```

This will create two artifacts:

```
Building pyiceberg (0.1.0)
  - Building sdist
  - Built pyiceberg-0.1.0.tar.gz
  - Building wheel
  - Built apache_iceberg-0.1.0-py3-none-any.whl
```

The `sdist` contains the source which can be used for checking licenses, and the wheel is a compiled version for quick installation.

Before committing the files to the Apache SVN artifact distribution SVN, we need to generate hashes, and we need to sign them using gpg:

```bash
for name in "pyiceberg-${VERSION_WITHOUT_RC}-py3-none-any.whl" "pyiceberg-${VERSION_WITHOUT_RC}.tar.gz"
do
    gpg --yes --armor --local-user fokko@apache.org --output "dist/${name}.asc" --detach-sig "dist/${name}"
    (cd dist/ && shasum -a 512 "${name}" > "${name}.sha512")
done
```

Next, we'll clone the Apache SVN, copy and commit the files:

```bash
export SVN_TMP_DIR=/tmp/iceberg-${VERSION_BRANCH}/
svn checkout https://dist.apache.org/repos/dist/dev/iceberg $SVN_TMP_DIR

export SVN_TMP_DIR_VERSIONED=${SVN_TMP_DIR}pyiceberg-$VERSION/
mkdir -p $SVN_TMP_DIR_VERSIONED
cp dist/* $SVN_TMP_DIR_VERSIONED
svn add $SVN_TMP_DIR_VERSIONED
svn ci -m "PyIceberg ${VERSION}" ${SVN_TMP_DIR_VERSIONED}
```

Next, we can upload them to pypi. Please keep in mind that this **won't** bump the version for everyone that hasn't pinned their version, we set it to a RC [pre-release and those are ignored](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#pre-release-versioning).

```
poetry version ${VERSION}
rm -rf dist/
poetry build
twine upload -s dist/*
```

Finally, we can generate the email what we'll send to the mail list:

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
[ ] +1 Release this as PyIceberg $VERSION
[ ] +0
[ ] -1 Do not release this because...
EOF

cat release-announcement-email.txt
```
