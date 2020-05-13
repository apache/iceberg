#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

if [ -z "$1" ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

if [ -z "$2" ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1-incubating
rc=$2

if [ -d tmp/ ]; then
  echo "Cannot run: tmp/ exists"
  exit
fi

tag=apache-iceberg-$version
tagrc=${tag}-rc${rc}

echo "Preparing source for $tagrc"

# create version.txt for this release
echo $version > version.txt
git add version.txt
git commit -m "Add version.txt for release $version" version.txt

set_version_hash=`git rev-list HEAD 2> /dev/null | head -n 1 `

git tag -am "Apache Iceberg $version" $tagrc $set_version_hash
git push apache $tagrc

release_hash=`git rev-list $tagrc 2> /dev/null | head -n 1 `

if [ -z "$release_hash" ]; then
  echo "Cannot continue: unknown git tag: $tag"
  exit
fi

echo "Using commit $release_hash"

tarball=$tag.tar.gz

# be conservative and use the release hash, even though git produces the same
# archive (identical hashes) using the scm tag
git archive $release_hash --prefix $tag/ -o $tarball .baseline api arrow common core data dev gradle gradlew hive mr orc parquet pig project spark spark-runtime LICENSE NOTICE DISCLAIMER README.md build.gradle baseline.gradle deploy.gradle tasks.gradle jmh.gradle gradle.properties settings.gradle versions.lock versions.props version.txt

# sign the archive
gpg --armor --output ${tarball}.asc --detach-sig $tarball
sha512sum $tarball > ${tarball}.sha512

# check out the Iceberg RC folder
svn co --depth=empty https://dist.apache.org/repos/dist/dev/incubator/iceberg tmp

# add the release candidate for the tag
mkdir -p tmp/$tagrc
cp ${tarball}* tmp/$tagrc
svn add tmp/$tagrc
svn ci -m "Apache Iceberg $version RC${rc}" tmp/$tagrc

# clean up
rm -rf tmp

echo "Success! The release candidate is available here:"
echo "  https://dist.apache.org/repos/dist/dev/incubator/iceberg/$tagrc"
echo ""
echo "Commit SHA1: $release_hash"

