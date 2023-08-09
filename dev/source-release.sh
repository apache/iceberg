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

#
# Generate a release-candidate and a text file containing the release
# announcement email.
#

set -e

usage () {
    echo "usage: $0 [-k <key_id>] [-g <git_remote>] -v <version_num> -r <rc_num>"
    echo "  -v      Version number of release"
    echo "  -r      Release candidate number"
    echo "  -k      Specify signing key. Defaults to \"GPG default key\""
    echo "  -g      Specify Git remote name. Defaults to \"apache\""
    echo "  -d      Turn on DEBUG output"
    exit 1
}

# Default repository remote name
remote="apache"

while getopts "v:r:k:g:d" opt; do
  case "${opt}" in
    v)
      version="${OPTARG}"
      ;;
    r)
      rc="${OPTARG}"
      ;;
    k)
      keyid="${OPTARG}"
      ;;
    g)
      remote="${OPTARG}"
      ;;
    d)
      set -x
      ;;
    *)
      usage
      ;;
  esac
done

shift $((OPTIND-1))

if [ -z "$version" ] || [ -z "$rc" ]; then
  echo "You must specify the version and RC numbers using the -v and -r switches"
  usage
fi

if ! git ls-remote --exit-code "$remote" >/dev/null 2>&1 ; then
  echo "The target remote git repository, ${remote}, is not configured in git. Please pass a valid value for the remote git repository to target via the -g switch"
  usage
fi

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
projectdir="$(dirname "$scriptdir")"
tmpdir=$projectdir/tmp

if [ -d $tmpdir ]; then
  echo "Cannot run: $tmpdir already exists"
  exit 1
fi

tag="apache-iceberg-$version"
tagrc="${tag}-rc${rc}"

echo "Preparing source for $tagrc"

echo "Creating release candidate tag: $tagrc..."
set_version_hash=`git rev-list HEAD 2> /dev/null | head -n 1 `
git tag -am "Apache Iceberg $version" $tagrc $set_version_hash

echo "Pushing $tagrc to $remote..."
git push $remote $tagrc

release_hash=`git rev-list $tagrc 2> /dev/null | head -n 1 `

if [ -z "$release_hash" ]; then
  echo "Cannot continue: unknown Git tag: $tag"
  exit
fi

echo "Generating version.txt and iceberg-build.properties..."
echo $version > $projectdir/version.txt
./gradlew generateGitProperties
cp $projectdir/build/iceberg-build.properties $projectdir/iceberg-build.properties

# be conservative and use the release hash, even though git produces the same
# archive (identical hashes) using the scm tag
echo "Creating tarball ${tarball} using commit $release_hash"
tarball=$tag.tar.gz
git archive $release_hash --worktree-attributes --prefix $tag/ --add-file $projectdir/version.txt --add-file $projectdir/iceberg-build.properties -o $projectdir/$tarball

# remove the uncommitted build files so they don't affect the current working copy
rm $projectdir/version.txt
rm $projectdir/iceberg-build.properties

echo "Signing the tarball..."
[[ -n "$keyid" ]] && keyopt="-u $keyid"
gpg $keyopt --armor --output ${projectdir}/${tarball}.asc --detach-sig ${projectdir}/$tarball
shasum -a 512 $tarball > ${projectdir}/${tarball}.sha512


echo "Checking out Iceberg RC subversion repo..."
svn co --depth=empty https://dist.apache.org/repos/dist/dev/iceberg $tmpdir

echo "Adding tarball to the Iceberg distribution Subversion repo..."
mkdir -p $tmpdir/$tagrc
cp $projectdir/${tarball}* $tmpdir/$tagrc
svn add $tmpdir/$tagrc
svn ci -m "Apache Iceberg $version RC${rc}" $tmpdir/$tagrc

echo "Creating release-announcement-email.txt..."
cat << EOF > $projectdir/release-announcement-email.txt
To: dev@iceberg.apache.org
Subject: [VOTE] Release Apache Iceberg ${version} RC${rc}

Hi Everyone,

I propose that we release the following RC as the official Apache Iceberg ${version} release.

The commit ID is ${release_hash}
* This corresponds to the tag: apache-iceberg-${version}-rc${rc}
* https://github.com/apache/iceberg/commits/apache-iceberg-${version}-rc${rc}
* https://github.com/apache/iceberg/tree/${release_hash}

The release tarball, signature, and checksums are here:
* https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-${version}-rc${rc}

You can find the KEYS file here:
* https://dist.apache.org/repos/dist/dev/iceberg/KEYS

Convenience binary artifacts are staged on Nexus. The Maven repository URL is:
* https://repository.apache.org/content/repositories/orgapacheiceberg-<ID>/

Please download, verify, and test.

Please vote in the next 72 hours. (Weekends excluded)

[ ] +1 Release this as Apache Iceberg ${version}
[ ] +0
[ ] -1 Do not release this because...

Only PMC members have binding votes, but other community members are encouraged to cast
non-binding votes. This vote will pass if there are 3 binding +1 votes and more binding
+1 votes than -1 votes.
EOF

echo "Success! The release candidate is available here:"
echo "  https://dist.apache.org/repos/dist/dev/iceberg/$tagrc"
echo ""
echo "Commit SHA1: $release_hash"
echo ""
echo "We have generated a release announcement email for you here:"
echo "$projectdir/release-announcement-email.txt"
echo ""
echo "Please note that you must update the Nexus repository URL"
echo "contained in the mail before sending it out."

rm -rf $tmpdir
