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
    echo "  -g      Specify Git remote name. Defaults to \"origin\""
    # TODO - Not sure we need this one but might be nice to pass in.
    echo "  -b      Git branch to switch to. No-op action. Acts as a dry-run (as getopts won't support full flag names)."
    echo "  -t      Enables TEST mode. Does not commit to Github or SVN. No-op action. Acts as a dry-run."
    echo "  -d      Turn on DEBUG output"
    exit 1
}

# Defaults
remote="apache"  # Remote repository. Can use your fork for testing.
testing=false

while getopts "v:r:k:g:r:b:td" opt; do
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
    b)
      branch="${OPTARG}"
      ;;
    t)
      testing=true
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

# Check if we're in branch mode and create and switch to that branch. Error out if it already exists.
# TODO - We add this, do we need a way to get back to the original branch? Maybe testing flag is enough.
#if [ -z "$branch" ] && [ -n "$(git rev-parse --verify --quiet "${branch}")" ]
#then
#   echo "Cannot run: Specified branch name $branch already exists."
#   exit 1
#else
#   git branch "$branch"
#   git checkout "$branch"
#fi

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
projectdir="$(dirname "$scriptdir")"
tmpdir=$projectdir/tmp

echo "scriptdir: $scriptdir"
echo "projectdir: $projectdir"
echo "tmpdir: $tmpdir"

if [ -d "$tmpdir" ]; then
  echo "Cannot run: $tmpdir already exists"
  exit 1
fi

tag="apache-iceberg-$version"
tagrc="${tag}-rc${rc}"

echo "Preparing source for $tagrc"

echo "Adding version.txt and tagging release..."
echo "$version" > "$projectdir/version.txt"
git add "$projectdir/version.txt"
git commit -m "Add version.txt for release $version" $projectdir/version.txt

set_version_hash=`git rev-list HEAD 2> /dev/null | head -n 1 `
git tag -am "Apache Iceberg $version" $tagrc $set_version_hash

if [ $testing = true ]; then
  echo "Pushing $tagrc to $remote..."
  git push $remote $tagrc
else
  echo "In test mode. Skipping pushing $tagrc to $remote"
fi

release_hash=`git rev-list $tagrc 2> /dev/null | head -n 1 `
echo "set_version_hash = $set_version_hash"
echo "release_hash = $release_hash"
exit 1

if [ -z "$release_hash" ]; then
  echo "Cannot continue: unknown Git tag: $tag"
  exit 1
fi

# be conservative and use the release hash, even though git produces the same
# archive (identical hashes) using the scm tag
echo "Creating tarball ${tarball} using commit $release_hash from tag $tagrc"
tarball=$tag.tar.gz
git archive $release_hash --worktree-attributes --prefix $tag/ -o $projectdir/$tarball

echo "Signing the tarball..."
[[ -z "$keyid" ]] && keyopt="-u $keyid"
gpg --detach-sig $keyopt --armor --output ${projectdir}/${tarball}.asc ${projectdir}/$tarball
shasum -a 512 ${projectdir}/$tarball > ${projectdir}/${tarball}.sha512


echo "Checking out Iceberg RC subversion repo..."
svn co --depth=empty https://dist.apache.org/repos/dist/dev/iceberg $tmpdir

echo "Adding tarball to the Iceberg distribution Subversion repo..."
mkdir -p $tmpdir/$tagrc
cp $projectdir/${tarball}* $tmpdir/$tagrc
svn add $tmpdir/$tagrc

# TODO - This part needs to be skipped if you're not a committer (or in any dry-run situation).
# TODO - We also might need instructions for setting up svn. System MacOS doesn't work and needs to be overridden.
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

Please vote in the next 72 hours.

[ ] +1 Release this as Apache Iceberg <VERSION>
[ ] +0
[ ] -1 Do not release this because...
EOF

echo "Success! The release candidate is available here:"
echo "  https://dist.apache.org/repos/dist/dev/iceberg/$tagrc"
echo ""
echo "Commit SHA1: $release_hash"
echo ""
echo "We have generated a release announcement email for you here:"
echo "$projectdir/release_announcement_email.txt"
echo ""
echo "Please note that you must update the Nexus repository URL"
echo "contained in the mail before sending it out."

rm -rf $tmpdir
