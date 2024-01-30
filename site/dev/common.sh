#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

REMOTE="iceberg_docs"

# Ensures the presence of a specified remote repository for documentation.
# If the remote doesn't exist, it adds it using the provided URL.
# Then, it fetches updates from the remote repository.
create_or_update_docs_remote () {
  echo " --> create or update docs remote"
  
  # Check if the remote exists before attempting to add it
  git config "remote.${REMOTE}.url" >/dev/null || 
    git remote add "${REMOTE}" https://github.com/apache/iceberg.git

  # Fetch updates from the remote repository
  git fetch "${REMOTE}"
}


# Pulls updates from a specified branch of a remote repository.
# Arguments:
#   $1: Branch name to pull updates from
pull_remote () {
  echo " --> pull remote"

  local BRANCH="$1"

  # Ensure the branch argument is not empty
  assert_not_empty "${BRANCH}"  

  # Perform a pull from the specified branch of the remote repository
  git pull "${REMOTE}" "${BRANCH}"  
}

# Pushes changes from a local branch to a specified branch of a remote repository.
# Arguments:
#   $1: Branch name to push changes to
push_remote () {
  echo " --> push remote"

  local BRANCH="$1"

  # Ensure the branch argument is not empty
  assert_not_empty "${BRANCH}"  

  # Push changes to the specified branch of the remote repository
  git push "${REMOTE}" "${BRANCH}"  
}

# Installs or upgrades dependencies specified in the 'requirements.txt' file using pip.
install_deps () {
  echo " --> install deps"

  # Use pip to install or upgrade dependencies from the 'requirements.txt' file quietly
  pip -q install -r requirements.txt --upgrade
}

# Checks if a provided argument is not empty. If empty, displays an error message and exits with a status code 1.
# Arguments:
#   $1: Argument to check for emptiness
assert_not_empty () {
  
  if [ -z "$1" ]; then
    echo "No argument supplied"

    # Exit with an error code if no argument is provided
    exit 1  
  fi
}

# Finds and retrieves the latest version of the documentation based on the directory structure.
# Assumes the documentation versions are numeric folders within 'docs/docs/'.
get_latest_version () {
  # Find the latest numeric folder within 'docs/docs/' structure
  local latest=$(ls -d docs/docs/[0-9]* | sort -V | tail -1)

  # Extract the version number from the latest directory path
  local latest_version=$(basename "${latest}")  

  # Output the latest version number
  echo "${latest_version}"  
}

# Creates a 'latest' version of the documentation based on a specified ICEBERG_VERSION.
# Arguments:
#   $1: ICEBERG_VERSION - The version number of the documentation to be treated as the latest.
create_latest () {
  echo " --> create latest"

  local ICEBERG_VERSION="$1"

  # Ensure ICEBERG_VERSION is not empty
  assert_not_empty "${ICEBERG_VERSION}"  

  # Output the provided ICEBERG_VERSION for verification
  echo "${ICEBERG_VERSION}"  

  # Remove any existing 'latest' directory and recreate it
  rm -rf docs/docs/latest/
  mkdir docs/docs/latest/

  # Create symbolic links and copy configuration files for the 'latest' documentation
  ln -s "../${ICEBERG_VERSION}/docs" docs/docs/latest/docs
  cp "docs/docs/${ICEBERG_VERSION}/mkdocs.yml" docs/docs/latest/

  cd docs/docs/

  # Update version information within the 'latest' documentation
  update_version "latest"  
  cd -
}

# Updates version information within the mkdocs.yml file for a specified ICEBERG_VERSION.
# Arguments:
#   $1: ICEBERG_VERSION - The version number used for updating the mkdocs.yml file.
update_version () {
  echo " --> update version"

  local ICEBERG_VERSION="$1"

  # Ensure ICEBERG_VERSION is not empty
  assert_not_empty "${ICEBERG_VERSION}"  

  # Update version information within the mkdocs.yml file using sed commands
  if [ "$(uname)" == "Darwin" ]
  then
    sed -i '' -E "s/(^site\_name:[[:space:]]+docs\/).*$/\1${ICEBERG_VERSION}/" ${ICEBERG_VERSION}/mkdocs.yml
    sed -i '' -E "s/(^[[:space:]]*-[[:space:]]+Javadoc:.*\/javadoc\/).*$/\1${ICEBERG_VERSION}/" ${ICEBERG_VERSION}/mkdocs.yml
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]
  then
    sed -i'' -E "s/(^site_name:[[:space:]]+docs\/)[^[:space:]]+/\1${ICEBERG_VERSION}/" "${ICEBERG_VERSION}/mkdocs.yml"
    sed -i'' -E "s/(^[[:space:]]*-[[:space:]]+Javadoc:.*\/javadoc\/).*$/\1${ICEBERG_VERSION}/" "${ICEBERG_VERSION}/mkdocs.yml"
  fi

}

# Excludes versioned documentation from search indexing by modifying .md files.
# Arguments:
#   $1: ICEBERG_VERSION - The version number of the documentation to exclude from search indexing.
search_exclude_versioned_docs () {
  echo " --> search exclude version docs"
  local ICEBERG_VERSION="$1"

  # Ensure ICEBERG_VERSION is not empty
  assert_not_empty "${ICEBERG_VERSION}"  

  cd "${ICEBERG_VERSION}/docs/"

  # Modify .md files to exclude versioned documentation from search indexing
  python3 -c "import os
for f in filter(lambda x: x.endswith('.md'), os.listdir()): lines = open(f).readlines(); open(f, 'w').writelines(lines[:2] + ['search:\n', '  exclude: true\n'] + lines[2:]);"

  cd -
}

# Sets up local worktrees for the documentation and performs operations related to different versions.
pull_versioned_docs () {
  echo " --> pull versioned docs"
  
  # Ensure the remote repository for documentation exists and is up-to-date
  create_or_update_docs_remote  

  rm -r docs/docs

  # Add local worktrees for documentation and javadoc from the remote repository
  git worktree add -f docs/docs "${REMOTE}/docs"
  git worktree add -f docs/javadoc "${REMOTE}/javadoc"
  
  # Retrieve the latest version of documentation for processing
  local latest_version=$(get_latest_version)  

  # Output the latest version for debugging purposes
  echo "Latest version is: ${latest_version}" 
  
  # Create the 'latest' version of documentation
  create_latest "${latest_version}"  
}

# Cleans up artifacts and temporary files generated during documentation management.
clean () {
  echo " --> clean"

  # Temporarily disable script exit on errors to ensure cleanup continues
  set +e 

  # Remove 'latest' directories and related Git worktrees
  rm -rf docs/docs/latest &> /dev/null
  git worktree remove docs/docs &> /dev/null
  git worktree remove docs/javadoc &> /dev/null

  git restore docs/docs

  # Remove any additional temporary artifacts (e.g., 'site/' directory)
  rm -rf site/ &> /dev/null

  set -e # Re-enable script exit on errors
}

