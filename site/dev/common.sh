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

export REMOTE="iceberg_docs"
export VENV_DIR=".venv"

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

# Creates the virtual environment if it doesn't exist.
create_venv () {
  if [ ! -d "${VENV_DIR}" ]; then
    echo " --> creating virtual environment at ${VENV_DIR}"
    python3 -m venv "${VENV_DIR}"
  fi
}

# Installs or upgrades dependencies specified in the 'requirements.txt' file using pip.
install_deps () {
  echo " --> install deps"

  # Use pip from venv to install or upgrade dependencies from the 'requirements.txt' file quietly
  "${VENV_DIR}/bin/pip3" -q install -r requirements.txt --upgrade
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

# Creates a 'nightly' version of the documentation that points to the current versioned docs
# located at the root-level `/docs` directory.
create_nightly () {
  echo " --> create nightly"

  # Remove any existing 'nightly' directory and recreate it
  rm -rf docs/docs/nightly/
  mkdir docs/docs/nightly/

  # Create symbolic links and copy configuration files for the 'nightly' documentation
  ln -s "../../../../docs/docs/" docs/docs/nightly/docs
  cp "../docs/mkdocs.yml" docs/docs/nightly/

  cd docs/docs/

  # Update version information within the 'nightly' documentation
  update_version "nightly"  
  cd -

  # Remove any existing javadoc 'nightly' link
  rm -fr docs/javadoc/nightly

  # Create symbolic link to 'nightly' javadoc
  cd docs/javadoc
  ln -s latest nightly
  cd -
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
  local ICEBERG_VERSION="$1"

  # Ensure ICEBERG_VERSION is not empty
  assert_not_empty "${ICEBERG_VERSION}"

  echo " --> create latest from ${ICEBERG_VERSION}"

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

  # Remove any javadoc 'latest' symbolic link
  rm -rf docs/javadoc/latest

  # Create symbolic link for the 'latest' javadoc
  cd docs/javadoc
  ln -s "${ICEBERG_VERSION}" latest
  cd -
}

# Updates version information within the mkdocs.yml file for a specified ICEBERG_VERSION.
# Arguments:
#   $1: ICEBERG_VERSION - The version number used for updating the mkdocs.yml file.
update_version () {
  local ICEBERG_VERSION="$1"

  # Ensure ICEBERG_VERSION is not empty
  assert_not_empty "${ICEBERG_VERSION}"  

  echo " --> update version: ${ICEBERG_VERSION}"

  # Update version information within the mkdocs.yml file using sed commands
  if [ "$(uname)" == "Darwin" ]
  then
    /usr/bin/sed -i '' -E "s/(^site\_name:[[:space:]]+docs\/).*$/\1${ICEBERG_VERSION}/" ${ICEBERG_VERSION}/mkdocs.yml
    /usr/bin/sed -i '' -E "s/(^[[:space:]]*-[[:space:]]+Javadoc:.*\/javadoc\/).*$/\1${ICEBERG_VERSION}/" ${ICEBERG_VERSION}/mkdocs.yml
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]
  then
    sed -i'' -E "s/(^site_name:[[:space:]]+docs\/)[^[:space:]]+/\1${ICEBERG_VERSION}/" "${ICEBERG_VERSION}/mkdocs.yml"
    sed -i'' -E "s/(^[[:space:]]*-[[:space:]]+Javadoc:.*\/javadoc\/).*$/\1${ICEBERG_VERSION}/" "${ICEBERG_VERSION}/mkdocs.yml"
  fi

}

# Sets up local worktrees for the documentation and performs operations related to different versions.
pull_versioned_docs () {
  echo " --> pull versioned docs"
  
  # Ensure the remote repository for documentation exists and is up-to-date
  create_or_update_docs_remote  

  # Add local worktrees for documentation and javadoc either from the remote repository
  # or from a local branch.
  local docs_branch="${ICEBERG_VERSIONED_DOCS_BRANCH:-${REMOTE}/docs}"
  local javadoc_branch="${ICEBERG_VERSIONED_JAVADOC_BRANCH:-${REMOTE}/javadoc}"
  git worktree add -f docs/docs "${docs_branch}"
  git worktree add -f docs/javadoc "${javadoc_branch}"
  
  # Retrieve the latest version of documentation for processing
  local latest_version=$(get_latest_version)  

  # Output the latest version for debugging purposes
  echo "Latest version is: ${latest_version}"
  
  # Create the 'latest' version of documentation
  create_latest "${latest_version}"

  # Create the 'nightly' version of documentation
  create_nightly  
}

check_markdown_files () {
  echo " --> check markdown file styles"
  if ! "${VENV_DIR}/bin/python3" -m pymarkdown --config markdownlint.yml scan ../docs/docs/*.md docs/*.md README.md
  then
    echo "Markdown style issues found. Please run 'make lint-fix' to fix them."
    exit 1
  fi
}

fix_markdown_files () {
  echo " --> fix markdown file styles"
  "${VENV_DIR}/bin/python3" -m pymarkdown --config markdownlint.yml fix ../docs/docs/*.md docs/*.md README.md
}

# Cleans up artifacts and temporary files generated during documentation management.
clean () {
  echo " --> clean"

  # Temporarily disable script exit on errors to ensure cleanup continues
  set +e 

  # Remove temp directories and related Git worktrees
  rm -rf docs/docs/latest &> /dev/null
  rm -rf docs/docs/nightly &> /dev/null

  git worktree remove docs/docs &> /dev/null
  git worktree remove docs/javadoc &> /dev/null

  # Remove any remaining artifacts
  rm -rf docs/javadoc docs/docs docs/.asf.yaml site/

  set -e # Re-enable script exit on errors
}
