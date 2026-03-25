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

"""Check that all GitHub Actions uses: refs are on the ASF allowlist.

Usage:
    python3 .github/scripts/check_asf_allowlist.py <allowlist_path>

The allowlist is the approved_patterns.yml file from
https://github.com/apache/infrastructure-actions/blob/main/approved_patterns.yml.

Exits with code 1 if any action ref is not allowlisted.
"""

import fnmatch
import glob
import sys

import yaml

# actions/*, github/*, apache/* are implicitly trusted by GitHub/ASF
# https://github.com/apache/infrastructure-actions/blob/main?tab=readme-ov-file#management-of-organization-wide-github-actions-allow-list
TRUSTED_OWNERS = {"actions", "github", "apache"}

# Glob pattern for YAML files to scan for action refs
GITHUB_YAML_GLOB = ".github/**/*.yml"

# Prefixes that indicate local or non-GitHub refs (not subject to allowlist)
LOCAL_REF_PREFIXES = ("./",)

# YAML key that references a GitHub Action
USES_KEY = "uses"


def find_action_refs(node):
    """Recursively find all `uses:` values from a parsed YAML tree."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == USES_KEY and isinstance(value, str):
                yield value
            else:
                yield from find_action_refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from find_action_refs(item)


def collect_action_refs():
    """Collect all third-party action refs from YAML files under .github/.

    Returns a dict mapping each action ref to the list of file paths that use it.
    Local refs (./) are excluded.
    """
    action_refs = {}
    for filepath in sorted(glob.glob(GITHUB_YAML_GLOB, recursive=True)):
        with open(filepath) as f:
            content = yaml.safe_load(f)
        if not content:
            continue
        for ref in find_action_refs(content):
            if ref.startswith(LOCAL_REF_PREFIXES):
                continue
            action_refs.setdefault(ref, []).append(filepath)
    return action_refs


def load_allowlist(allowlist_path):
    """Load the ASF approved_patterns.yml file.

    The file is a flat YAML list of entries like:
      - owner/action@<sha>       (exact SHA match)
      - owner/action@*           (any ref allowed)
      - golangci/*@*             (any repo under owner, any ref)

    Python's fnmatch.fnmatch matches "/" with "*" (unlike shell globs),
    so these patterns work directly without transformation.
    """
    with open(allowlist_path) as f:
        return yaml.safe_load(f)


def is_allowed(action_ref, allowlist):
    """Check whether a single action ref is allowed."""
    if action_ref.split("/")[0] in TRUSTED_OWNERS:
        return True
    return any(fnmatch.fnmatch(action_ref, pattern) for pattern in allowlist)


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <allowlist_path>", file=sys.stderr)
        sys.exit(2)

    allowlist_path = sys.argv[1]
    allowlist = load_allowlist(allowlist_path)
    action_refs = collect_action_refs()

    violations = []
    for action_ref, filepaths in sorted(action_refs.items()):
        if not is_allowed(action_ref, allowlist):
            for filepath in filepaths:
                violations.append(f"{filepath}\t{action_ref}")

    if violations:
        print("::error::Found action(s) not on the ASF allowlist:")
        for violation in violations:
            print(violation)
        sys.exit(1)
    else:
        print(f"All {len(action_refs)} unique action refs are on the ASF allowlist")


if __name__ == "__main__":
    main()
