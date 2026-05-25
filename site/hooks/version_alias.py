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

"""
MkDocs hook: version URL alias.

Creates a symlink so that /docs/<icebergVersion>/ resolves to /docs/latest/.
This allows version-specific URLs (e.g. /docs/1.11.0/) to work without
duplicating the navigation entry for the latest version.
"""

import logging
from pathlib import Path

log = logging.getLogger("mkdocs.hooks.version_alias")


def on_post_build(config):
    version = config["extra"].get("icebergVersion")
    if not version:
        return

    site_dir = Path(config["site_dir"])
    latest_dir = site_dir / "docs" / "latest"
    version_link = site_dir / "docs" / version

    if not latest_dir.exists():
        log.warning("docs/latest not found in site output; skipping version alias")
        return

    # Remove stale symlink if the version changed between rebuilds
    if version_link.is_symlink():
        version_link.unlink()

    if not version_link.exists():
        version_link.symlink_to("latest")
        log.info("Created version alias: docs/%s -> docs/latest", version)
