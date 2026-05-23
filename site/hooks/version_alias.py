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
MkDocs hook: latest URL alias.

The canonical built directory for the current release is /docs/<icebergVersion>/.
This hook creates /docs/latest/ as a symlink to it so both URLs resolve.
"""

import logging
from pathlib import Path

log = logging.getLogger("mkdocs.hooks.version_alias")


def on_post_build(config):
    version = config.get("extra", {}).get("icebergVersion")
    if not version:
        log.warning("extra.icebergVersion is not set; skipping docs/latest alias")
        return

    site_dir = Path(config["site_dir"])
    version_dir = site_dir / "docs" / version
    latest_link = site_dir / "docs" / "latest"

    if not version_dir.exists():
        log.warning("docs/%s not found in site output; skipping latest alias", version)
        return

    if latest_link.is_symlink():
        latest_link.unlink()
    elif latest_link.exists():
        log.warning("docs/latest exists as a real path; refusing to replace")
        return

    latest_link.symlink_to(version)
    log.info("Created latest alias: docs/latest -> docs/%s", version)
