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

The canonical built directory is /docs/<icebergVersion>/.
This hook copies it to /docs/latest/ so both URLs resolve. The source alias is
created during config processing so links to docs/latest/*.md are resolved by
MkDocs. The rendered output alias is created after the build.

A real output copy (not a symlink) is required because `mkdocs gh-deploy`
invokes ghp-import with its default `followlinks=False`, which silently drops
symlinked directories from the published `asf-site` branch.
"""

import logging
import shutil
from pathlib import Path

from mkdocs.structure.files import InclusionLevel

log = logging.getLogger("mkdocs.hooks.version_alias")


def on_config(config):
    version = _configured_version(config)
    if not version:
        log.warning("extra.icebergVersion is not set; skipping docs/latest alias")
        return config

    _copy_latest_alias(Path(config["docs_dir"]), version, "source")
    return config


def on_files(files, config):
    for file in files:
        if file.src_uri.startswith("docs/latest/"):
            file.inclusion = InclusionLevel.EXCLUDED

    return files


def on_post_build(config):
    version = _configured_version(config)
    if not version:
        log.warning("extra.icebergVersion is not set; skipping docs/latest alias")
        return

    _copy_latest_alias(Path(config["site_dir"]), version, "output")


def _configured_version(config):
    version = config.get("extra", {}).get("icebergVersion")
    return version


def _copy_latest_alias(root_dir, version, alias_type):
    version_dir = root_dir / "docs" / version
    latest_dir = root_dir / "docs" / "latest"
    if not version_dir.exists():
        log.warning("docs/%s not found in %s tree; skipping latest alias", version, alias_type)
        return

    if latest_dir.is_symlink() or latest_dir.is_file():
        latest_dir.unlink()
    elif latest_dir.is_dir():
        shutil.rmtree(latest_dir)

    shutil.copytree(version_dir, latest_dir, symlinks=False)
    log.info("Created latest %s alias: docs/latest (copy of docs/%s)", alias_type, version)
