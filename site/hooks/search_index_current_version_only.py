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
MkDocs hook: keep only the configured version's docs in the search index.

Excludes every page under ``docs/<segment>/`` where ``<segment>`` is not the
current ``extra.icebergVersion``. Top-level site pages (community, blog, etc.)
remain indexed. The ``docs/latest`` alias is available during the build for
link resolution, but search results stay on the configured version path.
"""

import logging

log = logging.getLogger("mkdocs.hooks.search_index_current_version_only")


def on_page_markdown(markdown, page, config, files):
    current = config.get("extra", {}).get("icebergVersion")
    if not current:
        return markdown

    parts = page.url.split("/", 2)
    is_versioned_doc = len(parts) >= 3 and parts[0] == "docs"

    if is_versioned_doc and parts[1] != current:
        meta = page.meta or {}
        search_meta = meta.get("search") or {}
        search_meta["exclude"] = True
        meta["search"] = search_meta
        page.meta = meta
        log.debug("Excluded from search: %s", page.url)

    return markdown
