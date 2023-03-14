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

from pathlib import Path

import mkdocs_gen_files  # type: ignore

root = Path(__file__).parent.parent
src_root = root.joinpath("pyiceberg")

for path in src_root.glob("**/*.py"):
    if "__init__" in str(path):  # skip __init__ files
        continue

    doc_path = Path("references", path.relative_to(root)).with_suffix(".md")

    with mkdocs_gen_files.open(doc_path, "w") as f:
        ident = ".".join(path.relative_to(root).with_suffix("").parts)
        print(f"::: {ident}", file=f)
