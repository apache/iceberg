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

import os
import shutil
from pathlib import Path

# Uncommend if your library can still function if extensions fail to compile.
allowed_to_fail = False
# allowed_to_fail = os.environ.get("CIBUILDWHEEL", "0") != "1"


def build_cython_extensions() -> None:
    import Cython.Compiler.Options  # pyright: ignore [reportMissingImports]
    from Cython.Build import build_ext, cythonize  # pyright: ignore [reportMissingImports]
    from setuptools import Extension
    from setuptools.dist import Distribution

    Cython.Compiler.Options.annotate = True

    if os.name == "nt":  # Windows
        extra_compile_args = [
            "/O2",
        ]
    else:  # UNIX-based systems
        extra_compile_args = [
            "-O3",
        ]
    # Relative to project root directory
    include_dirs = {
        "pyiceberg/",
    }

    extensions = [
        Extension(
            # Your .pyx file will be available to cpython at this location.
            "pyiceberg.avro.decoder_fast",
            [
                "pyiceberg/avro/decoder_fast.pyx",
            ],
            include_dirs=list(include_dirs),
            extra_compile_args=extra_compile_args,
            language="c",
        ),
    ]

    for extension in extensions:
        include_dirs.update(extension.include_dirs)

    ext_modules = cythonize(extensions, include_path=list(include_dirs), language_level=3, annotate=True)
    dist = Distribution({"ext_modules": ext_modules})
    cmd = build_ext(dist)
    cmd.ensure_finalized()

    cmd.run()

    for output in cmd.get_outputs():
        output = Path(output)
        relative_extension = output.relative_to(cmd.build_lib)
        shutil.copyfile(output, relative_extension)


try:
    build_cython_extensions()
except Exception:
    if not allowed_to_fail:
        raise
