import os
import shutil
from distutils.core import Distribution, Extension

from Cython.Build import build_ext, cythonize

compile_args = ["-O3"]


def build():
    extensions = [
        Extension(
            "*",
            ["pyiceberg/avro/*.pyx"],
            extra_compile_args=compile_args,
        )
    ]
    ext_modules = cythonize(
        extensions,
        compiler_directives={"binding": True, "language_level": 3},
    )

    dist = Distribution({"ext_modules": ext_modules})
    cmd = build_ext(dist)
    cmd.ensure_finalized()
    cmd.run()

    for output in cmd.get_outputs():
        relative_extension = os.path.relpath(output, cmd.build_lib)
        shutil.copyfile(output, relative_extension)

    # cmd = build_ext(distribution)
    # cmd.ensure_finalized()
    # cmd.run()

    # # Copy built extensions back to the project
    # for output in cmd.get_outputs():
    #     relative_extension = os.path.relpath(output, cmd.build_lib)
    #     shutil.copyfile(output, relative_extension)
    #     mode = os.stat(relative_extension).st_mode
    #     mode |= (mode & 0o444) >> 2
    #     os.chmod(relative_extension, mode)


if __name__ == "__main__":
    build()
