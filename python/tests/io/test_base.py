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

import io

from iceberg.io.base import FileIO, InputFile, OutputFile


class FooInputFile(InputFile):
    def __len__(self):
        return io.BytesIO(b"foo").getbuffer().nbytes

    def exists(self):
        return True

    def __enter__(self):
        super().__enter__()
        return io.BytesIO(b"foo")

    def __exit__(self, exc_type, exc_value, exc_traceback):
        super().__exit__(exc_type, exc_value, exc_traceback)
        return


class FooOutputFile(OutputFile):
    def __call__(self, overwrite: bool = False, **kwargs):
        super().__call__(overwrite=True)
        return self

    def __len__(self):
        return len(self._file_obj)

    def exists(self):
        return True

    def to_input_file(self):
        return FooInputFile(location=self.location)

    def __enter__(self):
        self._mock_storage = io.BytesIO()
        return self._mock_storage

    def __exit__(self, exc_type, exc_value, exc_traceback):
        super().__exit__(exc_type, exc_value, exc_traceback)
        return


class FooFileIO(FileIO):
    def new_input(self, location: str):
        return FooInputFile(location=location)

    def new_output(self, location: str):
        return FooOutputFile(location=location)

    def delete(self, location: str):
        return


def test_custom_input_file():

    input_file = FooInputFile(location="foo/bar.json")
    assert input_file.location == "foo/bar.json"

    with input_file as f:
        data = f.read()

    assert data == b"foo"


def test_custom_output_file():

    output_file = FooOutputFile(location="foo/bar.json")
    assert output_file.location == "foo/bar.json"

    with output_file as f:
        f.write(b"foo")

    output_file._mock_storage.seek(0)
    assert output_file._mock_storage.read() == b"foo"


def test_custom_output_file_with_overwrite():

    output_file = FooOutputFile(location="foo/bar.json", overwrite=True)
    assert output_file.location == "foo/bar.json"
    assert output_file.overwrite == True

    with output_file as f:
        f.write(b"foo")

    output_file._mock_storage.seek(0)
    assert output_file._mock_storage.read() == b"foo"


def test_custom_file_io():

    file_io = FooFileIO()
    input_file = file_io.new_input(location="foo")
    output_file = file_io.new_output(location="bar")

    assert isinstance(input_file, FooInputFile)
    assert isinstance(output_file, FooOutputFile)
