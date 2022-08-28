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

import uuid

import pytest

from pyiceberg.io import fsspec


@pytest.mark.s3
def test_fsspec_new_input_file(fsspec_fileio):
    """Test creating a new input file from an fsspec file-io"""
    filename = str(uuid.uuid4())

    input_file = fsspec_fileio.new_input(f"s3://warehouse/{filename}")

    assert isinstance(input_file, fsspec.FsspecInputFile)
    assert input_file.location == f"s3://warehouse/{filename}"


@pytest.mark.s3
def test_fsspec_new_s3_output_file(fsspec_fileio):
    """Test creating a new output file from an fsspec file-io"""
    filename = str(uuid.uuid4())

    output_file = fsspec_fileio.new_output(f"s3://warehouse/{filename}")

    assert isinstance(output_file, fsspec.FsspecOutputFile)
    assert output_file.location == f"s3://warehouse/{filename}"


@pytest.mark.s3
def test_fsspec_write_and_read_file(fsspec_fileio):
    """Test writing and reading a file using FsspecInputFile and FsspecOutputFile"""
    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as f:
        f.write(b"foo")

    input_file = fsspec_fileio.new_input(f"s3://warehouse/{filename}")
    assert input_file.open().read() == b"foo"

    fsspec_fileio.delete(input_file)


@pytest.mark.s3
def test_fsspec_getting_length_of_file(fsspec_fileio):
    """Test getting the length of an FsspecInputFile and FsspecOutputFile"""
    filename = str(uuid.uuid4())

    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as f:
        f.write(b"foobar")

    assert len(output_file) == 6

    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    assert len(input_file) == 6

    fsspec_fileio.delete(output_file)


@pytest.mark.s3
def test_fsspec_file_tell(fsspec_fileio):
    """Test finding cursor position for an fsspec file-io file"""

    filename = str(uuid.uuid4())

    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as f:
        f.write(b"foobar")

    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    f = input_file.open()

    f.seek(0)
    assert f.tell() == 0
    f.seek(1)
    assert f.tell() == 1
    f.seek(3)
    assert f.tell() == 3
    f.seek(0)
    assert f.tell() == 0


@pytest.mark.s3
def test_fsspec_read_specified_bytes_for_file(fsspec_fileio):
    """Test reading a specified number of bytes from an fsspec file-io file"""

    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as f:
        f.write(b"foo")

    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    f = input_file.open()

    f.seek(0)
    assert b"f" == f.read(1)
    f.seek(0)
    assert b"fo" == f.read(2)
    f.seek(1)
    assert b"o" == f.read(1)
    f.seek(1)
    assert b"oo" == f.read(2)
    f.seek(0)
    assert b"foo" == f.read(999)  # test reading amount larger than entire content length

    fsspec_fileio.delete(input_file)


@pytest.mark.s3
def test_fsspec_raise_on_opening_file_not_found(fsspec_fileio):
    """Test that an fsppec input file raises appropriately when the s3 file is not found"""

    filename = str(uuid.uuid4())
    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    with pytest.raises(FileNotFoundError) as exc_info:
        input_file.open().read()

    assert filename in str(exc_info.value)


@pytest.mark.s3
def test_checking_if_a_file_exists(fsspec_fileio):
    """Test checking if a file exists"""

    non_existent_file = fsspec_fileio.new_input(location="s3://warehouse/does-not-exist.txt")
    assert not non_existent_file.exists()

    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    assert not output_file.exists()
    with output_file.create() as f:
        f.write(b"foo")

    existing_input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    assert existing_input_file.exists()

    existing_output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    assert existing_output_file.exists()

    fsspec_fileio.delete(existing_output_file)


@pytest.mark.s3
def test_closing_a_file(fsspec_fileio):
    """Test closing an output file and input file"""
    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as f:
        f.write(b"foo")
        assert not f.closed
    assert f.closed

    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    f = input_file.open()
    assert not f.closed
    f.close()
    assert f.closed

    fsspec_fileio.delete(f"s3://warehouse/{filename}")


@pytest.mark.s3
def test_fsspec_converting_an_outputfile_to_an_inputfile(fsspec_fileio):
    """Test converting an output file to an input file"""
    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    input_file = output_file.to_input_file()
    assert input_file.location == output_file.location
