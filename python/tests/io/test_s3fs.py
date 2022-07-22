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

from pyiceberg.io import s3fs


@pytest.mark.s3
def test_s3fs_new_input_file(s3fs_fileio):
    """Test creating a new input file from an S3fsFileIO"""
    filename = str(uuid.uuid4())

    input_file = s3fs_fileio.new_input(f"s3://testbucket/{filename}")

    assert isinstance(input_file, s3fs.S3fsInputFile)
    assert input_file.location == f"s3://testbucket/{filename}"


@pytest.mark.s3
def test_s3fs_new_output_file(s3fs_fileio):
    """Test creating a new output file from an S3fsFileIO"""
    filename = str(uuid.uuid4())

    output_file = s3fs_fileio.new_output(f"s3://testbucket/{filename}")

    assert isinstance(output_file, s3fs.S3fsOutputFile)
    assert output_file.location == f"s3://testbucket/{filename}"


@pytest.mark.s3
def test_s3fs_write_and_read_file(s3fs_fileio):
    """Test writing and reading a file using S3fsInputFile and S3fsOutputFile"""
    filename = str(uuid.uuid4())
    output_file = s3fs_fileio.new_output(location=f"s3://testbucket/{filename}")
    f = output_file.create()
    f.write(b"foo")
    f.close()

    input_file = s3fs_fileio.new_input(f"s3://testbucket/{filename}")
    assert input_file.open().read() == b"foo"

    s3fs_fileio.delete(input_file)


@pytest.mark.s3
def test_getting_length_of_file(s3fs_fileio):
    """Test getting the length of an S3fsInputFile and S3fsOutputFile"""
    filename = str(uuid.uuid4())

    output_file = s3fs_fileio.new_output(location=f"s3://testbucket/{filename}")
    f = output_file.create()
    f.write(b"foobar")
    f.close()
    assert len(output_file) == 6

    input_file = s3fs_fileio.new_input(location=f"s3://testbucket/{filename}")
    assert len(input_file) == 6

    s3fs_fileio.delete(output_file)


@pytest.mark.s3
def test_s3fs_tell(s3fs_fileio):
    """Test finding cursor position for an S3fsInputFile"""

    filename = str(uuid.uuid4())

    output_file = s3fs_fileio.new_output(location=f"s3://testbucket/{filename}")
    f = output_file.create()
    f.write(b"foobar")
    f.close()

    input_file = s3fs_fileio.new_input(location=f"s3://testbucket/{filename}")
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
def test_s3fs_read_specified_bytes(s3fs_fileio):
    """Test reading a specified number of bytes from an S3fsInputFile"""

    filename = str(uuid.uuid4())
    output_file = s3fs_fileio.new_output(location=f"s3://testbucket/{filename}")
    f = output_file.create()
    f.write(b"foo")
    f.close()

    input_file = s3fs_fileio.new_input(location=f"s3://testbucket/{filename}")
    f = input_file.open()

    f.seek(0)
    assert b"f" == f.read(size=1)
    f.seek(0)
    assert b"fo" == f.read(size=2)
    f.seek(1)
    assert b"o" == f.read(size=1)
    f.seek(1)
    assert b"oo" == f.read(size=2)
    f.seek(0)
    assert b"foo" == f.read(size=999)  # test reading amount larger than entire content length

    s3fs_fileio.delete(input_file)


@pytest.mark.s3
def test_raise_on_opening_an_s3_file_not_found(s3fs_fileio):
    """Test that an S3fsInputFile raises appropriately when the file is not found"""

    filename = str(uuid.uuid4())
    input_file = s3fs_fileio.new_input(location=f"s3://testbucket/{filename}")
    with pytest.raises(FileNotFoundError) as exc_info:
        input_file.open().read()

    assert filename in str(exc_info.value)


@pytest.mark.s3
def test_checking_if_a_file_exists(s3fs_fileio):
    """Test checking if a file exists"""

    non_existent_file = s3fs_fileio.new_input(location="s3://testbucket/does-not-exist.txt")
    assert not non_existent_file.exists()

    filename = str(uuid.uuid4())
    output_file = s3fs_fileio.new_output(location=f"s3://testbucket/{filename}")
    assert not output_file.exists()
    f = output_file.create()
    f.write(b"foo")
    f.close()

    existing_input_file = s3fs_fileio.new_input(location=f"s3://testbucket/{filename}")
    assert existing_input_file.exists()

    existing_output_file = s3fs_fileio.new_output(location=f"s3://testbucket/{filename}")
    assert existing_output_file.exists()

    s3fs_fileio.delete(existing_output_file)


@pytest.mark.s3
def test_closing_a_file(s3fs_fileio):
    """Test closing an S3fsOutputFile and S3fsInputFile"""
    filename = str(uuid.uuid4())
    output_file = s3fs_fileio.new_output(location=f"s3://testbucket/{filename}")
    f = output_file.create()
    f.write(b"foo")
    assert not f.closed()
    f.close()
    assert f.closed()

    input_file = s3fs_fileio.new_input(location=f"s3://testbucket/{filename}")
    f = input_file.open()
    assert not f.closed()
    f.close()
    assert f.closed()

    s3fs_fileio.delete(f"s3://testbucket/{filename}")


@pytest.mark.s3
def test_converting_an_outputfile_to_an_inputfile(s3fs_fileio):
    """Test converting an S3fsOutputFile to an S3fsInputFile"""
    filename = str(uuid.uuid4())
    output_file = s3fs_fileio.new_output(location=f"s3://testbucket/{filename}")
    input_file = output_file.to_input_file()
    assert input_file.location == output_file.location
