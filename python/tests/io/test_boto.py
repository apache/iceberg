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
from botocore.exceptions import ClientError

from iceberg.io import boto


@pytest.mark.s3
def test_boto_new_input_file(boto_test_client_kwargs):
    """Test creating a new input file from a BotoFileIO"""
    filename = str(uuid.uuid4())

    fileio = boto.BotoFileIO(**boto_test_client_kwargs)
    input_file = fileio.new_input(f"s3://testbucket/{filename}")

    assert isinstance(input_file, boto.BotoInputFile)
    assert input_file.location == f"s3://testbucket/{filename}"


@pytest.mark.s3
def test_boto_new_output_file(boto_test_client_kwargs):
    """Test creating a new input file from a BotoFileIO"""
    filename = str(uuid.uuid4())

    fileio = boto.BotoFileIO(**boto_test_client_kwargs)
    output_file = fileio.new_output(f"s3://testbucket/{filename}")

    assert isinstance(output_file, boto.BotoOutputFile)
    assert output_file.location == f"s3://testbucket/{filename}"


@pytest.mark.s3
def test_boto_write_and_read_file(boto_test_client_kwargs):
    """Test writing and reading a file using BotoInputFile and BotoOutputFile"""
    filename = str(uuid.uuid4())
    output_file = boto.BotoOutputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    f = output_file.create()
    f.write(b"foo")
    f.close()

    input_file = boto.BotoInputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    assert input_file.open().read() == b"foo"

    fileio = boto.BotoFileIO(**boto_test_client_kwargs)
    fileio.delete(input_file)


@pytest.mark.s3
def test_length_of_file(boto_test_client_kwargs):
    """Test writing and reading a file using BotoInputFile and BotoOutputFile"""
    filename = str(uuid.uuid4())

    output_file = boto.BotoOutputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    f = output_file.create()
    f.write(b"foobar")
    f.close()
    assert len(output_file) == 6

    input_file = boto.BotoInputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    assert len(input_file) == 6

    fileio = boto.BotoFileIO(**boto_test_client_kwargs)
    fileio.delete(output_file)


@pytest.mark.s3
def test_boto_seek_invalid_whence(boto_test_client_kwargs):
    """Test seeking with an invalid whence"""

    input_file = boto.BotoInputFile(location=f"s3://testbucket/{str(uuid.uuid4())}", **boto_test_client_kwargs)
    f = input_file.open()

    with pytest.raises(ValueError) as exc_info:
        f.seek(offset=0, whence=3)

    assert "Cannot seek to position 0, invalid whence: 3" in str(exc_info.value)


@pytest.mark.s3
def test_boto_tell(boto_test_client_kwargs):
    """Test writing a file using BotoInputFile and BotoOutputFile"""

    input_file = boto.BotoInputFile(location=f"s3://testbucket/{str(uuid.uuid4())}", **boto_test_client_kwargs)
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
def test_boto_read_specified_bytes(boto_test_client_kwargs):
    """Test reading a specified number of bytes from a BotoInputFile"""

    filename = str(uuid.uuid4())
    output_file = boto.BotoOutputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    f = output_file.create()
    f.write(b"foo")
    f.close()

    input_file = boto.BotoInputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    f = input_file.open()

    f.seek(0)
    assert b"f" == f.read(n=1)
    f.seek(0)
    assert b"fo" == f.read(n=2)
    f.seek(1)
    assert b"o" == f.read(n=1)
    f.seek(1)
    assert b"oo" == f.read(n=2)
    f.seek(0)
    assert b"foo" == f.read(n=999)  # test reading amount larger than entire content length

    fileio = boto.BotoFileIO(**boto_test_client_kwargs)
    fileio.delete(input_file)


@pytest.mark.s3
def test_boto_invalid_scheme(boto_test_client_kwargs):
    """Test that a ValueError is raised if a location is provided with an invalid scheme"""

    with pytest.raises(ValueError) as exc_info:
        boto.BotoInputFile(location="foo://bar/baz.txt", **boto_test_client_kwargs)

    assert ("Cannot create BotoInputFile, scheme not supported: foo") in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        boto.BotoOutputFile(location="foo://bar/baz.txt", **boto_test_client_kwargs)

    assert ("Cannot create BotoOutputFile, scheme not supported: foo") in str(exc_info.value)


@pytest.mark.s3
def test_boto_invalid_scheme_when_deleting(boto_test_client_kwargs):
    """Test that a ValueError is raised when deleting location with an invalid scheme"""

    fileio = boto.BotoFileIO()
    with pytest.raises(ValueError) as exc_info:
        fileio.delete("foo://bar/baz.txt")

    assert ("Cannot delete location, scheme not supported: foo") in str(exc_info.value)


@pytest.mark.s3
def test_raise_on_opening_an_s3_file_not_found(boto_test_client_kwargs):
    """Test that a BotoInputFile raises appropriately when the file is not found"""

    input_file = boto.BotoInputFile(location=f"s3://testbucket/{str(uuid.uuid4())}", **boto_test_client_kwargs)
    with pytest.raises(ClientError) as exc_info:
        input_file.open().read()

    assert "An error occurred (404) when calling the HeadObject operation: Not Found" in str(exc_info.value)


@pytest.mark.s3
def test_raise_on_checking_if_file_exists(boto_test_client_kwargs):
    """Test checking if a file exists"""

    non_existent_file = boto.BotoInputFile(location=f"s3://testbucket/does-not-exist.txt", **boto_test_client_kwargs)
    assert not non_existent_file.exists()

    filename = str(uuid.uuid4())
    output_file = boto.BotoOutputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    f = output_file.create()
    f.write(b"foo")
    f.close()

    existing_input_file = boto.BotoInputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    assert existing_input_file.exists()

    existing_output_file = boto.BotoOutputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    assert existing_output_file.exists()

    fileio = boto.BotoFileIO(**boto_test_client_kwargs)
    fileio.delete(existing_output_file)


@pytest.mark.s3
def test_closing_a_file(boto_test_client_kwargs):
    """Test checking if a file exists"""
    filename = str(uuid.uuid4())
    output_file = boto.BotoOutputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    f = output_file.create()
    f.write(b"foo")
    assert not f.closed()
    f.close()
    assert f.closed()

    with pytest.raises(ValueError) as exc_info:
        f.write(b"foo")
    assert "Cannot write bytes, file closed" in str(exc_info.value)

    input_file = boto.BotoInputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    f = input_file.open()
    assert not f.closed()
    f.close()
    assert f.closed()

    with pytest.raises(ValueError) as exc_info:
        f.seek(0)
    assert "Cannot seek, file closed" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        f.read()
    assert "Cannot read, file closed" in str(exc_info.value)

    fileio = boto.BotoFileIO(**boto_test_client_kwargs)
    fileio.delete(f"s3://testbucket/{filename}")


@pytest.mark.s3
def test_converting_an_outputfile_to_an_inputfile(boto_test_client_kwargs):
    """Test checking if a file exists"""
    filename = str(uuid.uuid4())
    output_file = boto.BotoOutputFile(location=f"s3://testbucket/{filename}", **boto_test_client_kwargs)
    input_file = output_file.to_input_file()
    assert input_file.location == output_file.location
