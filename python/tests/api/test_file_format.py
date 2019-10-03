
from iceberg.api import FileFormat


def test_parquet():
    file_fmt = FileFormat.PARQUET
    file_name = "test_file.parquet"
    add_extension_file = "test_file"
    assert file_fmt.is_splittable()
    assert FileFormat.from_file_name(file_name) == FileFormat.PARQUET
    assert file_name == FileFormat.PARQUET.add_extension(add_extension_file)


def test_avro():
    file_fmt = FileFormat.AVRO
    file_name = "test_file.avro"
    add_extension_file = "test_file"
    assert file_fmt.is_splittable()
    assert FileFormat.from_file_name(file_name) == FileFormat.AVRO
    assert file_name == FileFormat.AVRO.add_extension(add_extension_file)


def test_orc():
    file_fmt = FileFormat.ORC
    file_name = "test_file.orc"
    add_extension_file = "test_file"
    assert file_fmt.is_splittable()
    assert FileFormat.from_file_name(file_name) == FileFormat.ORC
    assert file_name == FileFormat.ORC.add_extension(add_extension_file)
