import random
import re
import string
from typing import Set

BUCKET_NAME = "test_bucket"
RANDOM_LENGTH = 20
LIST_TEST_NUMBER = 100
TABLE_METADATA_LOCATION_REGEX = re.compile(
    r"""s3://test_bucket/my_iceberg_database-[a-z]{20}.db/
    my_iceberg_table-[a-z]{20}/metadata/
    00000-[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}.metadata.json""",
    re.X,
)


def get_random_table_name() -> str:
    prefix = "my_iceberg_table-"
    random_tag = "".join(random.choice(string.ascii_letters) for _ in range(RANDOM_LENGTH))
    return (prefix + random_tag).lower()


def get_random_tables(n: int) -> Set[str]:
    return {get_random_table_name() for _ in range(n)}


def get_random_database_name() -> str:
    prefix = "my_iceberg_database-"
    random_tag = "".join(random.choice(string.ascii_letters) for _ in range(RANDOM_LENGTH))
    return (prefix + random_tag).lower()


def get_random_databases(n: int) -> Set[str]:
    return {get_random_database_name() for _ in range(n)}
