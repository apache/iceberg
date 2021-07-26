import unittest

from iceberg.api.types import (BooleanType, NestedField, StructType)
from iceberg.api.types import type_util
from iceberg.exceptions import ValidationException


class TestConversions(unittest.TestCase):

    def test_validate_schema_via_index_by_name(self):
        nested_type = NestedField.required(
            1, "a", StructType.of(
                [NestedField.required(2, "b", StructType.of(
                    [NestedField.required(3, "c", BooleanType.get())])),
                 NestedField.required(4, "b.c", BooleanType.get())]))

        with self.assertRaises(ValidationException) as context:
            type_util.index_by_name(StructType.of([nested_type]))
        self.assertTrue('Invalid schema: multiple fields for name a.b.c: 3 and 4' in str(context.exception))
