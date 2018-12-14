import unittest

from iceberg.api.types import (BinaryType,
                               BooleanType,
                               DateType,
                               DecimalType,
                               DoubleType,
                               FixedType,
                               FloatType,
                               IntegerType,
                               LongType,
                               StringType,
                               TimestampType,
                               TimeType,
                               UUIDType)

PRIMITIVES = [BinaryType.get(),
              BooleanType.get(),
              DateType.get(),
              DecimalType.of(9, 2),
              DecimalType.of(11, 2),
              DecimalType.of(9, 3),
              DoubleType.get(),
              FixedType.of_length(3),
              FixedType.of_length(4),
              FloatType.get(),
              IntegerType.get(),
              LongType.get(),
              StringType.get(),
              TimestampType.with_timezone(),
              TimestampType.without_timezone(),
              TimeType.get(),
              UUIDType.get()]


class TestReadabilityChecks(unittest.TestCase):

    def test_primitive_types(self):
        # TO-DO:  Need to implement CheckCompatibility in type_util
        pass
