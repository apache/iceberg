from decimal import Decimal, getcontext
import unittest

import iceberg.api.expressions.literals as lit
from iceberg.api.transforms import (Bucket,
                                    BucketDouble,
                                    BucketFloat)
import iceberg.api.types as iceberg_types


class TestingBucketing(unittest.TestCase):

    def test_spec_values(self):
        self.assertEquals(1392991556, Bucket.get(iceberg_types.IntegerType.get(), 100).hash(1))
        self.assertEquals(2017239379, Bucket.get(iceberg_types.IntegerType.get(), 100).hash(34))
        self.assertEquals(2017239379, Bucket.get(iceberg_types.LongType.get(), 100).hash(34))
        self.assertEquals(-142385009, BucketFloat(100).hash(1.0))
        self.assertEquals(-142385009, BucketDouble(100).hash(1.0))
        # placeholder for decimal tests
        SCALE_FACTOR = Decimal(10) ** -2
        self.assertEquals(-500754589, Bucket.get(iceberg_types.DecimalType.of(9, 2),
                                                 100).hash(Decimal("14.20").quantize(SCALE_FACTOR)))
        getcontext().prec = 38
        SCALE_FACTOR = Decimal(10) ** -5
        self.assertEquals(-32334285, Bucket.get(iceberg_types.DecimalType.of(38, 5),
                                                100).hash(Decimal("137302769811943318102518958871258.37580")
                                                          .quantize(SCALE_FACTOR)))
        date = lit.Literal.of("2017-11-16").to(iceberg_types.DateType.get())
        self.assertEquals(-653330422, Bucket.get(iceberg_types.DateType.get(),
                                                 100).hash(date.value))
        time_value = lit.Literal.of("22:31:08").to(iceberg_types.TimeType.get())
        self.assertEquals(-662762989, Bucket.get(iceberg_types.TimeType.get(),
                                                 100).hash(time_value.value))
        timestamp_value = lit.Literal.of("2017-11-16T22:31:08").to(iceberg_types.TimestampType.without_timezone())
        self.assertEquals(-2047944441, Bucket.get(iceberg_types.TimestampType.without_timezone(),
                                                  100).hash(timestamp_value.value))

        timestamptz_value = lit.Literal.of("2017-11-16T14:31:08-08:00").to(iceberg_types.TimestampType.with_timezone())
        self.assertEquals(-2047944441, Bucket.get(iceberg_types.TimestampType.with_timezone(),
                                                  100).hash(timestamptz_value.value))

        # placeholder for uuid and byte buffer tests
        # uuid = lit.Literal.of("f79c3e09-677c-4bbd-a479-3f349cb785e7").to(iceberg_types.UUIDType.get())
        # self.assertEquals(1488055340, bucket.Bucket.get(iceberg_types.UUIDType.get(), 100).hash(uuid.value))

        """
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 1, 2, 3});
    Assert.assertEquals("Spec example: hash([00 01 02 03]) = -188683207",
        -188683207, Bucket.<ByteBuffer>get(Types.BinaryType.get(), 100).hash(bytes));
    Assert.assertEquals("Spec example: hash([00 01 02 03]) = -188683207",
        -188683207, Bucket.<ByteBuffer>get(Types.BinaryType.get(), 100).hash(bytes));"""
