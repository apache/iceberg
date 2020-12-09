/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergTimestampWithZoneObjectInspectorHive3 {

    @Test
    public void testIcebergTimestampLocalTZObjectInspector() {
        TimestampLocalTZObjectInspector oi = IcebergTimestampWithZoneObjectInspectorHive3.get();

        Assert.assertEquals(ObjectInspector.Category.PRIMITIVE, oi.getCategory());
        Assert.assertEquals(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ, oi.getPrimitiveCategory());

        Assert.assertEquals(TypeInfoFactory.timestampLocalTZTypeInfo, oi.getTypeInfo());
        Assert.assertEquals(TypeInfoFactory.timestampLocalTZTypeInfo.getTypeName(), oi.getTypeName());

        Assert.assertEquals(TimestampTZ.class, oi.getJavaPrimitiveClass());
        Assert.assertEquals(TimestampLocalTZWritable.class, oi.getPrimitiveWritableClass());

        Assert.assertNull(oi.copyObject(null));
        Assert.assertNull(oi.getPrimitiveJavaObject(null));
        Assert.assertNull(oi.getPrimitiveWritableObject(null));

        long epochSeconds = 1601471970L;
        OffsetDateTime offsetDateTime = OffsetDateTime.of(
            LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC), ZoneOffset.ofHours(4));
        TimestampTZ ts = new TimestampTZ(offsetDateTime.toZonedDateTime());

        Assert.assertEquals(ts, oi.getPrimitiveJavaObject(offsetDateTime));
        Assert.assertEquals(new TimestampLocalTZWritable(ts), oi.getPrimitiveWritableObject(offsetDateTime));

        TimestampTZ copy = (TimestampTZ) oi.copyObject(ts);

        Assert.assertEquals(ts, copy);
        Assert.assertNotSame(ts, copy);

        Assert.assertFalse(oi.preferWritable());
    }

}