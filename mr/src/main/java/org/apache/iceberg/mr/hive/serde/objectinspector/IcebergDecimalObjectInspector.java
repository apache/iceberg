/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.mr.hive.serde.objectinspector;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public final class IcebergDecimalObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements HiveDecimalObjectInspector, WriteObjectInspector {

  private static final Cache<Integer, IcebergDecimalObjectInspector> CACHE =
      Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();

  public static IcebergDecimalObjectInspector get(int precision, int scale) {
    Preconditions.checkArgument(scale < precision);
    Preconditions.checkArgument(precision <= HiveDecimal.MAX_PRECISION);
    Preconditions.checkArgument(scale <= HiveDecimal.MAX_SCALE);

    Integer key = precision << 8 | scale;
    return CACHE.get(key, k -> new IcebergDecimalObjectInspector(precision, scale));
  }

  private IcebergDecimalObjectInspector(int precision, int scale) {
    super(new DecimalTypeInfo(precision, scale));
  }

  @Override
  public HiveDecimal getPrimitiveJavaObject(Object o) {
    return o == null ? null : HiveDecimal.create((BigDecimal) o);
  }

  @Override
  public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
    HiveDecimal decimal = getPrimitiveJavaObject(o);
    return decimal == null ? null : new HiveDecimalWritable(decimal);
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof HiveDecimal) {
      HiveDecimal decimal = (HiveDecimal) o;
      return HiveDecimal.create(decimal.bigDecimalValue());
    } else if (o instanceof BigDecimal) {
      BigDecimal copy = new BigDecimal(o.toString());
      return copy;
    } else {
      return o;
    }
  }

  @Override
  public BigDecimal convert(Object o) {
    if (o == null) {
      return null;
    }

    BigDecimal result = ((HiveDecimal) o).bigDecimalValue();
    // during the HiveDecimal to BigDecimal conversion the scale is lost, when the value is 0
    result = result.setScale(scale());
    return result;
  }
}
