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
package org.apache.iceberg.spark.broadcastvar.expressions;

import java.util.Random;
import org.apache.iceberg.spark.source.DummyBroadcastedJoinKeysWrapper;
import org.apache.iceberg.spark.source.broadcastvar.BroadcastHRUnboundPredicate;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class RangeInTestUtils {
  private static final Random random = new Random(System.currentTimeMillis());

  private RangeInTestUtils() {}

  public static BroadcastHRUnboundPredicate<Integer> createPredicate(
      String attrib, Object[] values) {
    BroadcastedJoinKeysWrapper dummyWrapper =
        new DummyBroadcastedJoinKeysWrapper(DataTypes.IntegerType, values, random.nextLong());
    return new BroadcastHRUnboundPredicate<>(attrib, dummyWrapper);
  }

  public static <T> BroadcastHRUnboundPredicate<T> createPredicate(
      String attrib, Object[] values, DataType dataType) {
    BroadcastedJoinKeysWrapper dummyWrapper =
        new DummyBroadcastedJoinKeysWrapper(dataType, values, random.nextLong());
    return new BroadcastHRUnboundPredicate<T>(attrib, dummyWrapper);
  }
}
