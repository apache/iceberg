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
package org.apache.iceberg.spark;

import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class IcebergSpark {
  private IcebergSpark() {}

  public static void registerBucketUDF(
      SparkSession session, String funcName, DataType sourceType, int numBuckets) {
    SparkTypeToType typeConverter = new SparkTypeToType();
    Type sourceIcebergType = typeConverter.atomic(sourceType);
    Transform<Object, Integer> bucket = Transforms.bucket(sourceIcebergType, numBuckets);
    session
        .udf()
        .register(
            funcName,
            value -> bucket.apply(SparkValueConverter.convert(sourceIcebergType, value)),
            DataTypes.IntegerType);
  }

  public static void registerTruncateUDF(
      SparkSession session, String funcName, DataType sourceType, int width) {
    SparkTypeToType typeConverter = new SparkTypeToType();
    Type sourceIcebergType = typeConverter.atomic(sourceType);
    Transform<Object, Object> truncate = Transforms.truncate(sourceIcebergType, width);
    session
        .udf()
        .register(
            funcName,
            value -> truncate.apply(SparkValueConverter.convert(sourceIcebergType, value)),
            sourceType);
  }
}
