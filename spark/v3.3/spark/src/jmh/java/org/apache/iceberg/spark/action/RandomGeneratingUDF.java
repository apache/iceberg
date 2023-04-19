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
package org.apache.iceberg.spark.action;

import static org.apache.spark.sql.functions.udf;

import java.io.Serializable;
import java.util.Random;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

class RandomGeneratingUDF implements Serializable {
  private final long uniqueValues;
  private Random rand = new Random();

  RandomGeneratingUDF(long uniqueValues) {
    this.uniqueValues = uniqueValues;
  }

  UserDefinedFunction randomLongUDF() {
    return udf(() -> rand.nextLong() % (uniqueValues / 2), DataTypes.LongType)
        .asNondeterministic()
        .asNonNullable();
  }

  UserDefinedFunction randomString() {
    return udf(
            () -> (String) RandomUtil.generatePrimitive(Types.StringType.get(), rand),
            DataTypes.StringType)
        .asNondeterministic()
        .asNonNullable();
  }
}
