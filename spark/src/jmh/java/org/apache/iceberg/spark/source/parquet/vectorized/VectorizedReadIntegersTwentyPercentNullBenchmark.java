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

package org.apache.iceberg.spark.source.parquet.vectorized;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.when;

public class VectorizedReadIntegersTwentyPercentNullBenchmark extends VectorizedReadIntegersBenchmark {
  @Override
  protected void appendData() {
    for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
      Dataset<Row> df = spark().range(NUM_ROWS)
          .withColumn("longCol", when(pmod(col("id"), lit(2)).equalTo(lit(0)), lit(null)).otherwise(col("id")))
          .drop("id")
          .withColumn("intCol", expr("CAST(longCol AS INT)"));
      appendAsFile(df);
    }
  }
}
