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
package org.apache.iceberg.flink.source.reader;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(ParameterizedTestExtension.class)
public class TestColumnStatsWatermarkExtractorNano extends TestColumnStatsWatermarkExtractorBase {

  public static Schema SCHEMA =
      new Schema(
          required(1, "timestamp_column", Types.TimestampNanoType.withoutZone()),
          required(2, "timestamptz_column", Types.TimestampNanoType.withZone()),
          required(3, "long_column", Types.LongType.get()),
          required(4, "string_column", Types.StringType.get()));

  @RegisterExtension
  private static final HadoopTableExtension SOURCE_TABLE_EXTENSION =
      new HadoopTableExtension(DATABASE, TestFixtures.TABLE, SCHEMA, true);

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }
}
