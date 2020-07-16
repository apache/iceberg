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

package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;

class WriterUtil {
  private WriterUtil() {
  }

  static Function<InternalRow, PartitionKey> buildKeyGetter(PartitionSpec spec, Schema schema) {
    PartitionKey key = new PartitionKey(spec, schema);
    InternalRowWrapper wrapper = new InternalRowWrapper(SparkSchemaUtil.convert(schema));

    return row -> {
      key.partition(wrapper.wrap(row));
      return key;
    };
  }

  static TaskResult createTaskResult(List<DataFile> dataFiles) {
    return new TaskResult(dataFiles);
  }
}
