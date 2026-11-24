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
package org.apache.iceberg.mr.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.EncryptedTableReadTestBase;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.util.StructLikeSet;

class TestIcebergInputFormatEncryption extends EncryptedTableReadTestBase {

  @Override
  protected StructLikeSet read(Table encryptedTable) throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(encryptedTable.schema()));
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    IcebergInputFormat<Record> inputFormat = new IcebergInputFormat<>();
    Table serializableTable = SerializableTable.copyOf(encryptedTable);

    StructLikeSet actual = StructLikeSet.create(encryptedTable.schema().asStruct());
    try (CloseableIterable<CombinedScanTask> tasks = encryptedTable.newScan().planTasks()) {
      for (CombinedScanTask task : tasks) {
        IcebergSplit split = new IcebergSplit(serializableTable, conf, task);
        try (RecordReader<Void, Record> reader = inputFormat.createRecordReader(split, context)) {
          reader.initialize(split, context);
          while (reader.nextKeyValue()) {
            actual.add(reader.getCurrentValue());
          }
        }
      }
    }

    return actual;
  }
}
