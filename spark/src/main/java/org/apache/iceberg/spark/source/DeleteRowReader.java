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

import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;

public class DeleteRowReader extends RowDataReader {
  private final Schema tableSchema;
  private final Schema expectedSchema;

  public DeleteRowReader(CombinedScanTask task, Schema schema, String nameMapping, FileIO io,
                         EncryptionManager encryptionManager, boolean caseSensitive) {
    super(task, schema, schema, nameMapping, io, encryptionManager,
        caseSensitive);
    this.tableSchema = schema;
    Schema metaSchema = new Schema(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION);
    this.expectedSchema = TypeUtil.join(metaSchema, schema);
  }

  @Override
  CloseableIterator<InternalRow> open(FileScanTask task) {
    SparkDeleteMatcher matches = new SparkDeleteMatcher(task, tableSchema, expectedSchema);

    // schema or rows returned by readers
    Schema requiredSchema = matches.requiredSchema();
    Map<Integer, ?> idToConstant = PartitionUtil.constantsMap(task, RowDataReader::convertConstant);
    DataFile file = task.file();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(file.path().toString(), task.start(), task.length());

    return matches.matchEqDeletes(open(task, requiredSchema, idToConstant)).iterator();
  }

  protected class SparkDeleteMatcher extends DeleteFilter<InternalRow> {
    private final InternalRowWrapper asStructLike;

    SparkDeleteMatcher(FileScanTask task, Schema tableSchema, Schema requestedSchema) {
      super(task, tableSchema, requestedSchema);
      this.asStructLike = new InternalRowWrapper(SparkSchemaUtil.convert(requiredSchema()));
    }

    @Override
    protected StructLike asStructLike(InternalRow row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return DeleteRowReader.this.getInputFile(location);
    }
  }
}
