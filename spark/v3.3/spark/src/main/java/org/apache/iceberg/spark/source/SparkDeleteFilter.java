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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;

public class SparkDeleteFilter extends DeleteFilter<InternalRow> {
  private final InternalRowWrapper asStructLike;
  private final BaseReader reader;

  SparkDeleteFilter(FileScanTask task, Schema tableSchema, Schema requestedSchema, BaseReader reader) {
    super(task.file().path().toString(), task.deletes(), tableSchema, requestedSchema);
    this.asStructLike = new InternalRowWrapper(SparkSchemaUtil.convert(requiredSchema()));
    this.reader =  reader;
  }

  SparkDeleteFilter(String filePath, List<DeleteFile> deletes, Schema tableSchema, Schema requestedSchema,
                    BaseReader reader) {
    super(filePath, deletes, tableSchema, requestedSchema);
    this.asStructLike = new InternalRowWrapper(SparkSchemaUtil.convert(requiredSchema()));
    this.reader = reader;
  }

  @Override
  protected StructLike asStructLike(InternalRow row) {
    return asStructLike.wrap(row);
  }

  @Override
  protected InputFile getInputFile(String location) {
    return reader.getInputFile(location);
  }

  @Override
  protected void markRowDeleted(InternalRow row) {
    row.setBoolean(columnIsDeletedPosition(), true);
  }
}
