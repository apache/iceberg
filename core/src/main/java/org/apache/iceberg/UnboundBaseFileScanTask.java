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
package org.apache.iceberg;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;

public class UnboundBaseFileScanTask extends BaseFileScanTask {
  private BaseFile dataFile;
  private BaseFile[] deleteFiles;
  private Expression filter;

  public UnboundBaseFileScanTask(BaseFile dataFile, BaseFile[] deleteFiles, Expression filter) {
    super((DataFile) dataFile, (DeleteFile[]) deleteFiles, null, null, null);
    this.dataFile = dataFile;
    this.deleteFiles = deleteFiles;
    this.filter = filter;
  }

  @Override
  public Schema schema() {
    throw new UnsupportedOperationException("schema() is not supported in UnboundBaseFileScanTask");
  }

  @Override
  public PartitionSpec spec() {
    throw new UnsupportedOperationException("spec() is not supported in UnboundBaseFileScanTask");
  }

  public Expression filter() {
    return filter;
  }

  public FileScanTask bind(Schema schema, PartitionSpec spec, boolean caseSensitive) {
    // TODO before creating a new task
    // need to ensure that dataFile is refreshed with correct paritionData using parition spec
    // need to ensure deleteFiles is refreshed with spec info
    // need to ensure residual refreshed with spec.

    String schemaString = SchemaParser.toJson(schema);
    String specString = PartitionSpecParser.toJson(spec);
    ResidualEvaluator residualEvaluator = ResidualEvaluator.of(spec, filter, caseSensitive);

    PartitionData partitionData = new PartitionData(spec.partitionType());
    dataFile.setPartitionData(partitionData);
    for (BaseFile deleteFile : deleteFiles) {
      deleteFile.setPartitionData(partitionData);
    }

    return new BaseFileScanTask(
        (DataFile) dataFile,
        (DeleteFile[]) deleteFiles,
        schemaString,
        specString,
        residualEvaluator);
  }
}
