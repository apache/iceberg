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
  private DataFile unboundDataFile;
  private DeleteFile[] unboundDeleteFiles;
  private Expression filter;

  public UnboundBaseFileScanTask(
      DataFile unboundDataFile, DeleteFile[] unboundDeleteFiles, Expression filter) {
    super(unboundDataFile, unboundDeleteFiles, null, null, ResidualEvaluator.unpartitioned(filter));
    this.unboundDataFile = unboundDataFile;
    this.unboundDeleteFiles = unboundDeleteFiles;
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

  public FileScanTask bind(PartitionSpec spec, boolean caseSensitive) {
    Metrics dataFileMetrics =
        new Metrics(
            unboundDataFile.recordCount(),
            unboundDataFile.columnSizes(),
            unboundDataFile.valueCounts(),
            unboundDataFile.nullValueCounts(),
            unboundDataFile.nanValueCounts());
    PartitionData partitionData = new PartitionData(spec.partitionType());
    GenericDataFile boundDataFile =
        new GenericDataFile(
            spec.specId(),
            (String) unboundDataFile.path(),
            unboundDataFile.format(),
            partitionData,
            unboundDataFile.fileSizeInBytes(),
            dataFileMetrics,
            unboundDataFile.keyMetadata(),
            unboundDataFile.splitOffsets(),
            unboundDataFile.sortOrderId());

    DeleteFile[] boundDeleteFiles = new DeleteFile[unboundDeleteFiles.length];
    for (int i = 0; i < unboundDeleteFiles.length; i++) {
      DeleteFile deleteFile = unboundDeleteFiles[i];
      Metrics deleteFileMetrics =
          new Metrics(
              deleteFile.recordCount(),
              deleteFile.columnSizes(),
              deleteFile.valueCounts(),
              deleteFile.nullValueCounts(),
              deleteFile.nanValueCounts());

      int[] equalityDeletes = null;
      if (deleteFile.equalityFieldIds() != null) {
        equalityDeletes =
            deleteFile.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray();
      }

      DeleteFile genericDeleteFile =
          new GenericDeleteFile(
              spec.specId(),
              deleteFile.content(),
              (String) deleteFile.path(),
              deleteFile.format(),
              partitionData,
              deleteFile.fileSizeInBytes(),
              deleteFileMetrics,
              equalityDeletes,
              deleteFile.sortOrderId(),
              deleteFile.splitOffsets(),
              deleteFile.keyMetadata());

      boundDeleteFiles[i] = genericDeleteFile;
    }

    String schemaString = SchemaParser.toJson(spec.schema());
    String specString = PartitionSpecParser.toJson(spec);
    ResidualEvaluator boundResidual = ResidualEvaluator.of(spec, filter, caseSensitive);

    return new BaseFileScanTask(
        boundDataFile, boundDeleteFiles, schemaString, specString, boundResidual);
  }
}
