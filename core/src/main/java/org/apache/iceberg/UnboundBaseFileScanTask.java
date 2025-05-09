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

class UnboundBaseFileScanTask extends BaseFileScanTask {
  private UnboundGenericDataFile unboundDataFile;
  private UnboundGenericDeleteFile[] unboundDeleteFiles;
  private Expression filter;

  UnboundBaseFileScanTask(
      UnboundGenericDataFile unboundDataFile,
      UnboundGenericDeleteFile[] unboundDeleteFiles,
      Expression filter) {
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
    GenericDataFile boundDataFile = unboundDataFile.bindToSpec(spec);
    DeleteFile[] boundDeleteFiles = new DeleteFile[unboundDeleteFiles.length];
    for (int i = 0; i < unboundDeleteFiles.length; i++) {
      boundDeleteFiles[i] = unboundDeleteFiles[i].bindToSpec(spec);
    }

    String schemaString = SchemaParser.toJson(spec.schema());
    String specString = PartitionSpecParser.toJson(spec);
    ResidualEvaluator boundResidual = ResidualEvaluator.of(spec, filter, caseSensitive);

    return new BaseFileScanTask(
        boundDataFile, boundDeleteFiles, schemaString, specString, boundResidual);
  }
}
