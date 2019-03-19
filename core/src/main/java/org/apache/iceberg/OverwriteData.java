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

package com.netflix.iceberg;

import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.expressions.Evaluator;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Projections;
import com.netflix.iceberg.expressions.StrictMetricsEvaluator;
import java.util.List;

public class OverwriteData extends MergingSnapshotUpdate implements OverwriteFiles {
  private boolean validateAddedFiles = false;

  protected OverwriteData(TableOperations ops) {
    super(ops);
  }

  @Override
  protected String operation() {
    return DataOperations.OVERWRITE;
  }

  @Override
  public OverwriteFiles overwriteByRowFilter(Expression expr) {
    deleteByRowFilter(expr);
    return this;
  }

  @Override
  public OverwriteFiles addFile(DataFile file) {
    add(file);
    return this;
  }

  @Override
  public OverwriteFiles validateAddedFiles() {
    this.validateAddedFiles = true;
    return this;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    if (validateAddedFiles) {
      PartitionSpec spec = writeSpec();
      Expression rowFilter = rowFilter();

      Expression inclusiveExpr = Projections.inclusive(spec).project(rowFilter);
      Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);

      Expression strictExpr = Projections.strict(spec).project(rowFilter);
      Evaluator strict = new Evaluator(spec.partitionType(), strictExpr);

      StrictMetricsEvaluator metrics = new StrictMetricsEvaluator(
          base.schema(), rowFilter);

      for (DataFile file : addedFiles()) {
        // the real test is that the strict or metrics test matches the file, indicating that all
        // records in the file match the filter. inclusive is used to avoid testing the metrics,
        // which is more complicated
        ValidationException.check(
            inclusive.eval(file.partition()) &&
                (strict.eval(file.partition()) || metrics.eval(file)),
            "Cannot append file with rows that do not match filter: %s: %s",
            rowFilter, file.path());
      }
    }

    return super.apply(base);
  }
}
