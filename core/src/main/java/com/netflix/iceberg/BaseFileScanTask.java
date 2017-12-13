/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.base.Objects;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.ResidualEvaluator;

class BaseFileScanTask implements FileScanTask {
  private final DataFile file;
  private final ResidualEvaluator residuals;

  BaseFileScanTask(DataFile file, ResidualEvaluator residuals) {
    this.file = file;
    this.residuals = residuals;
  }

  @Override
  public DataFile file() {
    return file;
  }

  @Override
  public long start() {
    return 0;
  }

  @Override
  public long length() {
    return file.fileSizeInBytes();
  }

  @Override
  public Expression residual() {
    return residuals.residualFor(file.partition());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("file", file.path())
        .add("partition_data", file.partition())
        .add("residual", residual())
        .toString();
  }
}
