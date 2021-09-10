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

package org.apache.iceberg.flink;

import java.io.Serializable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class IcebergWatermark implements Serializable {
  private final String watermarkFieldName;
  private final RowType flinkRowType;
  private final int watermarkFieldPos;

  public IcebergWatermark(String watermarkFieldName, RowType flinkRowType) {
    Preconditions.checkArgument(
        watermarkFieldName != null && !"".equals(watermarkFieldName),
        "watermarkFieldName must not empty.");
    Preconditions.checkArgument(flinkRowType != null, "flinkRowType must not null.");
    Preconditions.checkArgument(
        flinkRowType.getFieldIndex(watermarkFieldName) > -1,
        "flink schema must have watermarkFieldName.");

    this.watermarkFieldName = watermarkFieldName;
    this.flinkRowType = flinkRowType;
    this.watermarkFieldPos = flinkRowType.getFieldIndex(watermarkFieldName);
  }

  public Long getWatermark(RowData rowData) {
    return rowData.getLong(watermarkFieldPos);
  }

  public String getWatermarkFieldName() {
    return watermarkFieldName;
  }

  public RowType getFlinkRowType() {
    return flinkRowType;
  }

  public int getWatermarkFieldPos() {
    return watermarkFieldPos;
  }
}
