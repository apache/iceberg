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
package org.apache.iceberg.flink.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/**
 * This is a small util class that try to hide calls to Flink Internal or PublicEvolve interfaces as
 * Flink can change those APIs during minor version release.
 */
public class FlinkCompatibilityUtil {

  private FlinkCompatibilityUtil() {}

  public static TypeInformation<RowData> toTypeInfo(RowType rowType) {
    return InternalTypeInfo.of(rowType);
  }

  public static boolean isPhysicalColumn(TableColumn column) {
    return column.isPhysical();
  }

  public static boolean isPhysicalColumn(Column column) {
    return column.isPhysical();
  }
}
