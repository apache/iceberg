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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkInputSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class TestHelpers {

  private TestHelpers() {
  }

  public static List<Row> getRows(FlinkInputFormat inputFormat, RowType rowType) throws IOException {
    FlinkInputSplit[] splits = inputFormat.createInputSplits(0);
    List<Row> results = Lists.newArrayList();

    DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(
        TypeConversions.fromLogicalToDataType(rowType));

    for (FlinkInputSplit s : splits) {
      inputFormat.open(s);
      while (!inputFormat.reachedEnd()) {
        RowData row = inputFormat.nextRecord(null);
        results.add((Row) converter.toExternal(row));
      }
    }
    inputFormat.close();
    return results;
  }

  public static List<RowData> getRowData(FlinkInputFormat inputFormat, RowType rowType) throws IOException {
    DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(
        TypeConversions.fromLogicalToDataType(rowType));

    return TestHelpers.getRows(inputFormat, rowType).stream()
        .map(converter::toInternal)
        .map(RowData.class::cast)
        .collect(Collectors.toList());
  }
}
