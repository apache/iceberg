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

package org.apache.iceberg.flink.source.reader;

import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.flink.TableInfo;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRowDataIteratorBulkFormat extends BulkFormatTestBase {

  @Parameterized.Parameters(name = "fileFormat={0}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { FileFormat.AVRO },
        new Object[] { FileFormat.ORC },
        new Object[] { FileFormat.PARQUET }
    };
  }

  public TestRowDataIteratorBulkFormat(FileFormat fileFormat) {
    super(fileFormat);
  }

  @Override
  protected BulkFormat<RowData, IcebergSourceSplit> getBulkFormat() {
    return new RowDataIteratorBulkFormat(TableInfo.fromTable(table), scanContext, rowType);
  }
}
