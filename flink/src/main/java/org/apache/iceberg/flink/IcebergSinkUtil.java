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

import java.util.Locale;
import java.util.Map;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

class IcebergSinkUtil {
  private IcebergSinkUtil() {
  }

  static IcebergStreamWriter<RowData> createStreamWriter(Table table, TableSchema requestedSchema) {
    Preconditions.checkArgument(table != null, "Iceberg table should't be null");

    RowType flinkSchema;
    if (requestedSchema != null) {
      // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing iceberg schema.
      Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), table.schema());
      TypeUtil.validateWriteSchema(table.schema(), writeSchema, true, true);

      // We use this flink schema to read values from RowData. The flink's TINYINT and SMALLINT will be promoted to
      // iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT (backend by 1 'byte'), we will
      // read 4 bytes rather than 1 byte, it will mess up the byte array in BinaryRowData. So here we must use flink
      // schema.
      flinkSchema = (RowType) requestedSchema.toRowDataType().getLogicalType();
    } else {
      flinkSchema = FlinkSchemaUtil.convert(table.schema());
    }

    Map<String, String> props = table.properties();
    long targetFileSize = getTargetFileSizeBytes(props);
    FileFormat fileFormat = getFileFormat(props);

    TaskWriterFactory<RowData> taskWriterFactory = new RowDataTaskWriterFactory(table.schema(), flinkSchema,
        table.spec(), table.locationProvider(), table.io(), table.encryption(), targetFileSize, fileFormat, props);

    return new IcebergStreamWriter<>(table.toString(), taskWriterFactory);
  }

  private static FileFormat getFileFormat(Map<String, String> properties) {
    String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  private static long getTargetFileSizeBytes(Map<String, String> properties) {
    return PropertyUtil.propertyAsLong(properties,
        WRITE_TARGET_FILE_SIZE_BYTES,
        WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }
}
