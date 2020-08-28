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

package org.apache.iceberg.flink.sink;

import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

public class FlinkSink {

  private static final String ICEBERG_STREAM_WRITER_NAME = IcebergStreamWriter.class.getSimpleName();
  private static final String ICEBERG_FILES_COMMITTER_NAME = IcebergFilesCommitter.class.getSimpleName();

  private FlinkSink() {
  }

  public static Builder forRow(DataStream<Row> input) {
    return new Builder().forRow(input);
  }

  public static Builder forRowData(DataStream<RowData> input) {
    return new Builder().forRowData(input);
  }

  public static class Builder {
    private DataStream<Row> rowInput = null;
    private DataStream<RowData> rowDataInput = null;
    private TableLoader tableLoader;
    private Configuration hadoopConf;
    private Table table;
    private TableSchema tableSchema;

    private Builder forRow(DataStream<Row> newRowInput) {
      this.rowInput = newRowInput;
      return this;
    }

    private Builder forRowData(DataStream<RowData> newRowDataInput) {
      this.rowDataInput = newRowDataInput;
      return this;
    }

    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder tableLoader(TableLoader newTableLoader) {
      this.tableLoader = newTableLoader;
      return this;
    }

    public Builder hadoopConf(Configuration newHadoopConf) {
      this.hadoopConf = newHadoopConf;
      return this;
    }

    public Builder tableSchema(TableSchema newTableSchema) {
      this.tableSchema = newTableSchema;
      return this;
    }

    private DataStream<RowData> convertToRowDataStream() {
      RowType rowType;
      DataType[] fieldDataTypes;
      if (tableSchema != null) {
        rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        fieldDataTypes = tableSchema.getFieldDataTypes();
      } else {
        rowType = FlinkSchemaUtil.convert(table.schema());
        fieldDataTypes = TypeConversions.fromLogicalToDataType(rowType.getChildren().toArray(new LogicalType[0]));
      }

      DataFormatConverters.RowConverter rowConverter = new DataFormatConverters.RowConverter(fieldDataTypes);

      return rowInput.map(rowConverter::toInternal, RowDataTypeInfo.of(rowType));
    }

    @SuppressWarnings("unchecked")
    public DataStreamSink<RowData> build() {
      Preconditions.checkArgument(rowInput != null || rowDataInput != null,
          "Neither Builder.forRow(..) nor Builder.forRowData(..) is called, " +
              "please use one of them to initialize the input DataStream.");
      Preconditions.checkArgument(rowInput == null || rowDataInput == null,
          "Both Builder.forRow(..) and Builder.forRowData(..) are called," +
              "please use only one of them to initialize the input DataStream.");
      Preconditions.checkNotNull(table, "Table shouldn't be null");
      Preconditions.checkNotNull(tableLoader, "Table loader shouldn't be null");
      Preconditions.checkNotNull(hadoopConf, "Hadoop configuration shouldn't be null");

      DataStream<RowData> inputStream = rowInput != null ? convertToRowDataStream() : rowDataInput;

      IcebergStreamWriter<RowData> streamWriter = createStreamWriter(table, tableSchema);
      IcebergFilesCommitter filesCommitter = new IcebergFilesCommitter(tableLoader, hadoopConf);

      DataStream<Void> returnStream = inputStream
          .transform(ICEBERG_STREAM_WRITER_NAME, TypeInformation.of(DataFile.class), streamWriter)
          .setParallelism(inputStream.getParallelism())
          .transform(ICEBERG_FILES_COMMITTER_NAME, Types.VOID, filesCommitter)
          .setParallelism(1)
          .setMaxParallelism(1);

      return returnStream.addSink(new DiscardingSink())
          .name(String.format("IcebergSink %s", table.toString()))
          .setParallelism(1);
    }
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
