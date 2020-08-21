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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

public class IcebergSinkUtil {

  private static final TypeInformation<DataFile> DATA_FILE_TYPE_INFO = TypeInformation.of(DataFile.class);
  private static final String ICEBERG_STREAM_WRITER_NAME = IcebergStreamWriter.class.getSimpleName();
  private static final String ICEBERG_FILES_COMMITTER_NAME = IcebergFilesCommitter.class.getSimpleName();

  private IcebergSinkUtil() {
  }

  public static Builder builder() {
    return new Builder();
  }

  private enum CatalogType {
    HIVE("hive"), HADOOP("hadoop");

    private final String catalogType;

    CatalogType(String catalogType) {
      this.catalogType = catalogType;
    }

    @Override
    public String toString() {
      return catalogType;
    }

    public static CatalogType of(String catalogType) {
      for (CatalogType type : CatalogType.values()) {
        if (Comparators.charSequences().compare(type.catalogType, catalogType) == 0) {
          return type;
        }
      }
      return null;
    }
  }

  public static class Builder {
    private DataStream<RowData> inputStream;
    private Table table;
    private TableIdentifier tableIdentifier;
    private Configuration hadoopConf;
    private CatalogType catalogType;
    private String catalogName;
    private String hadoopWarehouseLocation;
    private String hiveURI;
    private int hiveClientPoolSize;
    private TableSchema flinkSchema;

    public Builder inputStream(DataStream<RowData> newInputStream) {
      this.inputStream = newInputStream;
      return this;
    }

    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    public Builder tableIdentifier(TableIdentifier newTableIdentifier) {
      this.tableIdentifier = newTableIdentifier;
      return this;
    }

    public Builder hadoopConf(Configuration newHadoopConf) {
      this.hadoopConf = newHadoopConf;
      return this;
    }

    public Builder catalogType(String newCatalogType) {
      this.catalogType = CatalogType.of(newCatalogType);
      return this;
    }

    public Builder catalogName(String newCatalogName) {
      this.catalogName = newCatalogName;
      return this;
    }

    public Builder warehouseLocation(String newHadoopWarehouseLocation) {
      this.hadoopWarehouseLocation = newHadoopWarehouseLocation;
      return this;
    }

    public Builder hiveURI(String newHiveURI) {
      this.hiveURI = newHiveURI;
      return this;
    }

    public Builder hiveClientPoolSize(int clientPoolSize) {
      this.hiveClientPoolSize = clientPoolSize;
      return this;
    }

    public Builder flinkSchema(TableSchema newTableSchema) {
      this.flinkSchema = newTableSchema;
      return this;
    }

    @SuppressWarnings("unchecked")
    public DataStreamSink<RowData> build() {
      Preconditions.checkNotNull(inputStream, "Input DataStream shouldn't be null");
      Preconditions.checkNotNull(table, "Table shouldn't be null");
      Preconditions.checkNotNull(tableIdentifier, "TableIdentifier shouldn't be null");
      Preconditions.checkNotNull(hadoopConf, "Hadoop configuration shouldn't be null");

      // Initialize the CatalogLoader and TableLoader.
      CatalogLoader catalogLoader;
      Preconditions.checkNotNull(catalogType, "CatalogType should be 'hive' or 'hadoop'");
      Preconditions.checkNotNull(catalogName, "CatalogName shouldn't be null");
      switch (catalogType) {
        case HIVE:
          Preconditions.checkArgument(hiveURI != null, "Hive URI shouldn't be null");
          Preconditions.checkArgument(hiveClientPoolSize > 0, "Hive client pool size should be positive");
          catalogLoader = CatalogLoader.hive(catalogName, hiveURI, hiveClientPoolSize);
          break;
        case HADOOP:
          Preconditions.checkNotNull(hadoopWarehouseLocation, "Hadoop warehouse location shouldn't be null");
          catalogLoader = CatalogLoader.hadoop(catalogName, hadoopWarehouseLocation);
          break;
        default:
          throw new IllegalArgumentException("Invalid catalog type: " + catalogType);
      }
      TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

      IcebergStreamWriter<RowData> streamWriter = createStreamWriter(table, flinkSchema);
      IcebergFilesCommitter filesCommitter = new IcebergFilesCommitter(tableLoader, hadoopConf);

      DataStream<Void> returnStream = inputStream
          .transform(ICEBERG_STREAM_WRITER_NAME, DATA_FILE_TYPE_INFO, streamWriter)
          .setParallelism(inputStream.getParallelism())
          .transform(ICEBERG_FILES_COMMITTER_NAME, Types.VOID, filesCommitter)
          .setParallelism(1)
          .setMaxParallelism(1);

      return returnStream.addSink(new DiscardingSink())
          .name(String.format("IcebergSink %s", tableIdentifier.toString()))
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
