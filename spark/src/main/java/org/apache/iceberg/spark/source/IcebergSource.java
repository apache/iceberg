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

package org.apache.iceberg.spark.source;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.CheckCompatibility;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.StreamExecution;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.StreamWriteSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

public class IcebergSource implements DataSourceV2, ReadSupport, WriteSupport, DataSourceRegister, StreamWriteSupport {

  private SparkSession lazySpark = null;
  private JavaSparkContext lazySparkContext = null;
  private Configuration lazyConf = null;

  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return createReader(null, options);
  }

  @Override
  public DataSourceReader createReader(StructType readSchema, DataSourceOptions options) {
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    String caseSensitive = lazySparkSession().conf().get("spark.sql.caseSensitive");

    Broadcast<FileIO> io = lazySparkContext().broadcast(fileIO(table));
    Broadcast<EncryptionManager> encryptionManager = lazySparkContext().broadcast(table.encryption());

    Reader reader = new Reader(table, io, encryptionManager, Boolean.parseBoolean(caseSensitive), options);
    if (readSchema != null) {
      // convert() will fail if readSchema contains fields not in table.schema()
      SparkSchemaUtil.convert(table.schema(), readSchema);
      reader.pruneColumns(readSchema);
    }

    return reader;
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String jobId, StructType dsStruct, SaveMode mode,
                                                 DataSourceOptions options) {
    Preconditions.checkArgument(mode == SaveMode.Append || mode == SaveMode.Overwrite,
        "Save mode %s is not supported", mode);
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), dsStruct);
    validateWriteSchema(table.schema(), writeSchema, checkNullability(options), checkOrdering(options));
    validatePartitionTransforms(table.spec());
    String appId = lazySparkSession().sparkContext().applicationId();
    String wapId = lazySparkSession().conf().get("spark.wap.id", null);
    boolean replacePartitions = mode == SaveMode.Overwrite;

    Broadcast<FileIO> io = lazySparkContext().broadcast(fileIO(table));
    Broadcast<EncryptionManager> encryptionManager = lazySparkContext().broadcast(table.encryption());

    return Optional.of(new Writer(
        table, io, encryptionManager, options, replacePartitions, appId, wapId, writeSchema, dsStruct));
  }

  @Override
  public StreamWriter createStreamWriter(String runId, StructType dsStruct,
                                         OutputMode mode, DataSourceOptions options) {
    Preconditions.checkArgument(
        mode == OutputMode.Append() || mode == OutputMode.Complete(),
        "Output mode %s is not supported", mode);
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), dsStruct);
    validateWriteSchema(table.schema(), writeSchema, checkNullability(options), checkOrdering(options));
    validatePartitionTransforms(table.spec());
    // Spark 2.4.x passes runId to createStreamWriter instead of real queryId,
    // so we fetch it directly from sparkContext to make writes idempotent
    String queryId = lazySparkSession().sparkContext().getLocalProperty(StreamExecution.QUERY_ID_KEY());
    String appId = lazySparkSession().sparkContext().applicationId();

    Broadcast<FileIO> io = lazySparkContext().broadcast(fileIO(table));
    Broadcast<EncryptionManager> encryptionManager = lazySparkContext().broadcast(table.encryption());

    return new StreamingWriter(table, io, encryptionManager, options, queryId, mode, appId, writeSchema, dsStruct);
  }

  protected Table findTable(DataSourceOptions options, Configuration conf) {
    Optional<String> path = options.get("path");
    Preconditions.checkArgument(path.isPresent(), "Cannot open table: path is not set");

    if (path.get().contains("/")) {
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path.get());
    } else {
      HiveCatalog hiveCatalog = HiveCatalogs.loadCatalog(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path.get());
      return hiveCatalog.loadTable(tableIdentifier);
    }
  }

  private SparkSession lazySparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.builder().getOrCreate();
    }
    return lazySpark;
  }

  private JavaSparkContext lazySparkContext() {
    if (lazySparkContext == null) {
      this.lazySparkContext = new JavaSparkContext(lazySparkSession().sparkContext());
    }
    return lazySparkContext;
  }

  private Configuration lazyBaseConf() {
    if (lazyConf == null) {
      this.lazyConf = lazySparkSession().sessionState().newHadoopConf();
    }
    return lazyConf;
  }

  private Table getTableAndResolveHadoopConfiguration(
      DataSourceOptions options, Configuration conf) {
    // Overwrite configurations from the Spark Context with configurations from the options.
    mergeIcebergHadoopConfs(conf, options.asMap());
    Table table = findTable(options, conf);
    // Set confs from table properties
    mergeIcebergHadoopConfs(conf, table.properties());
    // Re-overwrite values set in options and table properties but were not in the environment.
    mergeIcebergHadoopConfs(conf, options.asMap());
    return table;
  }

  private static void mergeIcebergHadoopConfs(
      Configuration baseConf, Map<String, String> options) {
    options.keySet().stream()
        .filter(key -> key.startsWith("hadoop."))
        .forEach(key -> baseConf.set(key.replaceFirst("hadoop.", ""), options.get(key)));
  }

  private void validateWriteSchema(
          Schema tableSchema, Schema dsSchema, Boolean checkNullability, Boolean checkOrdering) {
    List<String> errors;
    if (checkNullability) {
      errors = CheckCompatibility.writeCompatibilityErrors(tableSchema, dsSchema, checkOrdering);
    } else {
      errors = CheckCompatibility.typeCompatibilityErrors(tableSchema, dsSchema, checkOrdering);
    }
    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Cannot write incompatible dataset to table with schema:\n")
          .append(tableSchema)
          .append("\nProblems:");
      for (String error : errors) {
        sb.append("\n* ").append(error);
      }
      throw new IllegalArgumentException(sb.toString());
    }
  }

  private void validatePartitionTransforms(PartitionSpec spec) {
    if (spec.fields().stream().anyMatch(field -> field.transform() instanceof UnknownTransform)) {
      String unsupported = spec.fields().stream()
          .map(PartitionField::transform)
          .filter(transform -> transform instanceof UnknownTransform)
          .map(Transform::toString)
          .collect(Collectors.joining(", "));

      throw new UnsupportedOperationException(
          String.format("Cannot write using unsupported transforms: %s", unsupported));
    }
  }

  private boolean checkNullability(DataSourceOptions options) {
    boolean sparkCheckNullability = Boolean.parseBoolean(lazySpark.conf()
        .get("spark.sql.iceberg.check-nullability", "true"));
    boolean dataFrameCheckNullability = options.getBoolean("check-nullability", true);
    return sparkCheckNullability && dataFrameCheckNullability;
  }

  private boolean checkOrdering(DataSourceOptions options) {
    boolean sparkCheckOrdering = Boolean.parseBoolean(lazySpark.conf()
            .get("spark.sql.iceberg.check-ordering", "true"));
    boolean dataFrameCheckOrdering = options.getBoolean("check-ordering", true);
    return sparkCheckOrdering && dataFrameCheckOrdering;
  }

  private FileIO fileIO(Table table) {
    if (table.io() instanceof HadoopFileIO) {
      // we need to use Spark's SerializableConfiguration to avoid issues with Kryo serialization
      SerializableConfiguration conf = new SerializableConfiguration(((HadoopFileIO) table.io()).conf());
      return new HadoopFileIO(conf::value);
    } else {
      return table.io();
    }
  }
}
