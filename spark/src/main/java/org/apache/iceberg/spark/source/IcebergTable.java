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

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtils;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.CheckCompatibility;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

final class IcebergTable implements Table, SupportsRead, SupportsWrite {

  private static final Set<TableCapability> CAPABILITIES = ImmutableSet.of(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.MICRO_BATCH_READ,
      TableCapability.STREAMING_WRITE,
      TableCapability.TRUNCATE,
      TableCapability.OVERWRITE_DYNAMIC);

  private final org.apache.iceberg.Table tableInIceberg;
  private StructType requestSchema;

  IcebergTable(org.apache.iceberg.Table tableInIceberg, StructType requestSchema) {
    this.tableInIceberg = tableInIceberg;

    if (requestSchema != null) {
      SparkSchemaUtil.convert(tableInIceberg.schema(), requestSchema);
      this.requestSchema = requestSchema;
    }
  }

  @Override
  public String name() {
    return tableInIceberg.name();
  }

  @Override
  public StructType schema() {
    if (requestSchema != null) {
      return requestSchema;
    }
    return SparkSchemaUtil.convert(tableInIceberg.schema());
  }

  @Override
  public Transform[] partitioning() {
    return SparkUtils.toTransforms(tableInIceberg.spec());
  }

  @Override
  public Map<String, String> properties() {
    return tableInIceberg.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return () -> new IcebergBatchScan(tableInIceberg, options, requestSchema);
  }

  @Override
  public WriteBuilder newWriteBuilder(CaseInsensitiveStringMap options) {
    return new IcebergWriteBuilder(tableInIceberg, options);
  }

  static class IcebergWriteBuilder implements WriteBuilder, SupportsDynamicOverwrite, SupportsTruncate {

    private org.apache.iceberg.Table table;
    private CaseInsensitiveStringMap writeOptions;
    private TableCapability writeBehavior = TableCapability.BATCH_WRITE;
    private String writeQueryId = null;
    private StructType dsStruct = null;

    IcebergWriteBuilder(org.apache.iceberg.Table table, CaseInsensitiveStringMap options) {
      this.table = table;
      this.writeOptions = options;
    }

    @Override
    public WriteBuilder withQueryId(String queryId) {
      this.writeQueryId = queryId;
      return this;
    }

    @Override
    public WriteBuilder withInputDataSchema(StructType schemaInput) {
      this.dsStruct = schemaInput;
      return this;
    }

    @Override
    public WriteBuilder overwriteDynamicPartitions() {
      this.writeBehavior = TableCapability.OVERWRITE_DYNAMIC;
      return this;
    }

    @Override
    public WriteBuilder truncate() {
      this.writeBehavior = TableCapability.TRUNCATE;
      return this;
    }

    @Override
    public BatchWrite buildForBatch() {
      // TODO. Check queryId and schema before build?

      // Validate
      Schema dsSchema = SparkSchemaUtil.convert(table.schema(), dsStruct);
      validateWriteSchema(table.schema(), dsSchema, checkNullability(writeOptions));
      validatePartitionTransforms(table.spec());

      // Get application id
      String appId = SparkUtils.getSparkSession().sparkContext().applicationId();

      // Get write-audit-publish id
      String wapId = SparkUtils.getSparkSession().conf().get("spark.wap.id", null);

      return new IcebergBatchWriter(table, writeOptions, writeBehavior, appId, wapId, dsSchema);
    }

    @Override
    public StreamingWrite buildForStreaming() {
      // TODO. Check queryId and schema before build?

      // Validate
      Schema dsSchema = SparkSchemaUtil.convert(table.schema(), dsStruct);
      validateWriteSchema(table.schema(), dsSchema, checkNullability(writeOptions));
      validatePartitionTransforms(table.spec());

      // Change to streaming write if it is just append
      if (writeBehavior.equals(TableCapability.BATCH_WRITE)) {
        writeBehavior = TableCapability.STREAMING_WRITE;
      }

      // Get application id
      String appId = SparkUtils.getSparkSession().sparkContext().applicationId();
      String wapId = SparkUtils.getSparkSession().conf().get("spark.wap.id", null);
      return new IcebergStreamingWriter(table, writeOptions, writeQueryId, writeBehavior, appId, wapId, table.schema());
    }

    private void validateWriteSchema(Schema tableSchema, Schema dsSchema, Boolean checkNullability) {
      List<String> errors;
      if (checkNullability) {
        errors = CheckCompatibility.writeCompatibilityErrors(tableSchema, dsSchema);
      } else {
        errors = CheckCompatibility.typeCompatibilityErrors(tableSchema, dsSchema);
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
            .map(org.apache.iceberg.transforms.Transform::toString)
            .collect(Collectors.joining(", "));

        throw new UnsupportedOperationException(
          String.format("Cannot write using unsupported transforms: %s", unsupported));
      }
    }

    private boolean checkNullability(CaseInsensitiveStringMap options) {
      boolean sparkCheckNullability = Boolean.parseBoolean(SparkUtils.getSparkSession().conf()
          .get("spark.sql.iceberg.check-nullability", "true"));
      boolean dataFrameCheckNullability = options.getBoolean("check-nullability", true);
      return sparkCheckNullability && dataFrameCheckNullability;
    }
  }
}
