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
package org.apache.iceberg.hudi;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseSnapshotHudiTableAction implements SnapshotHudiTable {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseSnapshotHudiTableAction.class.getName());

  private HoodieTableMetaClient hoodieTableMetaClient;

  public BaseSnapshotHudiTableAction(
      Configuration hoodieConfiguration, String hoodieTableBasePath) {
    this.hoodieTableMetaClient = buildTableMetaClient(hoodieConfiguration, hoodieTableBasePath);
  }

  @Override
  public SnapshotHudiTable tableProperties(Map<String, String> properties) {
    return null;
  }

  @Override
  public SnapshotHudiTable tableProperty(String key, String value) {
    return null;
  }

  @Override
  public Result execute() {
    LOG.info("Alpha test: hoodie table base path: {}", hoodieTableMetaClient.getBasePathV2());
    LOG.info(
        "Alpha test: hoodie getBootStrapIndexByFileId: {}",
        hoodieTableMetaClient.getBootstrapIndexByFileIdFolderNameFolderPath());
    LOG.info(
        "Alpha test: hoodie getBootStrapIndexByPartitionPath: {}",
        hoodieTableMetaClient.getBootstrapIndexByPartitionFolderPath());
    InternalSchema hudiSchema = getHudiSchema();
    LOG.info("Alpha test: hoodie table schema: {}", hudiSchema);
    LOG.info("Alpha test: get record type: {}", hudiSchema.getRecord());
    Schema icebergSchema = getIcebergSchema(hudiSchema);
    LOG.info("Alpha test: get converted schema: {}", icebergSchema);
    return null;
  }

  private InternalSchema getHudiSchema() {
    TableSchemaResolver schemaUtil = new TableSchemaResolver(hoodieTableMetaClient);
    Option<InternalSchema> hudiSchema = schemaUtil.getTableInternalSchemaFromCommitMetadata();
    LOG.info("Alpha test: hoodie schema: {}", hudiSchema);
    LOG.info("Alpha test: active timeline: {}", hoodieTableMetaClient.getActiveTimeline());
    LOG.info(
        "Alpha test: active timeline commit timeline: {}",
        hoodieTableMetaClient.getActiveTimeline().getCommitsTimeline());
    LOG.info(
        "Alpha test: active timeline commit timeline instants: {}",
        hoodieTableMetaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
    // TODO: need to add support for parquet format table
    return hudiSchema.orElseGet(
        () -> {
          try {
            return AvroInternalSchemaConverter.convert(schemaUtil.getTableAvroSchema());
          } catch (Exception e) {
            throw new HoodieException("cannot find schema for current table");
          }
        });
  }

  private Schema getIcebergSchema(InternalSchema hudiSchema) {
    Type converted =
        HudiDataTypeVisitor.visit(
            hudiSchema.getRecord(), new HudiDataTypeToType(hudiSchema.getRecord()));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  private static HoodieTableMetaClient buildTableMetaClient(Configuration conf, String basePath) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(conf)
            .setBasePath(basePath)
            .setLoadActiveTimelineOnLoad(true)
            .build();
    return metaClient;
  }
}
