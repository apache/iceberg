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

package org.apache.iceberg.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;

public class InputFormatConfig {

  private InputFormatConfig() {
  }

  // configuration values for Iceberg input formats
  public static final String REUSE_CONTAINERS = "iceberg.mr.reuse.containers";
  public static final String CASE_SENSITIVE = "iceberg.mr.case.sensitive";
  public static final String SKIP_RESIDUAL_FILTERING = "skip.residual.filtering";
  public static final String AS_OF_TIMESTAMP = "iceberg.mr.as.of.time";
  public static final String FILTER_EXPRESSION = "iceberg.mr.filter.expression";
  public static final String IN_MEMORY_DATA_MODEL = "iceberg.mr.in.memory.data.model";
  public static final String READ_SCHEMA = "iceberg.mr.read.schema";
  public static final String SNAPSHOT_ID = "iceberg.mr.snapshot.id";
  public static final String SPLIT_SIZE = "iceberg.mr.split.size";
  public static final String TABLE_IDENTIFIER = "iceberg.mr.table.identifier";
  public static final String TABLE_LOCATION = "iceberg.mr.table.location";
  public static final String TABLE_SCHEMA = "iceberg.mr.table.schema";
  public static final String PARTITION_SPEC = "iceberg.mr.table.partition.spec";
  public static final String LOCALITY = "iceberg.mr.locality";
  public static final String CATALOG = "iceberg.mr.catalog";
  public static final String HADOOP_CATALOG_WAREHOUSE_LOCATION = "iceberg.mr.catalog.hadoop.warehouse.location";
  public static final String CATALOG_LOADER_CLASS = "iceberg.mr.catalog.loader.class";
  public static final String EXTERNAL_TABLE_PURGE = "external.table.purge";

  // TODO: Find a better place for the OutputFormat configuration, or rename the class
  public static final String WRITE_FILE_FORMAT = "iceberg.mr.write.file.format";
  public static final FileFormat WRITE_FILE_FORMAT_DEFAULT = FileFormat.PARQUET;
  public static final String COMMIT_THREAD_POOL_SIZE = "iceberg.mr.commit.thread.pool.size";
  public static final int COMMIT_THREAD_POOL_SIZE_DEFAULT = 10;

  public static final String CATALOG_NAME = "iceberg.catalog";
  public static final String HADOOP_CATALOG = "hadoop.catalog";
  public static final String HADOOP_TABLES = "hadoop.tables";
  public static final String HIVE_CATALOG = "hive.catalog";
  public static final String ICEBERG_SNAPSHOTS_TABLE_SUFFIX = ".snapshots";
  public static final String SNAPSHOT_TABLE = "iceberg.snapshots.table";
  public static final String SNAPSHOT_TABLE_SUFFIX = "__snapshots";

  public enum InMemoryDataModel {
    PIG,
    HIVE,
    GENERIC // Default data model is of Iceberg Generics
  }

  public static class ConfigBuilder {
    private final Configuration conf;

    public ConfigBuilder(Configuration conf) {
      this.conf = conf;
      // defaults
      conf.setBoolean(SKIP_RESIDUAL_FILTERING, false);
      conf.setBoolean(CASE_SENSITIVE, true);
      conf.setBoolean(REUSE_CONTAINERS, false);
      conf.setBoolean(LOCALITY, false);
    }

    public Configuration conf() {
      return conf;
    }

    public ConfigBuilder filter(Expression expression) {
      conf.set(FILTER_EXPRESSION, SerializationUtil.serializeToBase64(expression));
      return this;
    }

    public ConfigBuilder project(Schema schema) {
      conf.set(READ_SCHEMA, SchemaParser.toJson(schema));
      return this;
    }

    public ConfigBuilder schema(Schema schema) {
      conf.set(TABLE_SCHEMA, SchemaParser.toJson(schema));
      return this;
    }

    public ConfigBuilder readFrom(TableIdentifier identifier) {
      conf.set(TABLE_IDENTIFIER, identifier.toString());
      return this;
    }

    public ConfigBuilder readFrom(String location) {
      conf.set(TABLE_LOCATION, location);
      return this;
    }

    public ConfigBuilder reuseContainers(boolean reuse) {
      conf.setBoolean(InputFormatConfig.REUSE_CONTAINERS, reuse);
      return this;
    }

    public ConfigBuilder caseSensitive(boolean caseSensitive) {
      conf.setBoolean(InputFormatConfig.CASE_SENSITIVE, caseSensitive);
      return this;
    }

    public ConfigBuilder snapshotId(long snapshotId) {
      conf.setLong(SNAPSHOT_ID, snapshotId);
      return this;
    }

    public ConfigBuilder asOfTime(long asOfTime) {
      conf.setLong(AS_OF_TIMESTAMP, asOfTime);
      return this;
    }

    public ConfigBuilder splitSize(long splitSize) {
      conf.setLong(SPLIT_SIZE, splitSize);
      return this;
    }

    /**
     * If this API is called. The input splits constructed will have host location information
     */
    public ConfigBuilder preferLocality() {
      conf.setBoolean(LOCALITY, true);
      return this;
    }

    public ConfigBuilder catalogLoader(Class<? extends CatalogLoader> catalogLoader) {
      conf.setClass(CATALOG_LOADER_CLASS, catalogLoader, CatalogLoader.class);
      return this;
    }

    public ConfigBuilder useHiveRows() {
      conf.set(IN_MEMORY_DATA_MODEL, InMemoryDataModel.HIVE.name());
      return this;
    }

    public ConfigBuilder usePigTuples() {
      conf.set(IN_MEMORY_DATA_MODEL, InMemoryDataModel.PIG.name());
      return this;
    }

    /**
     * Compute platforms pass down filters to data sources. If the data source cannot apply some filters, or only
     * partially applies the filter, it will return the residual filter back. If the platform can correctly apply the
     * residual filters, then it should call this api. Otherwise the current api will throw an exception if the passed
     * in filter is not completely satisfied.
     */
    public ConfigBuilder skipResidualFiltering() {
      conf.setBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, true);
      return this;
    }
  }

}
