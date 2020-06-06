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

import java.io.File;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;

public final class IcebergMRConfig {

  private static final String AS_OF_TIMESTAMP = "iceberg.mr.as.of.time";
  private static final String CASE_SENSITIVE = "iceberg.mr.case.sensitive";
  private static final String CATALOG = "iceberg.mr.catalog";
  private static final String FILTER_EXPRESSION = "iceberg.mr.filter.expression";
  private static final String IN_MEMORY_DATA_MODEL = "iceberg.mr.in.memory.data.model";
  private static final String LOCALITY = "iceberg.mr.locality";
  private static final String READ_SCHEMA = "iceberg.mr.read.schema";
  private static final String REUSE_CONTAINERS = "iceberg.mr.reuse.containers";
  private static final String SKIP_RESIDUAL_FILTERING = "skip.residual.filtering";
  private static final String SNAPSHOT_ID = "iceberg.mr.snapshot.id";
  private static final String SPLIT_SIZE = "iceberg.mr.split.size";
  private static final String TABLE_PATH = "iceberg.mr.table.path";
  private static final String TABLE_SCHEMA = "iceberg.mr.table.schema";

  private IcebergMRConfig() {
  }

  public static class Builder {

    private final Configuration conf;

    private Builder() {
      this(new Configuration());
    }

    private Builder(Configuration conf) {
      this.conf = conf;
    }

    public Configuration build() {
      Table table = IcebergInputFormat.findTable(conf);
      schema(table.schema());
      return conf;
    }

    public static Builder newInstance() {
      return new Builder();
    }

    public static Builder newInstance(Configuration conf) {
      return new Builder(conf);
    }

    public Builder copy() {
      return newInstance(new Configuration(conf));
    }

    public Builder asOfTime(long asOfTime) {
      conf.setLong(AS_OF_TIMESTAMP, asOfTime);
      return this;
    }

    public Builder caseSensitive(boolean caseSensitive) {
      conf.setBoolean(CASE_SENSITIVE, caseSensitive);
      return this;
    }

    public Builder catalogLoader(Class<? extends Function<Configuration, Catalog>> loader) {
      conf.setClass(CATALOG, loader, Function.class);
      return this;
    }

    public Builder filter(Expression expression) {
      conf.set(FILTER_EXPRESSION, SerializationUtil.serializeToBase64(expression));
      return this;
    }

    /**
     * If this API is called. The input splits
     * constructed will have host location information
     */
    public Builder preferLocality() {
      conf.setBoolean(LOCALITY, true);
      return this;
    }

    public Builder project(Schema schema) {
      conf.set(READ_SCHEMA, SchemaParser.toJson(schema));
      return this;
    }

    public Builder readFrom(File path) {
      return readFrom(path.toString());
    }

    public Builder readFrom(TableIdentifier identifier) {
      return readFrom(identifier.toString());
    }

    public Builder readFrom(String path) {
      conf.set(TABLE_PATH, path);
      return this;
    }

    public Builder schema(Schema schema) {
      conf.set(TABLE_SCHEMA, SchemaParser.toJson(schema));
      return this;
    }

    /**
     * Compute platforms pass down filters to data sources. If the data source cannot apply some filters, or only
     * partially applies the filter, it will return the residual filter back. If the platform can correctly apply
     * the residual filters, then it should call this api. Otherwise the current api will throw an exception if the
     * passed in filter is not completely satisfied.
     */
    public Builder skipResidualFiltering() {
      conf.setBoolean(SKIP_RESIDUAL_FILTERING, true);
      return this;
    }

    public Builder reuseContainers(boolean reuse) {
      conf.setBoolean(REUSE_CONTAINERS, reuse);
      return this;
    }

    public Builder snapshotId(long snapshotId) {
      conf.setLong(SNAPSHOT_ID, snapshotId);
      return this;
    }

    public Builder splitSize(long splitSize) {
      conf.setLong(SPLIT_SIZE, splitSize);
      return this;
    }

    public Builder useHiveRows() {
      conf.setEnum(IN_MEMORY_DATA_MODEL, InMemoryDataModel.HIVE);
      return this;
    }

    public Builder usePigTuples() {
      conf.setEnum(IN_MEMORY_DATA_MODEL, InMemoryDataModel.PIG);
      return this;
    }

  }

  public static long asOfTime(Configuration conf) {
    return conf.getLong(AS_OF_TIMESTAMP, -1);
  }

  public static boolean caseSensitive(Configuration conf) {
    return conf.getBoolean(CASE_SENSITIVE, true);
  }

  public static String catalogLoader(Configuration conf) {
    return conf.get(CATALOG);
  }

  public static Expression filter(Configuration conf) {
    // TODO add a filter parser to get rid of Serialization
    return SerializationUtil.deserializeFromBase64(conf.get(FILTER_EXPRESSION));
  }

  public static Schema projection(Configuration conf) {
    return parseSchemaFromJson(conf.get(READ_SCHEMA));
  }

  public static String readFrom(Configuration conf) {
    return conf.get(TABLE_PATH);
  }

  public static boolean reuseContainers(Configuration conf) {
    return conf.getBoolean(REUSE_CONTAINERS, false);
  }

  public static Schema schema(Configuration conf) {
    return parseSchemaFromJson(conf.get(TABLE_SCHEMA));
  }

  public static boolean applyResidualFiltering(Configuration conf) {
    return !conf.getBoolean(SKIP_RESIDUAL_FILTERING, false);
  }

  public static long snapshotId(Configuration conf) {
    return conf.getLong(SNAPSHOT_ID, -1);
  }

  public static InMemoryDataModel inMemoryDataModel(Configuration conf) {
    return conf.getEnum(IN_MEMORY_DATA_MODEL, InMemoryDataModel.defaultModel());
  }

  public static long splitSize(Configuration conf) {
    return conf.getLong(SPLIT_SIZE, 0);
  }

  public static boolean localityPreferred(Configuration conf) {
    return conf.getBoolean(LOCALITY, false);
  }

  private static Schema parseSchemaFromJson(@Nullable String schema) {
    return Optional.ofNullable(schema)
            .map(SchemaParser::fromJson)
            .orElse(null);
  }

}
