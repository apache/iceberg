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

import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewOperations;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkView implements org.apache.spark.sql.connector.catalog.View {

  private static final Logger LOG = LoggerFactory.getLogger(SparkView.class);

  private static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of("provider", "location", FORMAT_VERSION);

  private final View icebergView;
  private SparkSession lazySpark = null;

  public SparkView(View icebergView) {
    this.icebergView = icebergView;
  }

  private SparkSession sparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }

    return lazySpark;
  }

  public View view() {
    return icebergView;
  }

  @Override
  public String name() {
    return icebergView.toString();
  }

  @Override
  public String query() {
    Optional<SQLViewRepresentation> query =
        icebergView.currentVersion().representations().stream()
            .filter(SQLViewRepresentation.class::isInstance)
            .map(SQLViewRepresentation.class::cast)
            .filter(r -> r.dialect().equalsIgnoreCase("spark"))
            .findFirst();

    return query
        .orElseThrow(() -> new IllegalStateException("No SQL query found for view " + name()))
        .sql();
  }

  @Override
  public String currentCatalog() {
    return icebergView.currentVersion().defaultCatalog();
  }

  @Override
  public String[] currentNamespace() {
    return icebergView.currentVersion().defaultNamespace().levels();
  }

  @Override
  public StructType schema() {
    return SparkSchemaUtil.convert(icebergView.schema());
  }

  @Override
  public String[] queryColumnNames() {
    return new String[0];
  }

  @Override
  public String[] columnAliases() {
    return new String[0];
  }

  @Override
  public String[] columnComments() {
    return new String[0];
  }

  @Override
  public Map<String, String> properties() {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();

    propsBuilder.put("provider", "iceberg");
    propsBuilder.put("location", icebergView.location());

    if (icebergView instanceof BaseView) {
      ViewOperations ops = ((BaseView) icebergView).operations();
      propsBuilder.put(FORMAT_VERSION, String.valueOf(ops.current().formatVersion()));
    }

    icebergView.properties().entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(propsBuilder::put);

    return propsBuilder.build();
  }

  @Override
  public String toString() {
    return icebergView.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    // use only name in order to correctly invalidate Spark cache
    SparkView that = (SparkView) other;
    return icebergView.name().equals(that.icebergView.name());
  }

  @Override
  public int hashCode() {
    // use only name in order to correctly invalidate Spark cache
    return icebergView.name().hashCode();
  }
}
