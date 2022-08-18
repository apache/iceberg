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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public abstract class IcebergSourceBenchmark {

  private final Configuration hadoopConf = initHadoopConf();
  private final Table table = initTable();
  private SparkSession spark;

  protected abstract Configuration initHadoopConf();

  protected final Configuration hadoopConf() {
    return hadoopConf;
  }

  protected abstract Table initTable();

  protected final Table table() {
    return table;
  }

  protected final SparkSession spark() {
    return spark;
  }

  protected String newTableLocation() {
    String tmpDir = hadoopConf.get("hadoop.tmp.dir");
    Path tablePath = new Path(tmpDir, "spark-iceberg-table-" + UUID.randomUUID());
    return tablePath.toString();
  }

  protected String dataLocation() {
    Map<String, String> properties = table.properties();
    return properties.getOrDefault(
        TableProperties.WRITE_DATA_LOCATION, String.format("%s/data", table.location()));
  }

  protected void cleanupFiles() throws IOException {
    try (FileSystem fileSystem = FileSystem.get(hadoopConf)) {
      Path dataPath = new Path(dataLocation());
      fileSystem.delete(dataPath, true);
      Path tablePath = new Path(table.location());
      fileSystem.delete(tablePath, true);
    }
  }

  protected void setupSpark(boolean enableDictionaryEncoding) {
    SparkSession.Builder builder = SparkSession.builder().config("spark.ui.enabled", false);
    if (!enableDictionaryEncoding) {
      builder
          .config("parquet.dictionary.page.size", "1")
          .config("parquet.enable.dictionary", false)
          .config(TableProperties.PARQUET_DICT_SIZE_BYTES, "1");
    }
    builder.master("local");
    spark = builder.getOrCreate();
    Configuration sparkHadoopConf = spark.sessionState().newHadoopConf();
    hadoopConf.forEach(entry -> sparkHadoopConf.set(entry.getKey(), entry.getValue()));
  }

  protected void setupSpark() {
    setupSpark(false);
  }

  protected void tearDownSpark() {
    spark.stop();
  }

  protected void materialize(Dataset<?> ds) {
    ds.queryExecution().toRdd().toJavaRDD().foreach(record -> {});
  }

  protected void appendAsFile(Dataset<Row> ds) {
    // ensure the schema is precise (including nullability)
    StructType sparkSchema = SparkSchemaUtil.convert(table.schema());
    spark
        .createDataFrame(ds.rdd(), sparkSchema)
        .coalesce(1)
        .write()
        .format("iceberg")
        .mode(SaveMode.Append)
        .save(table.location());
  }

  protected void withSQLConf(Map<String, String> conf, Action action) {
    SQLConf sqlConf = SQLConf.get();

    Map<String, String> currentConfValues = Maps.newHashMap();
    conf.keySet()
        .forEach(
            confKey -> {
              if (sqlConf.contains(confKey)) {
                String currentConfValue = sqlConf.getConfString(confKey);
                currentConfValues.put(confKey, currentConfValue);
              }
            });

    conf.forEach(
        (confKey, confValue) -> {
          if (SQLConf.staticConfKeys().contains(confKey)) {
            throw new RuntimeException("Cannot modify the value of a static config: " + confKey);
          }
          sqlConf.setConfString(confKey, confValue);
        });

    try {
      action.invoke();
    } finally {
      conf.forEach(
          (confKey, confValue) -> {
            if (currentConfValues.containsKey(confKey)) {
              sqlConf.setConfString(confKey, currentConfValues.get(confKey));
            } else {
              sqlConf.unsetConf(confKey);
            }
          });
    }
  }

  protected void withTableProperties(Map<String, String> props, Action action) {
    Map<String, String> tableProps = table.properties();
    Map<String, String> currentPropValues = Maps.newHashMap();
    props
        .keySet()
        .forEach(
            propKey -> {
              if (tableProps.containsKey(propKey)) {
                String currentPropValue = tableProps.get(propKey);
                currentPropValues.put(propKey, currentPropValue);
              }
            });

    UpdateProperties updateProperties = table.updateProperties();
    props.forEach(updateProperties::set);
    updateProperties.commit();

    try {
      action.invoke();
    } finally {
      UpdateProperties restoreProperties = table.updateProperties();
      props.forEach(
          (propKey, propValue) -> {
            if (currentPropValues.containsKey(propKey)) {
              restoreProperties.set(propKey, currentPropValues.get(propKey));
            } else {
              restoreProperties.remove(propKey);
            }
          });
      restoreProperties.commit();
    }
  }

  protected FileFormat fileFormat() {
    throw new UnsupportedOperationException("Unsupported file format");
  }
}
