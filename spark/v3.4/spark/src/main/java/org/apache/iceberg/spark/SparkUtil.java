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
package org.apache.iceberg.spark;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.util.Pair;
import org.apache.spark.SparkEnv;
import org.apache.spark.scheduler.ExecutorCacheTaskLocation;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.BlockManagerMaster;
import org.joda.time.DateTime;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class SparkUtil {
  private static final String SPARK_CATALOG_CONF_PREFIX = "spark.sql.catalog";
  // Format string used as the prefix for Spark configuration keys to override Hadoop configuration
  // values for Iceberg tables from a given catalog. These keys can be specified as
  // `spark.sql.catalog.$catalogName.hadoop.*`, similar to using `spark.hadoop.*` to override
  // Hadoop configurations globally for a given Spark session.
  private static final String SPARK_CATALOG_HADOOP_CONF_OVERRIDE_FMT_STR =
      SPARK_CATALOG_CONF_PREFIX + ".%s.hadoop.";

  private static final Joiner DOT = Joiner.on(".");

  private SparkUtil() {}

  /**
   * Check whether the partition transforms in a spec can be used to write data.
   *
   * @param spec a PartitionSpec
   * @throws UnsupportedOperationException if the spec contains unknown partition transforms
   */
  public static void validatePartitionTransforms(PartitionSpec spec) {
    if (spec.fields().stream().anyMatch(field -> field.transform() instanceof UnknownTransform)) {
      String unsupported =
          spec.fields().stream()
              .map(PartitionField::transform)
              .filter(transform -> transform instanceof UnknownTransform)
              .map(Transform::toString)
              .collect(Collectors.joining(", "));

      throw new UnsupportedOperationException(
          String.format("Cannot write using unsupported transforms: %s", unsupported));
    }
  }

  /**
   * A modified version of Spark's LookupCatalog.CatalogAndIdentifier.unapply Attempts to find the
   * catalog and identifier a multipart identifier represents
   *
   * @param nameParts Multipart identifier representing a table
   * @return The CatalogPlugin and Identifier for the table
   */
  public static <C, T> Pair<C, T> catalogAndIdentifier(
      List<String> nameParts,
      Function<String, C> catalogProvider,
      BiFunction<String[], String, T> identiferProvider,
      C currentCatalog,
      String[] currentNamespace) {
    Preconditions.checkArgument(
        !nameParts.isEmpty(), "Cannot determine catalog and identifier from empty name");

    int lastElementIndex = nameParts.size() - 1;
    String name = nameParts.get(lastElementIndex);

    if (nameParts.size() == 1) {
      // Only a single element, use current catalog and namespace
      return Pair.of(currentCatalog, identiferProvider.apply(currentNamespace, name));
    } else {
      C catalog = catalogProvider.apply(nameParts.get(0));
      if (catalog == null) {
        // The first element was not a valid catalog, treat it like part of the namespace
        String[] namespace = nameParts.subList(0, lastElementIndex).toArray(new String[0]);
        return Pair.of(currentCatalog, identiferProvider.apply(namespace, name));
      } else {
        // Assume the first element is a valid catalog
        String[] namespace = nameParts.subList(1, lastElementIndex).toArray(new String[0]);
        return Pair.of(catalog, identiferProvider.apply(namespace, name));
      }
    }
  }

  /**
   * Pulls any Catalog specific overrides for the Hadoop conf from the current SparkSession, which
   * can be set via `spark.sql.catalog.$catalogName.hadoop.*`
   *
   * <p>Mirrors the override of hadoop configurations for a given spark session using
   * `spark.hadoop.*`.
   *
   * <p>The SparkCatalog allows for hadoop configurations to be overridden per catalog, by setting
   * them on the SQLConf, where the following will add the property "fs.default.name" with value
   * "hdfs://hanksnamenode:8020" to the catalog's hadoop configuration. SparkSession.builder()
   * .config(s"spark.sql.catalog.$catalogName.hadoop.fs.default.name", "hdfs://hanksnamenode:8020")
   * .getOrCreate()
   *
   * @param spark The current Spark session
   * @param catalogName Name of the catalog to find overrides for.
   * @return the Hadoop Configuration that should be used for this catalog, with catalog specific
   *     overrides applied.
   */
  public static Configuration hadoopConfCatalogOverrides(SparkSession spark, String catalogName) {
    // Find keys for the catalog intended to be hadoop configurations
    final String hadoopConfCatalogPrefix = hadoopConfPrefixForCatalog(catalogName);
    final Configuration conf = spark.sessionState().newHadoopConf();
    spark
        .sqlContext()
        .conf()
        .settings()
        .forEach(
            (k, v) -> {
              // these checks are copied from `spark.sessionState().newHadoopConfWithOptions()`
              // to avoid converting back and forth between Scala / Java map types
              if (v != null && k != null && k.startsWith(hadoopConfCatalogPrefix)) {
                conf.set(k.substring(hadoopConfCatalogPrefix.length()), v);
              }
            });
    return conf;
  }

  private static String hadoopConfPrefixForCatalog(String catalogName) {
    return String.format(SPARK_CATALOG_HADOOP_CONF_OVERRIDE_FMT_STR, catalogName);
  }

  public static void validateTimestampWithoutTimezoneConfig(RuntimeConfig conf) {
    validateTimestampWithoutTimezoneConfig(conf, ImmutableMap.of());
  }

  /**
   * Checks for properties both supplied by Spark's RuntimeConfig and the read or write options
   *
   * @param conf The RuntimeConfig of the active Spark session
   * @param options The read or write options supplied when reading/writing a table
   */
  public static void validateTimestampWithoutTimezoneConfig(
      RuntimeConfig conf, Map<String, String> options) {
    if (conf.contains(SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE)) {
      throw new UnsupportedOperationException(
          "Spark configuration "
              + SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE
              + " is not supported in Spark 3.4 due to the introduction of native support for timestamp without timezone.");
    }

    if (options.containsKey(SparkReadOptions.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE)) {
      throw new UnsupportedOperationException(
          "Option "
              + SparkReadOptions.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE
              + " is not supported in Spark 3.4 due to the introduction of native support for timestamp without timezone.");
    }

    if (conf.contains(SparkSQLProperties.USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES)) {
      throw new UnsupportedOperationException(
          "Spark configuration "
              + SparkSQLProperties.USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES
              + " is not supported in Spark 3.4 due to the introduction of native support for timestamp without timezone.");
    }
  }

  /**
   * Get a List of Spark filter Expression.
   *
   * @param schema table schema
   * @param filters filters in the format of a Map, where key is one of the table column name, and
   *     value is the specific value to be filtered on the column.
   * @return a List of filters in the format of Spark Expression.
   */
  public static List<Expression> partitionMapToExpression(
      StructType schema, Map<String, String> filters) {
    List<Expression> filterExpressions = Lists.newArrayList();
    for (Map.Entry<String, String> entry : filters.entrySet()) {
      try {
        int index = schema.fieldIndex(entry.getKey());
        DataType dataType = schema.fields()[index].dataType();
        BoundReference ref = new BoundReference(index, dataType, true);
        switch (dataType.typeName()) {
          case "integer":
            filterExpressions.add(
                new EqualTo(
                    ref,
                    Literal.create(Integer.parseInt(entry.getValue()), DataTypes.IntegerType)));
            break;
          case "string":
            filterExpressions.add(
                new EqualTo(ref, Literal.create(entry.getValue(), DataTypes.StringType)));
            break;
          case "short":
            filterExpressions.add(
                new EqualTo(
                    ref, Literal.create(Short.parseShort(entry.getValue()), DataTypes.ShortType)));
            break;
          case "long":
            filterExpressions.add(
                new EqualTo(
                    ref, Literal.create(Long.parseLong(entry.getValue()), DataTypes.LongType)));
            break;
          case "float":
            filterExpressions.add(
                new EqualTo(
                    ref, Literal.create(Float.parseFloat(entry.getValue()), DataTypes.FloatType)));
            break;
          case "double":
            filterExpressions.add(
                new EqualTo(
                    ref,
                    Literal.create(Double.parseDouble(entry.getValue()), DataTypes.DoubleType)));
            break;
          case "date":
            filterExpressions.add(
                new EqualTo(
                    ref,
                    Literal.create(
                        new Date(DateTime.parse(entry.getValue()).getMillis()),
                        DataTypes.DateType)));
            break;
          case "timestamp":
            filterExpressions.add(
                new EqualTo(
                    ref,
                    Literal.create(
                        new Timestamp(DateTime.parse(entry.getValue()).getMillis()),
                        DataTypes.TimestampType)));
            break;
          default:
            throw new IllegalStateException(
                "Unexpected data type in partition filters: " + dataType);
        }
      } catch (IllegalArgumentException e) {
        // ignore if filter is not on table columns
      }
    }

    return filterExpressions;
  }

  public static String toColumnName(NamedReference ref) {
    return DOT.join(ref.fieldNames());
  }

  public static boolean caseSensitive(SparkSession spark) {
    return Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive"));
  }

  public static List<String> executorLocations() {
    BlockManager driverBlockManager = SparkEnv.get().blockManager();
    List<BlockManagerId> executorBlockManagerIds = fetchPeers(driverBlockManager);
    return executorBlockManagerIds.stream()
        .map(SparkUtil::toExecutorLocation)
        .sorted()
        .collect(Collectors.toList());
  }

  private static List<BlockManagerId> fetchPeers(BlockManager blockManager) {
    BlockManagerMaster master = blockManager.master();
    BlockManagerId id = blockManager.blockManagerId();
    return toJavaList(master.getPeers(id));
  }

  private static <T> List<T> toJavaList(Seq<T> seq) {
    return JavaConverters.seqAsJavaListConverter(seq).asJava();
  }

  private static String toExecutorLocation(BlockManagerId id) {
    return ExecutorCacheTaskLocation.apply(id.host(), id.executorId()).toString();
  }
}
