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

package org.apache.iceberg.spark

import com.google.common.collect.Maps
import java.nio.ByteBuffer
import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.iceberg.{DataFile, DataFiles, FileFormat, ManifestFile, ManifestWriter}
import org.apache.iceberg.{Metrics, MetricsConfig, PartitionSpec, Table}
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.hadoop.{HadoopFileIO, HadoopInputFile, SerializableConfiguration}
import org.apache.iceberg.orc.OrcMetrics
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.Expression
import scala.collection.JavaConverters._
import scala.util.Try

object SparkTableUtil {
  /**
   * Returns a DataFrame with a row for each partition in the table.
   *
   * The DataFrame has 3 columns, partition key (a=1/b=2), partition location, and format
   * (avro or parquet).
   *
   * @param spark a Spark session
   * @param table a table name and (optional) database
   * @return a DataFrame of the table's partitions
   */
  def partitionDF(spark: SparkSession, table: String): DataFrame = {
    import spark.implicits._

    val partitions = getPartitions(spark, table)
    partitions.toDF("partition", "uri", "format")
  }

  /**
    * Returns a DataFrame with a row for each partition that matches the specified 'expression'.
    *
    * @param spark a Spark session.
    * @param table name of the table.
    * @param expression The expression whose matching partitions are returned.
    * @return a DataFrame of the table partitions.
    */
  def partitionDFByFilter(spark: SparkSession, table: String, expression: String): DataFrame = {
    import spark.implicits._

    val partitions = getPartitionsByFilter(spark, table, expression)
    partitions.toDF("partition", "uri", "format")
  }

  /**
   * Returns all partitions in the table.
   *
   * @param spark a Spark session
   * @param table a table name and (optional) database
   * @return all table's partitions
   */
  def getPartitions(spark: SparkSession, table: String): Seq[SparkPartition] = {
    val tableIdent = spark.sessionState.sqlParser.parseTableIdentifier(table)
    getPartitions(spark, tableIdent)
  }

  /**
   * Returns all partitions in the table.
   *
   * @param spark a Spark session
   * @param tableIdent a table identifier
   * @return all table's partitions
   */
  def getPartitions(spark: SparkSession, tableIdent: TableIdentifier): Seq[SparkPartition] = {
    val catalog = spark.sessionState.catalog
    val catalogTable = catalog.getTableMetadata(tableIdent)

    catalog
      .listPartitions(tableIdent)
      .map(catalogPartition => toSparkPartition(catalogPartition, catalogTable))
  }

  /**
   * Returns partitions that match the specified 'predicate'.
   *
   * @param spark a Spark session
   * @param table a table name and (optional) database
   * @param predicate a predicate on partition columns
   * @return matching table's partitions
   */
  def getPartitionsByFilter(spark: SparkSession, table: String, predicate: String): Seq[SparkPartition] = {
    val tableIdent = spark.sessionState.sqlParser.parseTableIdentifier(table)
    val unresolvedPredicateExpr = spark.sessionState.sqlParser.parseExpression(predicate)
    val resolvedPredicateExpr = resolveAttrs(spark, table, unresolvedPredicateExpr)
    getPartitionsByFilter(spark, tableIdent, resolvedPredicateExpr)
  }

  /**
   * Returns partitions that match the specified 'predicate'.
   *
   * @param spark a Spark session
   * @param tableIdent a table identifier
   * @param predicateExpr a predicate expression on partition columns
   * @return matching table's partitions
   */
  def getPartitionsByFilter(
      spark: SparkSession,
      tableIdent: TableIdentifier,
      predicateExpr: Expression): Seq[SparkPartition] = {

    val catalog = spark.sessionState.catalog
    val catalogTable = catalog.getTableMetadata(tableIdent)

    val resolvedPredicateExpr = if (!predicateExpr.resolved) {
      resolveAttrs(spark, tableIdent.quotedString, predicateExpr)
    } else {
      predicateExpr
    }

    catalog
      .listPartitionsByFilter(tableIdent, Seq(resolvedPredicateExpr))
      .map(catalogPartition => toSparkPartition(catalogPartition, catalogTable))
  }

  /**
   * Returns the data files in a partition by listing the partition location.
   *
   * For Parquet and ORC partitions, this will read metrics from the file footer. For Avro partitions,
   * metrics are set to null.
   *
   * @param partition a partition
   * @param conf a serializable Hadoop conf
   * @param metricsConfig a metrics conf
   * @return a Seq of [[SparkDataFile]]
   */
  def listPartition(
      partition: SparkPartition,
      conf: SerializableConfiguration,
      metricsConfig: MetricsConfig): Seq[SparkDataFile] = {

    listPartition(partition.values, partition.uri, partition.format, conf.get(), metricsConfig)
  }

  /**
   * Returns the data files in a partition by listing the partition location.
   *
   * For Parquet and ORC partitions, this will read metrics from the file footer. For Avro partitions,
   * metrics are set to null.
   *
   * @param partition partition key, e.g., "a=1/b=2"
   * @param uri partition location URI
   * @param format partition format, avro or parquet
   * @param conf a Hadoop conf
   * @param metricsConfig a metrics conf
   * @return a seq of [[SparkDataFile]]
   */
  def listPartition(
      partition: Map[String, String],
      uri: String,
      format: String,
      conf: Configuration = new Configuration(),
      metricsConfig: MetricsConfig = MetricsConfig.getDefault): Seq[SparkDataFile] = {

    if (format.contains("avro")) {
      listAvroPartition(partition, uri, conf)
    } else if (format.contains("parquet")) {
      listParquetPartition(partition, uri, conf, metricsConfig)
    } else if (format.contains("orc")) {
      // TODO: use MetricsConfig in listOrcPartition
      listOrcPartition(partition, uri, conf)
    } else {
      throw new UnsupportedOperationException(s"Unknown partition format: $format")
    }
  }

  /**
   * Case class representing a table partition.
   */
  case class SparkPartition(values: Map[String, String], uri: String, format: String)

  /**
   * Case class representing a data file.
   */
  case class SparkDataFile(
      path: String,
      partition: collection.Map[String, String],
      format: String,
      fileSize: Long,
      rowGroupSize: Long,
      rowCount: Long,
      columnSizes: Array[Long],
      valueCounts: Array[Long],
      nullValueCounts: Array[Long],
      lowerBounds: Seq[Array[Byte]],
      upperBounds: Seq[Array[Byte]]
    ) {

    /**
     * Convert this to a [[DataFile]] that can be added to a [[org.apache.iceberg.Table]].
     *
     * @param spec a [[PartitionSpec]] that will be used to parse the partition key
     * @return a [[DataFile]] that can be passed to [[org.apache.iceberg.AppendFiles]]
     */
    def toDataFile(spec: PartitionSpec): DataFile = {
      // values are strings, so pass a path to let the builder coerce to the right types
      val partitionKey = spec.fields.asScala.map(_.name).map { name =>
        s"$name=${partition(name)}"
      }.mkString("/")

      DataFiles.builder(spec)
        .withPath(path)
        .withFormat(format)
        .withFileSizeInBytes(fileSize)
        .withMetrics(new Metrics(rowCount,
          arrayToMap(columnSizes),
          arrayToMap(valueCounts),
          arrayToMap(nullValueCounts),
          arrayToMap(lowerBounds),
          arrayToMap(upperBounds)))
        .withPartitionPath(partitionKey)
        .build()
    }
  }

  private def bytesMapToArray(map: java.util.Map[Integer, ByteBuffer]): Seq[Array[Byte]] = {
    if (map != null && !map.isEmpty) {
      val keys = map.keySet.asScala
      val max = keys.max
      val arr = Array.fill(max + 1)(null.asInstanceOf[Array[Byte]])

      keys.foreach { key =>
        val buffer = map.get(key)

        val copy = if (buffer.hasArray) {
          val bytes = buffer.array()
          if (buffer.arrayOffset() == 0 && buffer.position() == 0 &&
              bytes.length == buffer.remaining()) {
            bytes
          } else {
            val start = buffer.arrayOffset() + buffer.position()
            val end = start + buffer.remaining()
            util.Arrays.copyOfRange(bytes, start, end);
          }
        } else {
          val bytes = Array.fill(buffer.remaining())(0.asInstanceOf[Byte])
          buffer.get(bytes)
          bytes
        }

        arr.update(key, copy)
      }

      arr
    } else {
      null
    }
  }

  private def mapToArray(map: java.util.Map[Integer, java.lang.Long]): Array[Long] = {
    if (map != null && !map.isEmpty) {
      val keys = map.keySet.asScala
      val max = keys.max
      val arr = Array.fill(max + 1)(-1L)

      keys.foreach { key =>
        arr.update(key, map.get(key))
      }

      arr
    } else {
      null
    }
  }

  private def arrayToMap(arr: Seq[Array[Byte]]): java.util.Map[Integer, ByteBuffer] = {
    if (arr != null) {
      val map: java.util.Map[Integer, ByteBuffer] = Maps.newHashMap()
      arr.zipWithIndex.foreach {
        case (null, _) => // skip
        case (value, index) => map.put(index, ByteBuffer.wrap(value))
      }
      map
    } else {
      null
    }
  }

  private def arrayToMap(arr: Array[Long]): java.util.Map[Integer, java.lang.Long] = {
    if (arr != null) {
      val map: java.util.Map[Integer, java.lang.Long] = Maps.newHashMap()
      arr.zipWithIndex.foreach {
        case (-1, _) => // skip default values
        case (value, index) => map.put(index, value)
      }
      map
    } else {
      null
    }
  }

  private object HiddenPathFilter extends PathFilter {
    override def accept(p: Path): Boolean = {
      !p.getName.startsWith("_") && !p.getName.startsWith(".")
    }
  }

  private def listAvroPartition(
      partitionPath: Map[String, String],
      partitionUri: String,
      conf: Configuration): Seq[SparkDataFile] = {
    val partition = new Path(partitionUri)
    val fs = partition.getFileSystem(conf)

    fs.listStatus(partition, HiddenPathFilter).filter(_.isFile).map { stat =>
      SparkDataFile(
        stat.getPath.toString,
        partitionPath, "avro", stat.getLen,
        stat.getBlockSize,
        -1,
        null,
        null,
        null,
        null,
        null)
    }
  }

  //noinspection ScalaDeprecation
  private def listParquetPartition(
      partitionPath: Map[String, String],
      partitionUri: String,
      conf: Configuration,
      metricsSpec: MetricsConfig): Seq[SparkDataFile] = {
    val partition = new Path(partitionUri)
    val fs = partition.getFileSystem(conf)

    fs.listStatus(partition, HiddenPathFilter).filter(_.isFile).map { stat =>
      val metrics = ParquetUtil.footerMetrics(ParquetFileReader.readFooter(conf, stat), metricsSpec)

      SparkDataFile(
        stat.getPath.toString,
        partitionPath, "parquet", stat.getLen,
        stat.getBlockSize,
        metrics.recordCount,
        mapToArray(metrics.columnSizes),
        mapToArray(metrics.valueCounts),
        mapToArray(metrics.nullValueCounts),
        bytesMapToArray(metrics.lowerBounds),
        bytesMapToArray(metrics.upperBounds))
    }
  }

  private def listOrcPartition(
      partitionPath: Map[String, String],
      partitionUri: String,
      conf: Configuration): Seq[SparkDataFile] = {
    val partition = new Path(partitionUri)
    val fs = partition.getFileSystem(conf)

    fs.listStatus(partition, HiddenPathFilter).filter(_.isFile).map { stat =>
      val metrics = OrcMetrics.fromInputFile(HadoopInputFile.fromPath(stat.getPath, conf))

      SparkDataFile(
        stat.getPath.toString,
        partitionPath, "orc", stat.getLen,
        stat.getBlockSize,
        metrics.recordCount,
        mapToArray(metrics.columnSizes),
        mapToArray(metrics.valueCounts),
        mapToArray(metrics.nullValueCounts),
        bytesMapToArray(metrics.lowerBounds()),
        bytesMapToArray(metrics.upperBounds())
      )
    }
  }

  private def toSparkPartition(partition: CatalogTablePartition, table: CatalogTable): SparkPartition = {
    val uri = partition.storage.locationUri.map(String.valueOf(_))
    require(uri.nonEmpty, "Partition URI should be defined")

    val format = partition.storage.serde.orElse(table.provider)
    require(format.nonEmpty, "Partition format should be defined")

    SparkPartition(partition.spec, uri.get, format.get)
  }

  private def resolveAttrs(spark: SparkSession, table: String, expr: Expression): Expression = {
    val resolver = spark.sessionState.analyzer.resolver
    val plan = spark.table(table).queryExecution.analyzed
    expr.transform {
      case attr: UnresolvedAttribute =>
        plan.resolve(attr.nameParts, resolver) match {
          case Some(resolvedAttr) => resolvedAttr
          case None => throw new IllegalArgumentException(s"Could not resolve $attr using columns: ${plan.output}")
        }
    }
  }

  private def buildManifest(
      conf: SerializableConfiguration,
      spec: PartitionSpec,
      basePath: String): Iterator[SparkDataFile] => Iterator[Manifest] = { files =>
    if (files.hasNext) {
      val io = new HadoopFileIO(conf.get())
      val ctx = TaskContext.get()
      val location = new Path(basePath, s"stage-${ctx.stageId()}-task-${ctx.taskAttemptId()}-manifest")
      val outputFile = io.newOutputFile(FileFormat.AVRO.addExtension(location.toString))
      val writer = ManifestWriter.write(spec, outputFile)
      try {
        files.foreach { file =>
          writer.add(file.toDataFile(spec))
        }
      } finally {
        writer.close()
      }

      val manifestFile = writer.toManifestFile
      Seq(Manifest(manifestFile.path, manifestFile.length, manifestFile.partitionSpecId)).iterator
    } else {
      Seq.empty.iterator
    }
  }

  private case class Manifest(location: String, fileLength: Long, specId: Int) {
    def toManifestFile: ManifestFile = new ManifestFile {
      override def path: String = location

      override def length: Long = fileLength

      override def partitionSpecId: Int = specId

      override def snapshotId: java.lang.Long = null

      override def addedFilesCount: Integer = null

      override def existingFilesCount: Integer = null

      override def deletedFilesCount: Integer = null

      override def partitions: java.util.List[ManifestFile.PartitionFieldSummary] = null

      override def copy: ManifestFile = this
    }
  }

  /**
   * Import files from an existing Spark table to an Iceberg table.
   *
   * The import uses the Spark session to get table metadata. It assumes no
   * operation is going on the original and target table and thus is not
   * thread-safe.
   *
   * @param spark a Spark session
   * @param sourceTableIdent an identifier of the source Spark table
   * @param targetTable an Iceberg table where to import the data
   * @param stagingDir a staging directory to store temporary manifest files
   */
  def importSparkTable(
      spark: SparkSession,
      sourceTableIdent: TableIdentifier,
      targetTable: Table,
      stagingDir: String): Unit = {

    val catalog = spark.sessionState.catalog

    val db = sourceTableIdent.database.getOrElse(catalog.getCurrentDatabase)
    val sourceTableIdentWithDB = sourceTableIdent.copy(database = Some(db))

    if (!catalog.tableExists(sourceTableIdentWithDB)) {
      throw new NoSuchTableException(s"Table $sourceTableIdentWithDB does not exist")
    }

    val spec = SparkSchemaUtil.specForTable(spark, sourceTableIdentWithDB.unquotedString)

    if (spec == PartitionSpec.unpartitioned) {
      importUnpartitionedSparkTable(spark, sourceTableIdentWithDB, targetTable)
    } else {
      val sourceTablePartitions = getPartitions(spark, sourceTableIdent)
      importSparkPartitions(spark, sourceTablePartitions, targetTable, spec, stagingDir)
    }
  }

  private def importUnpartitionedSparkTable(
      spark: SparkSession,
      sourceTableIdent: TableIdentifier,
      targetTable: Table): Unit = {

    val sourceTable = spark.sessionState.catalog.getTableMetadata(sourceTableIdent)
    val format = sourceTable.storage.serde.orElse(sourceTable.provider)
    require(format.nonEmpty, "Could not determine table format")

    val conf = spark.sessionState.newHadoopConf()
    val metricsConfig = MetricsConfig.fromProperties(targetTable.properties)

    val files = listPartition(Map.empty, sourceTable.location.toString, format.get, conf, metricsConfig)

    val append = targetTable.newAppend()
    files.foreach(file => append.appendFile(file.toDataFile(PartitionSpec.unpartitioned)))
    append.commit()
  }

  /**
   * Import files from given partitions to an Iceberg table.
   *
   * @param spark a Spark session
   * @param partitions partitions to import
   * @param targetTable an Iceberg table where to import the data
   * @param spec a partition spec
   * @param stagingDir a staging directory to store temporary manifest files
   */
  def importSparkPartitions(
      spark: SparkSession,
      partitions: Seq[SparkPartition],
      targetTable: Table,
      spec: PartitionSpec,
      stagingDir: String): Unit = {

    import spark.implicits._

    val conf = spark.sessionState.newHadoopConf()
    val serializableConf = new SerializableConfiguration(conf)
    val parallelism = Math.min(partitions.size, spark.sessionState.conf.parallelPartitionDiscoveryParallelism)
    val partitionDS = spark.sparkContext.parallelize(partitions, parallelism).toDS()
    val numShufflePartitions = spark.sessionState.conf.numShufflePartitions
    val metricsConfig = MetricsConfig.fromProperties(targetTable.properties)

    val manifests = partitionDS
      .flatMap(partition => listPartition(partition, serializableConf, metricsConfig))
      .repartition(numShufflePartitions)
      .orderBy($"path")
      .mapPartitions(buildManifest(serializableConf, spec, stagingDir))
      .collect()

    try {
      val append = targetTable.newAppend()
      manifests.foreach(manifest => append.appendManifest(manifest.toManifestFile))
      append.commit()
    } finally {
      val io = new HadoopFileIO(conf)
      manifests.foreach { manifest =>
        Try(io.deleteFile(manifest.location))
      }
    }
  }
}
