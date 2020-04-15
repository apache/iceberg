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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.iceberg.{DataFile, DataFiles, FileFormat, ManifestFile, ManifestFiles}
import org.apache.iceberg.{Metrics, MetricsConfig, PartitionSpec, Table, TableProperties}
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.hadoop.{HadoopFileIO, HadoopInputFile, SerializableConfiguration}
import org.apache.iceberg.orc.OrcMetrics
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.iceberg.util.PropertyUtil
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
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
   * @return a Seq of [[DataFile]]
   */
  def listPartition(
      partition: SparkPartition,
      spec: PartitionSpec,
      conf: SerializableConfiguration,
      metricsConfig: MetricsConfig): Seq[DataFile] = {

    listPartition(partition.values, partition.uri, partition.format, spec, conf.get(), metricsConfig)
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
   * @return a seq of [[DataFile]]
   */
  def listPartition(
      partition: Map[String, String],
      uri: String,
      format: String,
      spec: PartitionSpec,
      conf: Configuration = new Configuration(),
      metricsConfig: MetricsConfig = MetricsConfig.getDefault): Seq[DataFile] = {

    if (format.contains("avro")) {
      listAvroPartition(partition, uri, spec, conf)
    } else if (format.contains("parquet")) {
      listParquetPartition(partition, uri, spec, conf, metricsConfig)
    } else if (format.contains("orc")) {
      // TODO: use MetricsConfig in listOrcPartition
      listOrcPartition(partition, uri, spec, conf)
    } else {
      throw new UnsupportedOperationException(s"Unknown partition format: $format")
    }
  }

  /**
   * Case class representing a table partition.
   */
  case class SparkPartition(values: Map[String, String], uri: String, format: String)

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
      spec: PartitionSpec,
      conf: Configuration): Seq[DataFile] = {
    val partition = new Path(partitionUri)
    val fs = partition.getFileSystem(conf)

    fs.listStatus(partition, HiddenPathFilter).filter(_.isFile).map { stat =>
      val metrics = new Metrics(-1L, arrayToMap(null), arrayToMap(null), arrayToMap(null))
      val partitionKey = spec.fields.asScala.map(_.name).map { name =>
        s"$name=${partitionPath(name)}"
      }.mkString("/")

      DataFiles.builder(spec)
        .withPath(stat.getPath.toString)
        .withFormat("avro")
        .withFileSizeInBytes(stat.getLen)
        .withMetrics(metrics)
        .withPartitionPath(partitionKey)
        .build()
    }
  }

  //noinspection ScalaDeprecation
  private def listParquetPartition(
      partitionPath: Map[String, String],
      partitionUri: String,
      spec: PartitionSpec,
      conf: Configuration,
      metricsSpec: MetricsConfig): Seq[DataFile] = {
    val partition = new Path(partitionUri)
    val fs = partition.getFileSystem(conf)

    fs.listStatus(partition, HiddenPathFilter).filter(_.isFile).map { stat =>
      val metrics = ParquetUtil.footerMetrics(ParquetFileReader.readFooter(conf, stat), metricsSpec)
      val partitionKey = spec.fields.asScala.map(_.name).map { name =>
        s"$name=${partitionPath(name)}"
      }.mkString("/")

      DataFiles.builder(spec)
        .withPath(stat.getPath.toString)
        .withFormat("parquet")
        .withFileSizeInBytes(stat.getLen)
        .withMetrics(metrics)
        .withPartitionPath(partitionKey)
        .build()
    }
  }

  private def listOrcPartition(
      partitionPath: Map[String, String],
      partitionUri: String,
      spec: PartitionSpec,
      conf: Configuration): Seq[DataFile] = {
    val partition = new Path(partitionUri)
    val fs = partition.getFileSystem(conf)

    fs.listStatus(partition, HiddenPathFilter).filter(_.isFile).map { stat =>
      val metrics = OrcMetrics.fromInputFile(HadoopInputFile.fromPath(stat.getPath, conf))
      val partitionKey = spec.fields.asScala.map(_.name).map { name =>
        s"$name=${partitionPath(name)}"
      }.mkString("/")

      DataFiles.builder(spec)
        .withPath(stat.getPath.toString)
        .withFormat("orc")
        .withFileSizeInBytes(stat.getLen)
        .withMetrics(metrics)
        .withPartitionPath(partitionKey)
        .build()
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
      basePath: String): Iterator[DataFile] => Iterator[ManifestFile] = { files =>
    if (files.hasNext) {
      val io = new HadoopFileIO(conf.get())
      val ctx = TaskContext.get()
      val location = new Path(basePath, s"stage-${ctx.stageId()}-task-${ctx.taskAttemptId()}-manifest")
      val outputFile = io.newOutputFile(FileFormat.AVRO.addExtension(location.toString))
      val writer = ManifestFiles.write(spec, outputFile)
      try {
        files.foreach(writer.add)
      } finally {
        writer.close()
      }

      val manifestFile = writer.toManifestFile
      Seq(manifestFile).iterator
    } else {
      Seq.empty.iterator
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

    val partition = Map.empty[String, String]
    val spec = PartitionSpec.unpartitioned()
    val conf = spark.sessionState.newHadoopConf()
    val metricsConfig = MetricsConfig.fromProperties(targetTable.properties)

    val files = listPartition(partition, sourceTable.location.toString, format.get, spec, conf, metricsConfig)

    val append = targetTable.newAppend()
    files.foreach(append.appendFile)
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

    implicit val manifestFileEncoder: Encoder[ManifestFile] = Encoders.javaSerialization[ManifestFile]
    implicit val dataFileEncoder: Encoder[DataFile] = Encoders.javaSerialization[DataFile]
    implicit val pathDataFileEncoder: Encoder[(String, DataFile)] = Encoders.tuple(Encoders.STRING, dataFileEncoder)

    import spark.implicits._

    val conf = spark.sessionState.newHadoopConf()
    val serializableConf = new SerializableConfiguration(conf)
    val parallelism = Math.min(partitions.size, spark.sessionState.conf.parallelPartitionDiscoveryParallelism)
    val partitionDS = spark.sparkContext.parallelize(partitions, parallelism).toDS()
    val numShufflePartitions = spark.sessionState.conf.numShufflePartitions
    val metricsConfig = MetricsConfig.fromProperties(targetTable.properties)

    val manifests = partitionDS
      .flatMap(partition => listPartition(partition, spec, serializableConf, metricsConfig))
      .repartition(numShufflePartitions)
      .map(file => (file.path.toString, file))
      .orderBy($"_1")
      .mapPartitions(files => buildManifest(serializableConf, spec, stagingDir)(files.map(_._2)))
      .collect()

    try {
      val snapshotIdInheritanceEnabled = PropertyUtil.propertyAsBoolean(
        targetTable.properties,
        TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
        TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT)

      val append = targetTable.newAppend()
      manifests.foreach(manifest => append.appendManifest(manifest))
      append.commit()

      if (!snapshotIdInheritanceEnabled) {
        // delete original manifests as they were rewritten before the commit
        manifests.foreach(manifest => Try(targetTable.io.deleteFile(manifest.path)))
      }
    } catch {
      case e: Throwable =>
        // always clean up created manifests if the append fails
        manifests.foreach(manifest => Try(targetTable.io.deleteFile(manifest.path)))
        throw e;
    }
  }
}
