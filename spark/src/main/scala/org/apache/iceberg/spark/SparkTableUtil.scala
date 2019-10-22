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
import org.apache.iceberg.hadoop.{HadoopFileIO, HadoopInputFile, HadoopTables, SerializableConfiguration}
import org.apache.iceberg.orc.OrcMetrics
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.iceberg.spark.hacks.Hive
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import scala.collection.JavaConverters._

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

    val partitions: Seq[(Map[String, String], Option[String], Option[String])] =
      Hive.partitions(spark, table).map { p: CatalogTablePartition =>
        (p.spec, p.storage.locationUri.map(String.valueOf(_)), p.storage.serde)
      }

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

    val expr = spark.sessionState.sqlParser.parseExpression(expression)
    val partitions: Seq[(Map[String, String], Option[String], Option[String])] =
      Hive.partitionsByFilter(spark, table, expr).map { p: CatalogTablePartition =>
        (p.spec, p.storage.locationUri.map(String.valueOf(_)), p.storage.serde)
      }

    partitions.toDF("partition", "uri", "format")
  }

  /**
   * Returns the data files in a partition by listing the partition location.
   *
   * For Parquet partitions, this will read metrics from the file footer. For Avro partitions,
   * metrics are set to null.
   *
   * @param partition partition key, e.g., "a=1/b=2"
   * @param uri partition location URI
   * @param format partition format, avro or parquet
   * @return a seq of [[SparkDataFile]]
   */
  def listPartition(
      partition: Map[String, String],
      uri: String,
      format: String,
      conf: Configuration = new Configuration()): Seq[SparkDataFile] = {
    if (format.contains("avro")) {
      listAvroPartition(partition, uri, conf)
    } else if (format.contains("parquet")) {
      listParquetPartition(partition, uri, conf)
    } else if (format.contains("orc")) {
      listOrcPartition(partition, uri, conf)
    } else {
      throw new UnsupportedOperationException(s"Unknown partition format: $format")
    }
  }

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
      metricsSpec: MetricsConfig = MetricsConfig.getDefault): Seq[SparkDataFile] = {
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

  private def buildManifest(conf: SerializableConfiguration,
      sparkDataFiles: Seq[SparkDataFile],
      partitionSpec: PartitionSpec,
      basePath: String): Iterator[Manifest] = {
    if (sparkDataFiles.isEmpty) {
      Seq.empty.iterator
    } else {
      val io = new HadoopFileIO(conf.get())
      val ctx = TaskContext.get()
      val location = new Path(basePath,
        s"stage-${ctx.stageId()}-task-${ctx.taskAttemptId()}-manifest")
      val outputFile = io.newOutputFile(FileFormat.AVRO.addExtension(location.toString))
      val writer = ManifestWriter.write(partitionSpec, outputFile)
      try {
        sparkDataFiles.foreach { file =>
          writer.add(file.toDataFile(partitionSpec))
        }
      } finally {
        writer.close()
      }

      val manifestFile = writer.toManifestFile
      Seq(Manifest(manifestFile.path, manifestFile.length, manifestFile.partitionSpecId)).iterator
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
   * Import a spark table to a iceberg table.
   *
   * The import uses the spark session to get table metadata. It assumes no
   * operation is going on original table and target table and thus is not
   * thread-safe.
   *
   * @param source the database name of the table to be import
   * @param stagingDir the staging directory to store temporary manifest file
   * @param table the target table to import
   */
  def importSparkTable(
      source: TableIdentifier,
      stagingDir: String,
      table: Table): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.sqlContext.implicits._

    val dbName = source.database.getOrElse("default")
    val tableName = source.table

    if (!sparkSession.catalog.tableExists(dbName, tableName)) {
      throw new NoSuchTableException(s"Table $dbName.$tableName does not exist")
    }

    val partitionSpec = SparkSchemaUtil.specForTable(sparkSession, s"$dbName.$tableName")
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val serializableConfiguration = new SerializableConfiguration(conf)
    val appender = table.newAppend()

    if (partitionSpec == PartitionSpec.unpartitioned) {
      val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(source)
      val files = listPartition(Map.empty[String, String], catalogTable.location.toString,
        catalogTable.storage.serde.getOrElse("none"))
      files.foreach{f => appender.appendFile(f.toDataFile(PartitionSpec.unpartitioned))}
    } else {
      val partitions = partitionDF(sparkSession, s"$dbName.$tableName")
      val manifests = partitions.flatMap { row =>
        listPartition(row.getMap[String, String](0).toMap, row.getString(1), row.getString(2))
      }.repartition(sparkSession.sessionState.conf.numShufflePartitions)
        .orderBy($"path")
        .mapPartitions {
        files => buildManifest(serializableConfiguration, files.toSeq, partitionSpec, stagingDir)
      }.collect().map(_.toManifestFile)
      manifests.foreach(appender.appendManifest)
    }

    appender.commit()
  }

}

