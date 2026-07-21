/**
 * Build a SCALAR HASH index on NYC Yellow Taxi data (medallion column).
 *
 * Run with spark-shell or spark-submit:
 *   spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0 \
 *               --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
 *               --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
 *               --conf spark.sql.catalog.local.type=hadoop \
 *               --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse \
 *               -i scripts/build_scalar_index_taxi.scala
 *
 * Or paste directly into spark-shell.
 */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.iceberg.index._
import org.apache.iceberg.catalog.{TableIdentifier, Namespace}
import org.apache.iceberg.hadoop.HadoopFileIO
import com.google.common.collect.ImmutableList
import java.io.File

// ── 1. Setup ──────────────────────────────────────────────────────────────────

val TABLE_NAME    = "local.taxi.yellow_trips"
val INDEX_LOCATION = "/tmp/iceberg-warehouse/taxi/yellow_trips/index/medallion_idx"
val NUM_BUCKETS   = 256
val KEY_COLUMN    = "medallion"
val KEY_COLUMN_ID = 3  // Iceberg field id for medallion in the table schema

// ── 2. Load source table ───────────────────────────────────────────────────────

println(s"Reading $TABLE_NAME ...")
val taxiDf = spark.read.format("iceberg").load(TABLE_NAME)
println(s"Total rows: ${taxiDf.count()}")
println(s"Source files: approximately ${taxiDf.select(input_file_name()).distinct().count()}")

// ── 3. Compute transform values and collect file metadata ─────────────────────

val transform = new HashTransform(NUM_BUCKETS)

// Broadcast the transform to executors
val numBuckets = NUM_BUCKETS
val withTransform = taxiDf
  .select(
    col(KEY_COLUMN),
    input_file_name().as("source_file_path")
  )
  // Compute hash bucket on the driver-broadcast numBuckets
  .withColumn(
    "transform_value",
    (hash(col(KEY_COLUMN)) % numBuckets + numBuckets) % numBuckets
  )

println("Sample transform values:")
withTransform.show(5)

// ── 4. Write sorted leaf files (Parquet) ──────────────────────────────────────

val leafOutputPath = s"$INDEX_LOCATION/data"
println(s"Writing leaf files to $leafOutputPath ...")

withTransform
  .repartitionByRange(numBuckets / 64, col("transform_value"))  // ~4 leaf files
  .sortWithinPartitions(col("transform_value"), col(KEY_COLUMN))
  .write
  .format("parquet")
  .mode("overwrite")
  .save(leafOutputPath)

println("Leaf files written.")

// ── 5. Collect leaf file metadata (path, count, size, bounds) ─────────────────

val leafFiles = spark.read.parquet(leafOutputPath)
  .select(
    input_file_name().as("path"),
    col("transform_value")
  )
  .groupBy("path")
  .agg(
    count("*").as("record_count"),
    min("transform_value").as("tv_min"),
    max("transform_value").as("tv_max")
  )
  .collect()
  .map { row =>
    val path     = row.getString(0)
    val count    = row.getLong(1)
    val tvMin    = row.getLong(2)
    val tvMax    = row.getLong(3)
    val sizeBytes = new File(path.replace("file:", "")).length()
    new LeafFileMetadata(path, "parquet", count, sizeBytes, tvMin, tvMax)
  }
  .toList

println(s"Leaf file count: ${leafFiles.size}")
leafFiles.foreach { lf =>
  println(s"  ${lf.path().split("/").last} | rows=${lf.recordCount()} " +
    s"| buckets=[${lf.transformValueMin()}, ${lf.transformValueMax()}]")
}

// ── 6. Commit index via ScalarIndexCommitter ───────────────────────────────────

val hadoopConf = spark.sparkContext.hadoopConfiguration
val fileIO     = new HadoopFileIO(hadoopConf)
val catalog    = new InMemoryIndexCatalog()  // swap for HadoopIndexCatalog in production
val committer  = new ScalarIndexCommitter(catalog, fileIO)

val tableIdent = TableIdentifier.of(Namespace.of("taxi"), "yellow_trips")
val indexIdent = IndexIdentifier.of(tableIdent, "medallion_idx")

// Get the current table snapshot id
val icebergTable = spark.sessionState.catalogManager
  .catalog("local")
  .asInstanceOf[org.apache.iceberg.spark.SparkCatalog]
  .loadTable(tableIdent.asInstanceOf[org.apache.iceberg.catalog.TableIdentifier])
val tableSnapshotId = icebergTable.currentSnapshot().snapshotId()

import scala.jdk.CollectionConverters._
committer.commit(
  indexIdent,
  icebergTable.uuid(),
  tableSnapshotId,
  "SCALAR",
  "HASH",
  ImmutableList.of(KEY_COLUMN_ID),
  ImmutableList.of(),
  Map("hash.num-buckets" -> NUM_BUCKETS.toString).asJava,
  INDEX_LOCATION,
  leafFiles.asJava
)

println(s"\n✅ Index committed: $indexIdent")
val meta = catalog.loadIndex(indexIdent)
println(s"   UUID:     ${meta.uuid()}")
println(s"   Snapshot: ${meta.currentSnapshotId()}")
println(s"   Tracking: ${meta.currentSnapshot().trackingFile()}")

// ── 7. Simulate a planner lookup ──────────────────────────────────────────────

val queryMedallion = "D7D598CD99978BD012A87A76A7C891B7"
val queryBucket    = transform.apply(queryMedallion)
println(s"\n🔍 Planning query: WHERE medallion = '$queryMedallion'")
println(s"   Hash bucket: $queryBucket")

val trackingPath = meta.currentSnapshot().trackingFile()
val trackingFile = fileIO.newInputFile(trackingPath)
val matchingLeafFiles = TrackingFileReader.readMatching(trackingFile, queryBucket, queryBucket)

println(s"   Leaf files to scan: ${matchingLeafFiles.size()} (out of ${leafFiles.size} total)")
matchingLeafFiles.forEach { entry =>
  println(s"   → ${entry.location().split("/").last} | buckets=[${entry.transformValueLowerBound()},${entry.transformValueUpperBound()}]")
}
println(s"\n   Without index: scan all ${taxiDf.select(input_file_name()).distinct().count()} source files")
println(s"   With index:    scan 1 leaf file → at most 1 source file")
