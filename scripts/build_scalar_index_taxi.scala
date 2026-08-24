/**
 * Build a SCALAR HASH index on NYC Yellow Taxi data (medallion column), and demonstrate a
 * subsequent point lookup being pruned to fewer files.
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
 *
 * Unlike the version of this script that predates the real build_scalar_index procedure, this
 * one does not hand-roll the build pipeline (compute buckets, sort, write Parquet, commit) --
 * it calls the actual procedure via SQL, so it exercises the real code path rather than a
 * parallel implementation that could drift from it. The old hand-rolled version also never
 * computed `position`, so it only ever achieved file-level pruning, never the exact-row
 * `(file, position)` pruning that's the actual point of a SCALAR index over a Bloom filter
 * index; this version's build path does compute position correctly (see
 * BuildScalarIndexProcedure), though the read-side demonstration below still only observes
 * file-level pruning via input_file_name()/inputFiles(), since row-position-level pushdown into
 * the scan itself is a documented, not-yet-attempted follow-up (see SparkScanBuilder's
 * tryPruneUsingScalarIndex javadoc).
 */

import org.apache.spark.sql.functions._

// ── 1. Setup ────────────────────────────────────────────────────────────────

val TABLE_NAME  = "local.taxi.yellow_trips"
val KEY_COLUMN  = "medallion"

// ── 2. Look at the source table before indexing ──────────────────────────────

println(s"Reading $TABLE_NAME ...")
val taxiDf = spark.read.format("iceberg").load(TABLE_NAME)
val totalRows = taxiDf.count()
val totalSourceFiles = taxiDf.select(input_file_name()).distinct().count()
println(s"Total rows: $totalRows")
println(s"Source files: $totalSourceFiles")

// ── 3. Build the index via the real procedure ────────────────────────────────

println(s"\nBuilding SCALAR HASH index on $KEY_COLUMN ...")
val buildResult = spark.sql(
  s"""CALL local.system.build_scalar_index(
        table     => '$TABLE_NAME',
        columns   => array('$KEY_COLUMN'),
        transform => 'HASH',
        options   => map('hash.num-buckets', '256')
      )"""
)
buildResult.show(false)

// ── 4. Point lookup before vs. after ──────────────────────────────────────────

// Pick a real medallion value from the table so the lookup actually matches a row, rather than
// hardcoding a value that may not exist in whatever dataset is loaded.
val sampleMedallion = taxiDf.select(KEY_COLUMN).limit(1).collect()(0).getString(0)
println(s"\nLooking up $KEY_COLUMN = '$sampleMedallion' ...")

val lookupDf = spark.sql(s"SELECT * FROM $TABLE_NAME WHERE $KEY_COLUMN = '$sampleMedallion'")
val matchedRows = lookupDf.count()
val filesReadForLookup = lookupDf.inputFiles.length

val unindexedComparisonDf = spark.sql(s"SELECT * FROM $TABLE_NAME WHERE id = -1")
val filesReadWithoutIndexablePredicate = unindexedComparisonDf.inputFiles.length

println(s"Rows matched: $matchedRows")
println(s"Files read for the indexed lookup: $filesReadForLookup (out of $totalSourceFiles total)")
println(
  s"Files read for a predicate the index can't help with: $filesReadWithoutIndexablePredicate " +
    s"(shown only as a rough point of comparison, not an apples-to-apples baseline)"
)
