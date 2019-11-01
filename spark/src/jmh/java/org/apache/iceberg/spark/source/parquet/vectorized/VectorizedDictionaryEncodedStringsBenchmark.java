package org.apache.iceberg.spark.source.parquet.vectorized;

import com.google.common.collect.Maps;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.UUID;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.spark.sql.functions.*;

public class VectorizedDictionaryEncodedStringsBenchmark extends VectorizedDictionaryEncodedBenchmark {
    @Override
    protected final Table initTable() {
        Schema schema = new Schema(
                optional(1, "longCol", Types.LongType.get()), optional(2, "stringCol", Types.StringType.get()));
        PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
        HadoopTables tables = new HadoopTables(hadoopConf());
        Map<String, String> properties = Maps.newHashMap();
        properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
        return tables.create(schema, partitionSpec, properties, newTableLocation());
    }

    @Override
    protected void appendData() {
        for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
            Dataset<Row> df = spark().range(NUM_ROWS)
                    .withColumn("longCol",
                            when(pmod(col("id"), lit(9))
                                    .equalTo(lit(0)), lit(0l))
                                    .when(expr("id > NUM_ROWS/2"), lit(UUID.randomUUID().toString()))
                                    .when(pmod(col("id"), lit(9))
                                            .equalTo(lit(1)), lit(1l))
                                    .when(pmod(col("id"), lit(9))
                                            .equalTo(lit(2)), lit(2l))
                                    .when(pmod(col("id"), lit(9))
                                            .equalTo(lit(3)), lit(3l))
                                    .when(pmod(col("id"), lit(9))
                                            .equalTo(lit(4)), lit(4l))
                                    .when(pmod(col("id"), lit(9))
                                            .equalTo(lit(5)), lit(5l))
                                    .when(pmod(col("id"), lit(9))
                                            .equalTo(lit(6)), lit(6l))
                                    .when(pmod(col("id"), lit(9))
                                            .equalTo(lit(7)), lit(7l))
                                    .when(pmod(col("id"), lit(9))
                                            .equalTo(lit(8)), lit(8l))
                                    .otherwise(lit(2l)))
                    .drop("id")
                    .withColumn("stringCol",
                            when(col("longCol")
                                    .equalTo(lit(1)), lit("1"))
                                    .when(col("longCol")
                                            .equalTo(lit(2)), lit("2"))
                                    .when(col("longCol")
                                            .equalTo(lit(3)), lit("3"))
                                    .when(col("longCol")
                                            .equalTo(lit(4)), lit("4"))
                                    .when(col("longCol")
                                            .equalTo(lit(5)), lit("5"))
                                    .when(col("longCol")
                                            .equalTo(lit(6)), lit("6"))
                                    .when(col("longCol")
                                            .equalTo(lit(7)), lit("7"))
                                    .when(col("longCol")
                                            .equalTo(lit(8)), lit("8"))
                                        .when(col("longCol")
                                            .equalTo(lit(9)), lit("9"))
                                    .otherwise(lit(UUID.randomUUID().toString())));
            appendAsFile(df);
        }
    }
}
