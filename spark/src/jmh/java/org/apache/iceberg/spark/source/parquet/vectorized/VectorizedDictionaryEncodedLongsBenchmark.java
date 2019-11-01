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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class VectorizedDictionaryEncodedLongsBenchmark extends VectorizedDictionaryEncodedBenchmark {
    @Override
    protected final Table initTable() {
        Schema schema = new Schema(
                optional(1, "longCol", Types.LongType.get()),
                optional(2, "longCol2", Types.LongType.get()));
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
                    .withColumnRenamed("id", "longCol")
                    .drop("id")
                    .withColumn("longCol2", when(pmod(col("longCol"), lit(2)).equalTo(lit(0l)), lit(0)).otherwise(lit(1l)));
            appendAsFile(df);
        }
    }
}
