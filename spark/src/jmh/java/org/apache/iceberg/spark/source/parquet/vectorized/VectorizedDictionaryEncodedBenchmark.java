package org.apache.iceberg.spark.source.parquet.vectorized;

public abstract class VectorizedDictionaryEncodedBenchmark extends VectorizedIcebergSourceBenchmark {

    @Override
    protected void setupSpark() {
        setupSpark(true);
    }
}
