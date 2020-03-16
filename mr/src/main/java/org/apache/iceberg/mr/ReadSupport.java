package org.apache.iceberg.mr;

import java.util.function.BiFunction;
import org.apache.avro.io.DatumReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.MessageType;

import java.util.function.Function;


public interface ReadSupport<T> {
  default T withPartitionColumns(T row, Schema partitionSchema, PartitionSpec spec, StructLike partitionData) {
    return row;
  }

  default Function<MessageType, ParquetValueReader<?>> parquetReadFunction() {
    return null;
  }

  default Function<MessageType, VectorizedReader<?>> parquetBatchReadFunction() {
    return null;
  }

  default Function<org.apache.avro.Schema, DatumReader<?>> avroReadFunction() {
    return null;
  }

  default BiFunction<Schema, org.apache.avro.Schema, DatumReader<?>> avroReadBiFunction() {
    return null;
  }

  default Function<TypeDescription, OrcValueReader<?>> orcReadFunction() {
    return null;
  }
}
