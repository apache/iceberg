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


public interface ReadSupport {
  default <T> T addPartitionColumns(T row, Schema partitionSchema, PartitionSpec spec, StructLike partitionData) {
    return row;
  }

  default AvroReadFuncs avroReadFuncs() {
    return null;
  }

  default ParquetReadFuncs parquetReadFuncs() {
    return null;
  }

  default OrcReadFuncs orcReadFuncs() {
    return null;
  }

  interface ParquetReadFuncs {
    default Function<MessageType, ParquetValueReader<?>> parquetReadFunction() {
      return null;
    }

    default Function<MessageType, VectorizedReader<?>> parquetBatchReadFunction() {
      return null;
    }
  }

  interface AvroReadFuncs {
    default Function<org.apache.avro.Schema, DatumReader<?>> avroReadFunction() {
      return null;
    }

    default BiFunction<Schema, org.apache.avro.Schema, DatumReader<?>> avroReadBiFunction() {
      return null;
    }
  }

  interface OrcReadFuncs {
    default Function<TypeDescription, OrcValueReader<?>> orcReadFunction() {
      return null;
    }
  }
}
