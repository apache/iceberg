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
package org.apache.iceberg.avro;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestInternalWriter {

  @TempDir Path temp;

  @Test
  public void testDataWriter() throws IOException {
    Schema schema =
        new Schema(
            required(100, "b", Types.BooleanType.get()),
            optional(101, "i", Types.IntegerType.get()),
            required(102, "l", Types.LongType.get()),
            optional(103, "f", Types.FloatType.get()),
            required(104, "d", Types.DoubleType.get()),
            optional(105, "date", Types.DateType.get()),
            required(106, "time", Types.TimeType.get()),
            required(107, "ts", Types.TimestampType.withoutZone()),
            required(108, "ts_tz", Types.TimestampType.withZone()),
            required(109, "s", Types.StringType.get()),
            required(110, "uuid", Types.UUIDType.get()),
            required(111, "fixed", Types.FixedType.ofLength(7)),
            optional(112, "bytes", Types.BinaryType.get()),
            required(113, "dec_38_10", Types.DecimalType.of(38, 10)));

    // Consuming the data as per Type.java
    GenericRecord record = GenericRecord.create(schema);
    record.set(0, true);
    record.set(1, 42);
    record.set(2, 42L);
    record.set(3, 3.14f);
    record.set(4, 3.141592653589793);
    record.set(5, Literal.of("2022-01-01").to(Types.DateType.get()).value());
    record.set(6, Literal.of("10:10:10").to(Types.TimeType.get()).value());
    record.set(
        7, Literal.of("2017-12-01T10:12:55.038194").to(Types.TimestampType.withoutZone()).value());
    record.set(
        8,
        Literal.of("2017-11-29T11:30:07.123456+01:00").to(Types.TimestampType.withZone()).value());
    record.set(9, "string");
    record.set(10, UUID.randomUUID());
    record.set(11, ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6}));
    record.set(12, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    record.set(
        13, Literal.of("12345678901234567890.1234567890").to(Types.DecimalType.of(38, 10)).value());

    StructProjection structProjection = StructProjection.create(schema, schema);
    StructProjection row = structProjection.wrap(record);

    OutputFile file = Files.localOutput(temp.toFile());

    DataWriter<StructLike> dataWriter =
        Avro.writeData(file)
            .schema(schema)
            .createWriterFunc(InternalWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    try {
      dataWriter.write(row);
    } finally {
      dataWriter.close();
    }

    DataFile dataFile = dataWriter.toDataFile();

    assertThat(dataFile.format()).as("Format should be Avro").isEqualTo(FileFormat.AVRO);
    assertThat(dataFile.content()).as("Should be data file").isEqualTo(FileContent.DATA);
    assertThat(dataFile.partition().size()).as("Partition should be empty").isEqualTo(0);
    assertThat(dataFile.keyMetadata()).as("Key metadata should be null").isNull();

    List<StructLike> writtenRecords;
    try (AvroIterable<StructLike> reader =
        Avro.read(file.toInputFile())
            .project(schema)
            .createResolvingReader(InternalReader::create)
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }
    assertThat(writtenRecords).hasSize(1);
    Comparator<StructLike> structLikeComparator = Comparators.forType(schema.asStruct());
    assertThat(structLikeComparator.compare(writtenRecords.get(0), row))
        .as("Written records should match")
        .isZero();
  }
}
