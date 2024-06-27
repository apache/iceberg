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
package org.apache.iceberg.data.avro;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.message.BadHeaderException;
import org.apache.avro.message.MessageDecoder;
import org.apache.avro.message.MessageEncoder;
import org.apache.avro.message.MissingSchemaException;
import org.apache.avro.message.SchemaStore;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Ordering;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestSingleMessageEncoding {
  private static final Schema SCHEMA_V1 =
      new Schema(
          required(0, "id", Types.IntegerType.get()), optional(1, "msg", Types.StringType.get()));

  private static Record v1Record(int id, String msg) {
    Record rec = GenericRecord.create(SCHEMA_V1.asStruct());
    rec.setField("id", id);
    rec.setField("msg", msg);
    return rec;
  }

  private static final List<Record> V1_RECORDS =
      Arrays.asList(v1Record(1, "m-1"), v1Record(2, "m-2"), v1Record(4, "m-4"), v1Record(6, "m-6"));

  private static final Schema SCHEMA_V2 =
      new Schema(
          required(0, "id", Types.LongType.get()),
          optional(1, "message", Types.StringType.get()),
          optional(2, "data", Types.DoubleType.get()));

  private static Record v2Record(long id, String message, Double data) {
    Record rec = GenericRecord.create(SCHEMA_V2.asStruct());
    rec.setField("id", id);
    rec.setField("message", message);
    rec.setField("data", data);
    return rec;
  }

  private static final List<Record> V2_RECORDS =
      Arrays.asList(
          v2Record(3L, "m-3", 12.3),
          v2Record(5L, "m-5", 23.4),
          v2Record(7L, "m-7", 34.5),
          v2Record(8L, "m-8", 35.6));

  @Test
  public void testByteBufferRoundTrip() throws Exception {
    MessageEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA_V2);
    MessageDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA_V2);

    Record copy = decoder.decode(encoder.encode(V2_RECORDS.get(0)));

    Assert.assertTrue("Copy should not be the same object", copy != V2_RECORDS.get(0));
    Assert.assertEquals("Record should be identical after round-trip", V2_RECORDS.get(0), copy);
  }

  @Test
  public void testSchemaEvolution() throws Exception {
    List<ByteBuffer> buffers = Lists.newArrayList();
    List<Record> records =
        Ordering.usingToString().sortedCopy(Iterables.concat(V1_RECORDS, V2_RECORDS));

    MessageEncoder<Record> v1Encoder = new IcebergEncoder<>(SCHEMA_V1);
    MessageEncoder<Record> v2Encoder = new IcebergEncoder<>(SCHEMA_V2);

    for (Record record : records) {
      if (record.struct() == SCHEMA_V1.asStruct()) {
        buffers.add(v1Encoder.encode(record));
      } else {
        buffers.add(v2Encoder.encode(record));
      }
    }

    Set<Record> allAsV2 = Sets.newHashSet(V2_RECORDS);
    allAsV2.add(v2Record(1L, "m-1", null));
    allAsV2.add(v2Record(2L, "m-2", null));
    allAsV2.add(v2Record(4L, "m-4", null));
    allAsV2.add(v2Record(6L, "m-6", null));

    IcebergDecoder<Record> v2Decoder = new IcebergDecoder<>(SCHEMA_V2);
    v2Decoder.addSchema(SCHEMA_V1);

    Set<Record> decodedUsingV2 = Sets.newHashSet();
    for (ByteBuffer buffer : buffers) {
      decodedUsingV2.add(v2Decoder.decode(buffer));
    }

    Assert.assertEquals(allAsV2, decodedUsingV2);
  }

  @Test
  public void testCompatibleReadFailsWithoutSchema() throws Exception {
    MessageEncoder<Record> v1Encoder = new IcebergEncoder<>(SCHEMA_V1);
    MessageDecoder<Record> v2Decoder = new IcebergDecoder<>(SCHEMA_V2);

    ByteBuffer v1Buffer = v1Encoder.encode(V1_RECORDS.get(3));

    assertThatThrownBy(() -> v2Decoder.decode(v1Buffer))
        .isInstanceOf(MissingSchemaException.class)
        .hasMessageContaining("Cannot resolve schema for fingerprint");
  }

  @Test
  public void testCompatibleReadWithSchema() throws Exception {
    MessageEncoder<Record> v1Encoder = new IcebergEncoder<>(SCHEMA_V1);
    IcebergDecoder<Record> v2Decoder = new IcebergDecoder<>(SCHEMA_V2);
    v2Decoder.addSchema(SCHEMA_V1);

    ByteBuffer v1Buffer = v1Encoder.encode(V1_RECORDS.get(3));

    Record record = v2Decoder.decode(v1Buffer);

    Assert.assertEquals(v2Record(6L, "m-6", null), record);
  }

  @Test
  public void testCompatibleReadWithSchemaFromLookup() throws Exception {
    MessageEncoder<Record> v1Encoder = new IcebergEncoder<>(SCHEMA_V1);

    SchemaStore.Cache schemaCache = new SchemaStore.Cache();
    schemaCache.addSchema(AvroSchemaUtil.convert(SCHEMA_V1, "table"));
    IcebergDecoder<Record> v2Decoder = new IcebergDecoder<>(SCHEMA_V2, schemaCache);

    ByteBuffer v1Buffer = v1Encoder.encode(V1_RECORDS.get(2));

    Record record = v2Decoder.decode(v1Buffer);

    Assert.assertEquals(v2Record(4L, "m-4", null), record);
  }

  @Test
  public void testBufferReuse() throws Exception {
    // This test depends on the serialized version of record 1 being smaller or
    // the same size as record 0 so that the reused ByteArrayOutputStream won't
    // expand its internal buffer.
    MessageEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA_V1, false);

    ByteBuffer b0 = encoder.encode(V1_RECORDS.get(0));
    ByteBuffer b1 = encoder.encode(V1_RECORDS.get(1));

    Assert.assertEquals(b0.array(), b1.array());

    MessageDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA_V1);
    Assert.assertEquals(
        "Buffer was reused, decode(b0) should be record 1", V1_RECORDS.get(1), decoder.decode(b0));
  }

  @Test
  public void testBufferCopy() throws Exception {
    MessageEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA_V1);

    ByteBuffer b0 = encoder.encode(V1_RECORDS.get(0));
    ByteBuffer b1 = encoder.encode(V1_RECORDS.get(1));

    Assert.assertNotEquals(b0.array(), b1.array());

    MessageDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA_V1);
    // bytes are not changed by reusing the encoder
    Assert.assertEquals(
        "Buffer was copied, decode(b0) should be record 0", V1_RECORDS.get(0), decoder.decode(b0));
  }

  @Test
  public void testByteBufferMissingPayload() throws Exception {
    MessageEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA_V2);
    MessageDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));

    buffer.limit(12);

    assertThatThrownBy(() -> decoder.decode(buffer))
        .isInstanceOf(AvroRuntimeException.class)
        .hasMessageContaining("Decoding datum failed");
  }

  @Test
  public void testByteBufferMissingFullHeader() throws Exception {
    MessageEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA_V2);
    MessageDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));

    buffer.limit(8);

    assertThatThrownBy(() -> decoder.decode(buffer))
        .isInstanceOf(BadHeaderException.class)
        .hasMessage("Not enough header bytes");
  }

  @Test
  public void testByteBufferBadMarkerByte() throws Exception {
    MessageEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA_V2);
    MessageDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));
    buffer.array()[0] = 0x00;

    assertThatThrownBy(() -> decoder.decode(buffer))
        .isInstanceOf(BadHeaderException.class)
        .hasMessageContaining("Unrecognized header bytes");
  }

  @Test
  public void testByteBufferBadVersionByte() throws Exception {
    MessageEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA_V2);
    MessageDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));
    buffer.array()[1] = 0x00;

    assertThatThrownBy(() -> decoder.decode(buffer))
        .isInstanceOf(BadHeaderException.class)
        .hasMessageContaining("Unrecognized header bytes");
  }

  @Test
  public void testByteBufferUnknownSchema() throws Exception {
    MessageEncoder<Record> encoder = new IcebergEncoder<>(SCHEMA_V2);
    MessageDecoder<Record> decoder = new IcebergDecoder<>(SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));
    buffer.array()[4] = 0x00;

    assertThatThrownBy(() -> decoder.decode(buffer))
        .isInstanceOf(MissingSchemaException.class)
        .hasMessageContaining("Cannot resolve schema for fingerprint");
  }
}
