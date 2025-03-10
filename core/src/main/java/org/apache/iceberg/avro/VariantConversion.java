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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;

public class VariantConversion extends Conversion<Variant> {
  @Override
  public Class<Variant> getConvertedType() {
    return Variant.class;
  }

  @Override
  public String getLogicalTypeName() {
    return VariantLogicalType.NAME;
  }

  @Override
  public Variant fromRecord(IndexedRecord record, Schema schema, LogicalType type) {
    int metadataPos = schema.getField("metadata").pos();
    int valuePos = schema.getField("value").pos();
    VariantMetadata metadata = VariantMetadata.from((ByteBuffer) record.get(metadataPos));
    VariantValue value = VariantValue.from(metadata, (ByteBuffer) record.get(valuePos));
    return Variant.of(metadata, value);
  }

  @Override
  public IndexedRecord toRecord(Variant variant, Schema schema, LogicalType type) {
    int metadataPos = schema.getField("metadata").pos();
    int valuePos = schema.getField("value").pos();
    GenericRecord record = new GenericData.Record(schema);
    ByteBuffer metadataBuffer =
        ByteBuffer.allocate(variant.metadata().sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    variant.metadata().writeTo(metadataBuffer, 0);
    record.put(metadataPos, metadataBuffer);
    ByteBuffer valueBuffer =
        ByteBuffer.allocate(variant.value().sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    variant.value().writeTo(valueBuffer, 0);
    record.put(valuePos, valueBuffer);
    return record;
  }
}
