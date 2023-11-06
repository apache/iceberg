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
package org.apache.iceberg.connect.events;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.avro.DecoderResolver;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

/** Class for Avro-related utility methods. */
class AvroUtil {
  static final Map<Integer, String> FIELD_ID_TO_CLASS =
      ImmutableMap.of(
          DataComplete.ASSIGNMENTS_ELEMENT,
          TopicPartitionOffset.class.getName(),
          DataFile.PARTITION_ID,
          PartitionData.class.getName(),
          DataWritten.TABLE_REFERENCE,
          TableReference.class.getName(),
          DataWritten.DATA_FILES_ELEMENT,
          "org.apache.iceberg.GenericDataFile",
          DataWritten.DELETE_FILES_ELEMENT,
          "org.apache.iceberg.GenericDeleteFile",
          CommitToTable.TABLE_REFERENCE,
          TableReference.class.getName());

  public static byte[] encode(Event event) {
    try {
      return AvroEncoderUtil.encode(event, event.getSchema());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Event decode(byte[] bytes) {
    try {
      Event event = AvroEncoderUtil.decode(bytes);
      // clear the cache to avoid memory leak
      DecoderResolver.clearCache();
      return event;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static Schema convert(Types.StructType icebergSchema, Class<? extends IndexedRecord> javaClass) {
    return convert(icebergSchema, javaClass, FIELD_ID_TO_CLASS);
  }

  static Schema convert(
      Types.StructType icebergSchema,
      Class<? extends IndexedRecord> javaClass,
      Map<Integer, String> typeMap) {
    return AvroSchemaUtil.convert(
        icebergSchema,
        (fieldId, struct) ->
            struct.equals(icebergSchema) ? javaClass.getName() : typeMap.get(fieldId));
  }

  static int positionToId(int position, Schema avroSchema) {
    List<Schema.Field> fields = avroSchema.getFields();
    Preconditions.checkArgument(
        position >= 0 && position < fields.size(), "Invalid field position: " + position);
    Object val = fields.get(position).getObjectProp(AvroSchemaUtil.FIELD_ID_PROP);
    return val == null ? -1 : (int) val;
  }

  private AvroUtil() {}
}
