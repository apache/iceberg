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
package org.apache.iceberg.variants;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.DateTimeUtil;

public class VariantObjectBuilder extends VariantBuilderBase {
  private final List<FieldEntry> fields;

  VariantObjectBuilder(ByteBufferWrapper valueBuffer, Dictionary dict) {
    super(valueBuffer, dict);
    fields = Lists.newArrayList();
  }

  public VariantObjectBuilder startObject(String key) {
    writeKey(key);
    return new VariantObjectBuilder(valueBuffer(), dict());
  }

  public VariantArrayBuilder startArray(String key) {
    writeKey(key);
    return new VariantArrayBuilder(valueBuffer(), dict());
  }

  private void writeKey(String key) {
    int id = dict().add(key);
    fields.add(new FieldEntry(key, id, valueBuffer().pos() - startPos()));
  }

  public VariantObjectBuilder writeNull(String key) {
    writeKey(key);
    writeNullInternal();
    return this;
  }

  public VariantObjectBuilder writeBoolean(String key, boolean value) {
    writeKey(key);
    writeBooleanInternal(value);
    return this;
  }

  public VariantObjectBuilder writeIntegral(String key, long value) {
    writeKey(key);
    writeIntegralInternal(value);
    return this;
  }

  public VariantObjectBuilder writeDouble(String key, double value) {
    writeKey(key);
    writeDoubleInternal(value);
    return this;
  }

  public VariantObjectBuilder writeDecimal(String key, BigDecimal value) {
    writeKey(key);
    writeDecimalInternal(value);
    return this;
  }

  public VariantObjectBuilder writeDate(String key, LocalDate value) {
    writeKey(key);
    writeDateInternal(DateTimeUtil.daysFromDate(value));
    return this;
  }

  public VariantObjectBuilder writeTimestampTz(String key, OffsetDateTime value) {
    writeKey(key);
    writeTimestampTzInternal(DateTimeUtil.microsFromTimestamptz(value));
    return this;
  }

  public VariantObjectBuilder writeTimestampNtz(String key, LocalDateTime value) {
    writeKey(key);
    writeTimestampNtzInternal(DateTimeUtil.microsFromTimestamp(value));
    return this;
  }

  public VariantObjectBuilder writeFloat(String key, float value) {
    writeKey(key);
    writeFloatInternal(value);
    return this;
  }

  public VariantObjectBuilder writeBinary(String key, byte[] value) {
    writeKey(key);
    writeBinaryInternal(value);
    return this;
  }

  public VariantObjectBuilder writeString(String key, String value) {
    writeKey(key);
    writeStringInternal(value);
    return this;
  }

  public void endObject() {
    super.endObject(startPos(), fields);
  }
}
