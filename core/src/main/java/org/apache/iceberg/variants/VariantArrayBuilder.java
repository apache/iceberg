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

public class VariantArrayBuilder extends VariantBuilderBase {
  private final List<Integer> offsets;

  public VariantArrayBuilder(ByteBufferWrapper valueBuffer, Dictionary dict) {
    super(valueBuffer, dict);
    offsets = Lists.newArrayList();
  }

  public VariantObjectBuilder startObject() {
    addOffset();
    return new VariantObjectBuilder(valueBuffer, dict);
  }

  public VariantArrayBuilder startArray() {
    addOffset();
    return new VariantArrayBuilder(valueBuffer, dict);
  }

  public VariantArrayBuilder writeNull() {
    addOffset();
    writeNullInternal();
    return this;
  }

  public VariantArrayBuilder writeBoolean(boolean value) {
    addOffset();
    writeBooleanInternal(value);
    return this;
  }

  public VariantArrayBuilder writeNumeric(long value) {
    addOffset();
    writeNumericInternal(value);
    return this;
  }

  public VariantArrayBuilder writeDouble(double value) {
    addOffset();
    writeDoubleInternal(value);
    return this;
  }

  public VariantArrayBuilder writeDecimal(BigDecimal value) {
    addOffset();
    writeDecimalInternal(value);
    return this;
  }

  public VariantArrayBuilder writeDate(LocalDate value) {
    addOffset();
    writeDateInternal(DateTimeUtil.daysFromDate(value));
    return this;
  }

  public VariantArrayBuilder writeTimestampTz(OffsetDateTime value) {
    addOffset();
    writeTimestampTzInternal(DateTimeUtil.microsFromTimestamptz(value));
    return this;
  }

  public VariantArrayBuilder writeTimestampNtz(LocalDateTime value) {
    addOffset();
    writeTimestampNtzInternal(DateTimeUtil.microsFromTimestamp(value));
    return this;
  }

  public VariantArrayBuilder writeFloat(float value) {
    addOffset();
    writeFloatInternal(value);
    return this;
  }

  public VariantArrayBuilder writeBinary(byte[] value) {
    addOffset();
    writeBinaryInternal(value);
    return this;
  }

  public VariantArrayBuilder writeString(String str) {
    addOffset();
    writeStringInternal(str);
    return this;
  }

  private void addOffset() {
    offsets.add(valueBuffer.pos() - startPos);
  }

  public void endArray() {
    super.endArray(startPos, offsets);
  }
}
