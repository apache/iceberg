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
import org.apache.iceberg.util.DateTimeUtil;

public class VariantPrimitiveBuilder extends VariantBuilderBase {
  public VariantPrimitiveBuilder(ByteBufferWrapper valueBuffer, Dictionary dict) {
    super(valueBuffer, dict);
  }

  public VariantPrimitiveBuilder writeNull() {
    writeNullInternal();
    return this;
  }

  public VariantPrimitiveBuilder writeBoolean(boolean value) {
    writeBooleanInternal(value);
    return this;
  }

  public VariantPrimitiveBuilder writeNumeric(long value) {
    writeNumericInternal(value);
    return this;
  }

  public VariantPrimitiveBuilder writeDouble(double value) {
    writeDoubleInternal(value);
    return this;
  }

  public VariantPrimitiveBuilder writeDecimal(BigDecimal value) {
    writeDecimalInternal(value);
    return this;
  }

  public VariantPrimitiveBuilder writeDate(LocalDate value) {
    writeDateInternal(DateTimeUtil.daysFromDate(value));
    return this;
  }

  public VariantPrimitiveBuilder writeTimestampTz(OffsetDateTime value) {
    writeTimestampTzInternal(DateTimeUtil.microsFromTimestamptz(value));
    return this;
  }

  public VariantPrimitiveBuilder writeTimestampNtz(LocalDateTime value) {
    writeTimestampNtzInternal(DateTimeUtil.microsFromTimestamp(value));
    return this;
  }

  public VariantPrimitiveBuilder writeFloat(float value) {
    writeFloatInternal(value);
    return this;
  }

  public VariantPrimitiveBuilder writeBinary(byte[] value) {
    writeBinaryInternal(value);
    return this;
  }

  public VariantPrimitiveBuilder writeString(String value) {
    writeStringInternal(value);
    return this;
  }
}
