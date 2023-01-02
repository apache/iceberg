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
package org.apache.iceberg.encryption;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecordBuilderBase;

public class AvroKeyRecord extends SpecificRecordBase {
  public static final org.apache.avro.Schema SCHEMA$ =
      SchemaBuilder.record("AvroKeyRecord")
          .namespace("iceberg.encryption")
          .fields()
          .requiredBytes("encryptionKey")
          .optionalString("wrappingKeyId")
          .optionalBytes("aadPrefix")
          .endRecord();
  private ByteBuffer encryptionKey;
  private CharSequence wrappingKeyId;
  private ByteBuffer aadPrefix;

  private AvroKeyRecord() {}

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field) {
    switch (field) {
      case 0:
        return encryptionKey;
      case 1:
        return wrappingKeyId;
      case 2:
        return aadPrefix;
      default:
        throw new AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value = "unchecked")
  public void put(int field, Object value) {
    switch (field) {
      case 0:
        encryptionKey = (ByteBuffer) value;
        break;
      case 1:
        wrappingKeyId = (CharSequence) value;
        break;
      case 2:
        aadPrefix = (ByteBuffer) value;
        break;
      default:
        throw new AvroRuntimeException("Bad index");
    }
  }

  public ByteBuffer getEncryptionKey() {
    return encryptionKey;
  }

  public CharSequence getWrappingKeyId() {
    return wrappingKeyId;
  }

  public ByteBuffer getAadPrefix() {
    return aadPrefix;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends SpecificRecordBuilderBase<AvroKeyRecord> {
    private ByteBuffer encryptionKey;
    private CharSequence wrappingKeyId;
    private ByteBuffer aadPrefix;

    private Builder() {
      super(SCHEMA$);
    }

    public Builder setEncryptionKey(ByteBuffer value) {
      validate(fields()[0], value);
      this.encryptionKey = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    public Builder setWrappingKeyId(CharSequence value) {
      validate(fields()[1], value);
      this.wrappingKeyId = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    public Builder setAadPrefix(ByteBuffer value) {
      validate(fields()[2], value);
      this.aadPrefix = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    @Override
    public AvroKeyRecord build() {
      try {
        AvroKeyRecord record = new AvroKeyRecord();
        record.encryptionKey =
            fieldSetFlags()[0] ? this.encryptionKey : (ByteBuffer) defaultValue(fields()[0]);
        record.wrappingKeyId =
            fieldSetFlags()[1] ? this.wrappingKeyId : (CharSequence) defaultValue(fields()[1]);
        record.aadPrefix =
            fieldSetFlags()[2] ? this.aadPrefix : (ByteBuffer) defaultValue(fields()[2]);
        return record;
      } catch (IOException e) {
        throw new AvroRuntimeException(e);
      }
    }
  }
}
