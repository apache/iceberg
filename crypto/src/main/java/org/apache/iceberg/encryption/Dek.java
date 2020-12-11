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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import org.apache.iceberg.util.conf.Conf;
import org.apache.iceberg.util.conf.Dumpable;
import org.apache.iceberg.util.conf.LoadableBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dek implements Serializable, Dumpable {
  private static final Logger log = LoggerFactory.getLogger(Dek.class);
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String ENCRYPTED_DEK = "encodedDek";

  public static final String IV = "encodedIv";

  /** * PRIVATE VARIABLES ** */
  private final byte[] encryptedDek;

  private transient byte[] plaintextDek;
  private final byte[] iv;

  /** * CONSTRUCTOR ** */
  public Dek(byte[] encryptedDek, byte[] plaintextDek, byte[] iv) {
    this.encryptedDek = encryptedDek;
    this.plaintextDek = plaintextDek;
    this.iv = iv;
  }

  public Dek(byte[] encryptedDek, byte[] iv) {
    this.encryptedDek = encryptedDek;
    this.plaintextDek = null;
    this.iv = iv;
  }

  public static Dek load(Conf conf) {
    return new Dek.Builder().load(conf).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements LoadableBuilder<Builder, Dek> {
    private byte[] encryptedDek;
    private byte[] plaintextDek;
    private byte[] iv;

    public Builder setEncryptedDek(byte[] encryptedDek) {
      this.encryptedDek = encryptedDek;
      return this;
    }

    public Builder setPlaintextDek(byte[] plaintextDek) {
      this.plaintextDek = plaintextDek;
      return this;
    }

    public Builder setIv(byte[] iv) {
      this.iv = iv;
      return this;
    }

    @Override
    public Builder load(Conf conf) {
      Base64.Decoder decoder = Base64.getDecoder();
      encryptedDek =
          decoder.decode(conf.propertyAsString(ENCRYPTED_DEK).getBytes(StandardCharsets.UTF_8));
      iv = decoder.decode(conf.propertyAsString(IV));
      return this;
    }

    @Override
    public Dek build() {
      return new Dek(encryptedDek, plaintextDek, iv);
    }
  }

  /** * DUMPER ** */
  @Override
  public void dump(Conf conf) {
    Base64.Encoder encoder = Base64.getEncoder();
    conf.setString(ENCRYPTED_DEK, new String(encoder.encode(encryptedDek), StandardCharsets.UTF_8));
    conf.setString(IV, new String(encoder.encode(iv), StandardCharsets.UTF_8));
  }

  /** * GETTERS ** */
  public byte[] encryptedDek() {
    return encryptedDek;
  }

  public byte[] iv() {
    return iv;
  }

  public byte[] plaintextDek() {
    return plaintextDek;
  }

  public void setPlaintextDek(byte[] plaintextDek) {
    this.plaintextDek = plaintextDek;
  }

  /** EQUALS HASH CODE WARNING: plaintext dek is not checked in equals and hashcode */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (this.getClass() != o.getClass()) {
      return false;
    }
    Dek dek = (Dek) o;
    return Arrays.equals(encryptedDek, dek.encryptedDek) && Arrays.equals(iv, dek.iv);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(encryptedDek);
    result = 31 * result + Arrays.hashCode(iv);
    return result;
  }

  /** TO STRING WARNING: plaintext dek should be redacted */
  @Override
  public String toString() {
    return "Dek{" +
        "encryptedDek=" +
        "REDACTED" +
        ", plaintextDek=" +
        "REDACTED" +
        ", iv=" +
        Arrays.toString(iv) + '}';
  }
}
