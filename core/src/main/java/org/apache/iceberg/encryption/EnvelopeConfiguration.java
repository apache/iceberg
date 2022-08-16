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
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Configuration parameters for standard envelope encryption (where information is encrypted with a
 * "data encryption key" - DEK, and the dek is encrypted with a "key encryption key" - KEK).
 * Currently, supports only basic (single) envelope encryption and one KEK per table. Future
 * versions will add support for double envelope encryption (where KEKs are encrypted with a "master
 * encryption key"), and for multiple keys per table (column keys).
 */
public class EnvelopeConfiguration implements Serializable {

  private final String kekId;
  private final EncryptionAlgorithm algorithm;

  private EnvelopeConfiguration(String kekId, EncryptionAlgorithm algorithm) {
    Preconditions.checkNotNull(
        kekId, "Cannot construct envelope config because KEK ID is not specified");
    this.kekId = kekId;

    Preconditions.checkNotNull(
        algorithm,
        "Cannot construct envelope config because encryption algorithm is not specified");
    this.algorithm = algorithm;
  }

  public String kekId() {
    return kekId;
  }

  public EncryptionAlgorithm algorithm() {
    return algorithm;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EnvelopeConfiguration config = (EnvelopeConfiguration) o;
    return Objects.equals(kekId, config.kekId) && algorithm == config.algorithm;
  }

  @Override
  public int hashCode() {
    return Objects.hash(kekId, algorithm);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("kekId", kekId)
        .add("algorithm", algorithm)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** A builder used to create valid {@link EnvelopeConfiguration}. */
  public static class Builder {
    private String kekId;
    private EncryptionAlgorithm algorithm;

    /**
     * Sets the kekId for a basic (single) envelope encryption, where a DEK is wrapped once
     *
     * @param keyId ID of the wrapping "key encryption key"
     */
    public Builder singleWrap(String keyId) {
      this.kekId = keyId;
      return this;
    }

    public Builder useAlgorithm(EncryptionAlgorithm encryptionAlgorithm) {
      this.algorithm = encryptionAlgorithm;
      return this;
    }

    public EnvelopeConfiguration build() {
      return new EnvelopeConfiguration(kekId, algorithm);
    }
  }
}
