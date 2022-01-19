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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Barebone encryption parameters, one object per content file.
 * Carries the file encryption key (and optional AAD prefix, column keys).
 */
public class NativeFileCryptoParameters {
  private ByteBuffer fileAadPrefix;
  private Map<String, ByteBuffer> columnKeys;
  private ByteBuffer fileKey;
  private String fileEncryptionAlgorithm;

  private NativeFileCryptoParameters(Map<String, ByteBuffer> columnKeys, ByteBuffer fileKey,
                                     ByteBuffer fileAadPrefix, String fileEncryptionAlgorithm) {
    Preconditions.checkState((columnKeys != null && columnKeys.size() > 0) || fileKey != null,
            "No file or column keys are supplied");
    this.columnKeys = columnKeys;
    this.fileKey = fileKey;
    this.fileAadPrefix = fileAadPrefix;
    this.fileEncryptionAlgorithm = fileEncryptionAlgorithm;
  }

  /**
   * Creates the builder.
   * @param fileKey per-file encryption key. For example, used as "footer key" DEK in Parquet encryption.
   */
  public static Builder create(ByteBuffer fileKey) {
    return new Builder(fileKey);
  }

  public static class Builder {
    private ByteBuffer fileAadPrefix;
    private Map<String, ByteBuffer> columnKeys;
    private ByteBuffer fileKey;
    private String fileEncryptionAlgorithm;

    private Builder(ByteBuffer fileKey) {
      this.fileKey = fileKey;
    }

    /**
     * Set column encryption keys.
     * @param columnKeyMap Map of column names to column keys. Column names must be the original names,
     *                     used during content file creation. For example, Parquet will use them to find and
     *                     encrypt the relevant columns.
     */
    public Builder columnKeys(Map<String, ByteBuffer> columnKeyMap) {
      this.columnKeys = columnKeyMap;
      return this;
    }

    public Builder aadPrefix(ByteBuffer aadPrefix) {
      this.fileAadPrefix = aadPrefix;
      return this;
    }

    public Builder encryptionAlgorithm(String encryptionAlgorithm) {
      this.fileEncryptionAlgorithm = encryptionAlgorithm;
      return this;
    }

    public NativeFileCryptoParameters build() {
      return new NativeFileCryptoParameters(columnKeys, fileKey, fileAadPrefix, fileEncryptionAlgorithm);
    }
  }

  public ByteBuffer aadPrefix() {
    return fileAadPrefix;
  }

  public ByteBuffer fileKey() {
    return fileKey;
  }

  public Map<String, ByteBuffer> columnKeys() {
    return columnKeys;
  }

  public String encryptionAlgorithm() {
    return fileEncryptionAlgorithm;
  }
}
