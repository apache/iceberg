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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class EncryptionUtil {

  private EncryptionUtil() {}

  public static KeyManagementClient createKmsClient(Map<String, String> catalogProperties) {
    String kmsType = catalogProperties.get(CatalogProperties.ENCRYPTION_KMS_TYPE);
    String kmsImpl = catalogProperties.get(CatalogProperties.ENCRYPTION_KMS_IMPL);

    Preconditions.checkArgument(
        kmsType == null || kmsImpl == null,
        "Cannot set both KMS type (%s) and KMS impl (%s)",
        kmsType,
        kmsImpl);

    // TODO: Add KMS implementations
    Preconditions.checkArgument(kmsType == null, "Unsupported KMS type: %s", kmsType);

    KeyManagementClient kmsClient;
    DynConstructors.Ctor<KeyManagementClient> ctor;
    try {
      ctor = DynConstructors.builder(KeyManagementClient.class).impl(kmsImpl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize KeyManagementClient, missing no-arg constructor for class %s",
              kmsImpl),
          e);
    }

    try {
      kmsClient = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize kms client, %s does not implement KeyManagementClient interface",
              kmsImpl),
          e);
    }

    kmsClient.initialize(catalogProperties);

    return kmsClient;
  }

  public static EncryptionManager createEncryptionManager(
      String tableKeyId, int dekLength, KeyManagementClient kmsClient) {
    Preconditions.checkArgument(kmsClient != null, "Invalid KMS client: null");

    if (null == tableKeyId) {
      // Unencrypted table
      return PlaintextEncryptionManager.instance();
    }

    Preconditions.checkState(
        dekLength == 16 || dekLength == 24 || dekLength == 32,
        "Invalid data key length: %s (must be 16, 24, or 32)",
        dekLength);

    return new StandardEncryptionManager(tableKeyId, dekLength, kmsClient);
  }

  public static EncryptedOutputFile plainAsEncryptedOutput(OutputFile encryptingOutputFile) {
    return new BaseEncryptedOutputFile(encryptingOutputFile, EncryptionKeyMetadata.empty());
  }

  public static EncryptionKeyMetadata createKeyMetadata(ByteBuffer key, ByteBuffer aadPrefix) {
    Preconditions.checkState(
        key.arrayOffset() == 0, "Invalid key array offset {}", key.arrayOffset());
    Preconditions.checkState(
        aadPrefix.arrayOffset() == 0, "Invalid aad array offset {}", aadPrefix.arrayOffset());
    return new StandardKeyMetadata(key.array(), aadPrefix.array());
  }

  public static NativeEncryptionKeyMetadata parseKeyMetadata(ByteBuffer keyMetadataBytes) {
    return StandardKeyMetadata.parse(keyMetadataBytes);
  }
}
