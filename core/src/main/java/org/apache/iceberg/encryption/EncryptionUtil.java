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
import org.apache.iceberg.common.DynConstructors;

public class EncryptionUtil {

  private EncryptionUtil() {}

  public static KeyMetadata parseKeyMetadata(ByteBuffer metadataBuffer) {
    return KeyMetadata.parse(metadataBuffer);
  }

  public static EncryptionKeyMetadata createKeyMetadata(ByteBuffer key, ByteBuffer aadPrefix) {
    return new KeyMetadata(key, aadPrefix);
  }

  static KeyManagementClient createKmsClient(String kmsImpl) {
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
      return ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize kms client, %s does not implement KeyManagementClient interface",
              kmsImpl),
          e);
    }
  }

  public static boolean useNativeEncryption(EncryptionKeyMetadata keyMetadata) {
    return keyMetadata != null && keyMetadata instanceof KeyMetadata;
  }
}
