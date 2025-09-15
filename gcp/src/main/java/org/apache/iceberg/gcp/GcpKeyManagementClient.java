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
package org.apache.iceberg.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.kms.v1.DecryptRequest;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptRequest;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.io.CloseableGroup;

/**
 * Key management client implementation that uses Google Cloud Key Management. To be used for
 * encrypting/decrypting keys with a KMS-managed master key (by referencing its key ID)
 *
 * <p>Uses {@link ByteStringShim} to ensure this class works with and without iceberg-gcp-bundle.
 * Since the bundle relocates {@link com.google.protobuf.ByteString}, all related methods need to be
 * loaded dynamically. During runtime if the relocated class is observed, it will be preferred over
 * the original one.
 */
public class GcpKeyManagementClient implements KeyManagementClient {

  private KeyManagementServiceClient kmsClient;
  private CloseableGroup closeableGroup = new CloseableGroup();

  @Override
  public void initialize(Map<String, String> properties) {
    this.closeableGroup = new CloseableGroup();
    closeableGroup.setSuppressCloseFailure(true);

    GCPProperties gcpProperties = new GCPProperties(properties);

    try {
      KeyManagementServiceSettings.Builder kmsBuilder = KeyManagementServiceSettings.newBuilder();
      if (gcpProperties.oauth2Token().isPresent()) {
        OAuth2Credentials oAuth2Credentials =
            GCPAuthUtils.oauth2CredentialsFromGcpProperties(gcpProperties, closeableGroup);
        kmsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(oAuth2Credentials));
      }

      // if not OAuth then defaults to GoogleCredentials.getApplicationDefault()
      this.kmsClient = KeyManagementServiceClient.create(kmsBuilder.build());
      closeableGroup.addCloseable(kmsClient);

    } catch (IOException e) {
      throw new RuntimeException("Failed to create GCP cloud KMS service client", e);
    }
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    EncryptRequest.Builder requestBuilder = EncryptRequest.newBuilder().setName(wrappingKeyId);
    requestBuilder = ByteStringShim.setPlainText(requestBuilder, key);

    EncryptRequest encryptRequest = requestBuilder.build();
    EncryptResponse encryptResponse = kmsClient.encrypt(encryptRequest);

    // need ByteString.copyFrom() leaves the BB in an end position, need to reset
    key.position(0);
    return ByteBuffer.wrap(ByteStringShim.getCipherText(encryptResponse));
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    DecryptRequest.Builder requestBuilder = DecryptRequest.newBuilder().setName(wrappingKeyId);
    requestBuilder = ByteStringShim.setCipherText(requestBuilder, wrappedKey);

    DecryptRequest decryptRequest = requestBuilder.build();
    DecryptResponse decryptResponse = kmsClient.decrypt(decryptRequest);

    // need ByteString.copyFrom() leaves the BB in an end position, need to reset
    wrappedKey.position(0);
    return ByteBuffer.wrap(ByteStringShim.getPlainText(decryptResponse));
  }

  @Override
  public void close() {
    try {
      closeableGroup.close();
    } catch (IOException ioe) {
      // closure exceptions already suppressed and logged in closeableGroup
    }
  }

  private static final class ByteStringShim {
    private static final String ORIGINAL_BYTE_STRING_CLASS_NAME = "com.google.protobuf.ByteString";
    private static final String SHADED_BYTE_STRING_CLASS_NAME =
        "org.apache.iceberg.gcp.shaded." + ORIGINAL_BYTE_STRING_CLASS_NAME;
    private static final Class<?> BYTE_STRING_CLASS;

    static {
      Class<?> byteStringClass =
          DynClasses.builder().impl(SHADED_BYTE_STRING_CLASS_NAME).orNull().build();
      if (byteStringClass == null) {
        byteStringClass = DynClasses.builder().impl(ORIGINAL_BYTE_STRING_CLASS_NAME).build();
      }

      BYTE_STRING_CLASS = byteStringClass;
    }

    private static final DynMethods.UnboundMethod SET_PLAIN_TEXT_METHOD =
        new DynMethods.Builder("setPlaintext")
            .hiddenImpl(EncryptRequest.Builder.class, BYTE_STRING_CLASS)
            .build();
    private static final DynMethods.UnboundMethod SET_CIPHER_TEXT_METHOD =
        new DynMethods.Builder("setCiphertext")
            .hiddenImpl(DecryptRequest.Builder.class, BYTE_STRING_CLASS)
            .build();
    private static final DynMethods.UnboundMethod GET_PLAIN_TEXT_METHOD =
        new DynMethods.Builder("getPlaintext").hiddenImpl(DecryptResponse.class).build();
    private static final DynMethods.UnboundMethod GET_CIPHER_TEXT_METHOD =
        new DynMethods.Builder("getCiphertext").hiddenImpl(EncryptResponse.class).build();
    private static final DynMethods.StaticMethod COPY_FROM_BYTEBUFFER_METHOD =
        new DynMethods.Builder("copyFrom")
            .hiddenImpl(BYTE_STRING_CLASS, ByteBuffer.class)
            .buildStatic();
    private static final DynMethods.UnboundMethod TO_BYTE_ARRAY_METHOD =
        new DynMethods.Builder("toByteArray").hiddenImpl(BYTE_STRING_CLASS).build();

    static EncryptRequest.Builder setPlainText(
        EncryptRequest.Builder requestBuilder, ByteBuffer key) {
      // dynamic call: requestBuilder.setPlaintext(ByteString.copyFrom(key))
      Object byteStringKey = COPY_FROM_BYTEBUFFER_METHOD.invoke(key);
      return SET_PLAIN_TEXT_METHOD.invoke(requestBuilder, byteStringKey);
    }

    static DecryptRequest.Builder setCipherText(
        DecryptRequest.Builder requestBuilder, ByteBuffer wrappedKey) {
      // dynamic call: requestBuilder.setCiphertext(ByteString.copyFrom(wrappedKey))
      Object byteStringWrappedKey = COPY_FROM_BYTEBUFFER_METHOD.invoke(wrappedKey);
      return SET_CIPHER_TEXT_METHOD.invoke(requestBuilder, byteStringWrappedKey);
    }

    static byte[] getCipherText(EncryptResponse response) {
      // dynamic call: response.getCipherText().toByteArray()
      Object byteStringEncryptedKey = GET_CIPHER_TEXT_METHOD.invoke(response);
      return TO_BYTE_ARRAY_METHOD.invoke(byteStringEncryptedKey);
    }

    static byte[] getPlainText(DecryptResponse response) {
      // dynamic call: response.getPlainText().toByteArray()
      Object byteStringDecryptedKey = GET_PLAIN_TEXT_METHOD.invoke(response);
      return TO_BYTE_ARRAY_METHOD.invoke(byteStringDecryptedKey);
    }
  }
}
