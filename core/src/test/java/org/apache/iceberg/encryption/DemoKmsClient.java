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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Base64;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.SecretKey;

public class DemoKmsClient implements KmsClient {

  public static final String KEY_FILE_PATH_PROP = "demo.kms.client.keyfile.path";
  public static final String KEY_FILE_PASSWORD_PROP = "demo.kms.client.keyfile.password";

  private Map<String, byte[]> keys;

  @Override
  public String wrapKey(ByteBuffer key, String wrappingKeyId, Map<String, String> context) {
    // keytool keeps key names in lower case
    byte[] wrappingKey = keys.get(wrappingKeyId.toLowerCase());
    if (null == wrappingKey) {
      throw new RuntimeException("Key " + wrappingKeyId + " is not found");
    }
    Ciphers.AesGcmEncryptor keyEncryptor = new Ciphers.AesGcmEncryptor(wrappingKey);
    byte[] encryptedKey = keyEncryptor.encrypt(key.array());
    return Base64.getEncoder().encodeToString(encryptedKey);
  }

  @Override
  public boolean supportsKeyGeneration() {
    return false;
  }

  @Override
  public KeyGenerationResult generateKey(String keyId, Map<String, String> context) {
    return null;
  }

  @Override
  public ByteBuffer unwrapKey(String wrappedKey, String wrappingKeyId, Map<String, String> context) {
    byte[] encryptedKey = Base64.getDecoder().decode(wrappedKey);
    // keytool keeps key names in lower case
    byte[] wrappingKey = keys.get(wrappingKeyId.toLowerCase());
    if (null == wrappingKey) {
      throw new RuntimeException("Key " + wrappingKeyId + " is not found");
    }
    Ciphers.AesGcmDecryptor keyDecryptor = new Ciphers.AesGcmDecryptor(wrappingKey);
    byte[] key = keyDecryptor.decrypt(encryptedKey);
    return ByteBuffer.wrap(key);
  }

  @Override
  public KeyGenerationAlgorithm keyGenerationAlgorithm() {
    return null;
  }

  @Override
  public KeySpec keySpec() {
    return null;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    String keyFilePath = properties.get(KEY_FILE_PATH_PROP);
    if (null == keyFilePath) {
      throw new RuntimeException("Property " + KEY_FILE_PATH_PROP + " is not found");
    }
    String keyFilePassword = properties.get(KEY_FILE_PASSWORD_PROP);
    if (null == keyFilePassword) {
      throw new RuntimeException("Property " + KEY_FILE_PASSWORD_PROP + " is not found");
    }

    KeyStore keyStore;
    try {
      keyStore = KeyStore.getInstance("PKCS12");
    } catch (KeyStoreException e) {
      throw new RuntimeException("Failed to init keystore", e);
    }

    char[] pwdArray = keyFilePassword.toCharArray();

    FileInputStream fis;
    try {
      fis = new FileInputStream(keyFilePath);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Failed to find keystore file " + keyFilePath, e);
    }

    try {
      keyStore.load(fis, pwdArray);
    } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
      throw new RuntimeException("Failed to load keystore file " + keyFilePath, e);
    }

    Enumeration<String> keyAliases;
    try {
      keyAliases =  keyStore.aliases();
    } catch (KeyStoreException e) {
      throw new RuntimeException("Failed to get key aliases in keystore file " + keyFilePath, e);
    }

    keys = new HashMap<>();
    while (keyAliases.hasMoreElements()) {
      String keyAlias = keyAliases.nextElement();
      SecretKey secretKey;
      try {
        secretKey = (SecretKey) keyStore.getKey(keyAlias, pwdArray);
      } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
        throw new RuntimeException("Failed to get key " + keyAlias, e);
      }

      keys.put(keyAlias, secretKey.getEncoded());
    }

    if (keys.isEmpty()) {
      throw new RuntimeException("No keys found in " + keyFilePath);
    }
  }
}
