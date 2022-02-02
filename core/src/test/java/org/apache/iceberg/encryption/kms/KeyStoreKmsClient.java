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

package org.apache.iceberg.encryption.kms;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.SecretKey;

/**
 * KMS client demo class, based on the Java KeyStore API that reads keys from standard PKCS12 keystore files
 */
public class KeyStoreKmsClient extends MemoryMockKMS {

  public static final String KEY_FILE_PATH_PROP = "keystore.kms.client.file.path";
  public static final String KEY_FILE_PASSWORD_PROP = "keystore.kms.client.file.password";

  @Override
  public String wrapKey(ByteBuffer key, String wrappingKeyId) {
    // keytool keeps key names in lower case
    return super.wrapKey(key, wrappingKeyId.toLowerCase());
  }

  @Override
  public ByteBuffer unwrapKey(String wrappedKey, String wrappingKeyId) {
    // keytool keeps key names in lower case
    return super.unwrapKey(wrappedKey, wrappingKeyId.toLowerCase());
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

    masterKeys = new HashMap<>();
    while (keyAliases.hasMoreElements()) {
      String keyAlias = keyAliases.nextElement();
      SecretKey secretKey;
      try {
        secretKey = (SecretKey) keyStore.getKey(keyAlias, pwdArray);
      } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
        throw new RuntimeException("Failed to get key " + keyAlias, e);
      }

      masterKeys.put(keyAlias, secretKey.getEncoded());
    }

    if (masterKeys.isEmpty()) {
      throw new RuntimeException("No keys found in " + keyFilePath);
    }
  }
}
