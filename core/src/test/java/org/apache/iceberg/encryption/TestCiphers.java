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

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import org.junit.Assert;
import org.junit.Test;

public class TestCiphers {

  @Test
  public void testBasicEncrypt() {
    testEncryptDecrypt(null);
  }

  @Test
  public void testAAD() {
    byte[] aad = "abcd".getBytes(StandardCharsets.UTF_8);
    testEncryptDecrypt(aad);
  }

  private void testEncryptDecrypt(byte[] aad) {
    SecureRandom random = new SecureRandom();
    int[] aesKeyLengthArray = {16, 24, 32};
    for (int keyLength : aesKeyLengthArray) {
      byte[] key = new byte[keyLength];
      random.nextBytes(key);
      Ciphers.AesGcmEncryptor encryptor = new Ciphers.AesGcmEncryptor(key);
      byte[] plaintext = new byte[16]; // typically used to encrypt DEKs
      random.nextBytes(plaintext);
      byte[] ciphertext = encryptor.encrypt(plaintext, aad);

      Ciphers.AesGcmDecryptor decryptor = new Ciphers.AesGcmDecryptor(key);
      byte[] decryptedText = decryptor.decrypt(ciphertext, aad);
      Assert.assertArrayEquals("Key length " + keyLength, plaintext, decryptedText);
    }
  }
}
