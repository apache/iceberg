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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import org.junit.jupiter.api.Test;

public class TestCiphers {

  @Test
  public void testBasicEncrypt() {
    testEncryptDecrypt(null, true, false, false);
  }

  @Test
  public void testAAD() {
    byte[] aad = "abcd".getBytes(StandardCharsets.UTF_8);
    testEncryptDecrypt(aad, true, false, false);
  }

  @Test
  public void testBadAAD() {
    byte[] aad = "abcd".getBytes(StandardCharsets.UTF_8);
    testEncryptDecrypt(aad, false, true, false);
  }

  @Test
  public void testContentCorruption() {
    byte[] aad = "abcd".getBytes(StandardCharsets.UTF_8);
    testEncryptDecrypt(aad, false, false, true);
  }

  private void testEncryptDecrypt(
      byte[] aad, boolean testDecrypt, boolean testBadAad, boolean testCorruption) {
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

      if (testDecrypt) {
        byte[] decryptedText = decryptor.decrypt(ciphertext, aad);
        assertThat(decryptedText).as("Key length " + keyLength).isEqualTo(plaintext);
      }

      if (testBadAad) {
        final byte[] badAad = (aad == null) ? new byte[1] : aad;
        badAad[0]++;

        assertThatThrownBy(() -> decryptor.decrypt(ciphertext, badAad))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("GCM tag check failed");
      }

      if (testCorruption) {
        ciphertext[ciphertext.length / 2]++;

        assertThatThrownBy(() -> decryptor.decrypt(ciphertext, aad))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("GCM tag check failed");
      }
    }
  }
}
