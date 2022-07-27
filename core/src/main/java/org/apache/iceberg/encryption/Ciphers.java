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

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class Ciphers {
  private static final int NONCE_LENGTH = 12;
  private static final int GCM_TAG_LENGTH = 16;
  private static final int GCM_TAG_LENGTH_BITS = 8 * GCM_TAG_LENGTH;

  public static class AesGcmEncryptor {
    private final SecretKeySpec aesKey;
    private final Cipher cipher;
    private final SecureRandom randomGenerator;

    public AesGcmEncryptor(byte[] keyBytes) {
      Preconditions.checkArgument(keyBytes != null, "Key can't be null");
      int keyLength = keyBytes.length;
      Preconditions.checkArgument(
          (keyLength == 16 || keyLength == 24 || keyLength == 32),
          "Cannot use a key of length "
              + keyLength
              + " because AES only allows 16, 24 or 32 bytes");
      this.aesKey = new SecretKeySpec(keyBytes, "AES");

      try {
        this.cipher = Cipher.getInstance("AES/GCM/NoPadding");
      } catch (GeneralSecurityException e) {
        throw new RuntimeException("Failed to create GCM cipher", e);
      }

      this.randomGenerator = new SecureRandom();
    }

    public byte[] encrypt(byte[] plainText, byte[] aad) {
      byte[] nonce = new byte[NONCE_LENGTH];
      randomGenerator.nextBytes(nonce);
      int cipherTextLength = NONCE_LENGTH + plainText.length + GCM_TAG_LENGTH;
      byte[] cipherText = new byte[cipherTextLength];

      try {
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);
        if (null != aad) {
          cipher.updateAAD(aad);
        }
        cipher.doFinal(plainText, 0, plainText.length, cipherText, NONCE_LENGTH);
      } catch (GeneralSecurityException e) {
        throw new RuntimeException("Failed to encrypt", e);
      }

      // Add the nonce
      System.arraycopy(nonce, 0, cipherText, 0, NONCE_LENGTH);

      return cipherText;
    }
  }

  public static class AesGcmDecryptor {
    private final SecretKeySpec aesKey;
    private final Cipher cipher;

    public AesGcmDecryptor(byte[] keyBytes) {
      Preconditions.checkArgument(keyBytes != null, "Key can't be null");
      int keyLength = keyBytes.length;
      Preconditions.checkArgument(
          (keyLength == 16 || keyLength == 24 || keyLength == 32),
          "Cannot use a key of length "
              + keyLength
              + " because AES only allows 16, 24 or 32 bytes");
      this.aesKey = new SecretKeySpec(keyBytes, "AES");

      try {
        this.cipher = Cipher.getInstance("AES/GCM/NoPadding");
      } catch (GeneralSecurityException e) {
        throw new RuntimeException("Failed to create GCM cipher", e);
      }
    }

    public byte[] decrypt(byte[] ciphertext, byte[] aad) {
      int plainTextLength = ciphertext.length - GCM_TAG_LENGTH - NONCE_LENGTH;
      Preconditions.checkState(
          plainTextLength >= 1,
          "Cannot decrypt cipher text of length "
              + ciphertext.length
              + " because text must longer than GCM_TAG_LENGTH + NONCE_LENGTH bytes. Text may not be encrypted"
              + " with AES GCM cipher");

      // Get the nonce from ciphertext
      byte[] nonce = new byte[NONCE_LENGTH];
      System.arraycopy(ciphertext, 0, nonce, 0, NONCE_LENGTH);

      byte[] plainText = new byte[plainTextLength];
      int inputLength = ciphertext.length - NONCE_LENGTH;
      try {
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
        cipher.init(Cipher.DECRYPT_MODE, aesKey, spec);
        if (null != aad) {
          cipher.updateAAD(aad);
        }
        cipher.doFinal(ciphertext, NONCE_LENGTH, inputLength, plainText, 0);
      } catch (AEADBadTagException e) {
        throw new RuntimeException(
            "GCM tag check failed. Possible reasons: wrong decryption key; or corrupt/tampered"
                + "data. AES GCM doesn't differentiate between these two.. ",
            e);
      } catch (GeneralSecurityException e) {
        throw new RuntimeException("Failed to decrypt", e);
      }

      return plainText;
    }
  }
}
