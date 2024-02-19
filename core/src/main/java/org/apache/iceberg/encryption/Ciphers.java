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
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class Ciphers {
  public static final int PLAIN_BLOCK_SIZE = 1024 * 1024;
  public static final int NONCE_LENGTH = 12;
  public static final int GCM_TAG_LENGTH = 16;
  public static final int BLOCK_SIZE_DELTA = NONCE_LENGTH + GCM_TAG_LENGTH;
  public static final int CIPHER_BLOCK_SIZE = PLAIN_BLOCK_SIZE + BLOCK_SIZE_DELTA;
  public static final String GCM_STREAM_MAGIC_STRING = "AGS1";

  static final byte[] GCM_STREAM_MAGIC_ARRAY =
      GCM_STREAM_MAGIC_STRING.getBytes(StandardCharsets.UTF_8);
  static final ByteBuffer GCM_STREAM_MAGIC =
      ByteBuffer.wrap(GCM_STREAM_MAGIC_ARRAY).asReadOnlyBuffer();
  static final int GCM_STREAM_HEADER_LENGTH =
      GCM_STREAM_MAGIC_ARRAY.length + 4; // magic_len + block_size_len

  private static final int GCM_TAG_LENGTH_BITS = 8 * GCM_TAG_LENGTH;

  static final int MIN_STREAM_LENGTH = GCM_STREAM_HEADER_LENGTH + NONCE_LENGTH + GCM_TAG_LENGTH;

  private Ciphers() {}

  public static class AesGcmEncryptor {
    private final SecretKeySpec aesKey;
    private final Cipher cipher;
    private final SecureRandom randomGenerator;
    private final byte[] nonce;

    public AesGcmEncryptor(byte[] keyBytes) {
      this.aesKey = newKey(keyBytes);
      this.cipher = newCipher();

      this.randomGenerator = new SecureRandom();
      this.nonce = new byte[NONCE_LENGTH];
    }

    public byte[] encrypt(byte[] plaintext, byte[] aad) {
      return encrypt(plaintext, 0, plaintext.length, aad);
    }

    public byte[] encrypt(byte[] plaintext, int plaintextOffset, int plaintextLength, byte[] aad) {
      int cipherTextLength = plaintextLength + BLOCK_SIZE_DELTA;
      byte[] cipherText = new byte[cipherTextLength];
      encrypt(plaintext, plaintextOffset, plaintextLength, cipherText, 0, aad);
      return cipherText;
    }

    public int encrypt(
        byte[] plaintext,
        int plaintextOffset,
        int plaintextLength,
        byte[] ciphertextBuffer,
        int ciphertextOffset,
        byte[] aad) {
      Preconditions.checkArgument(
          plaintextLength >= 0, "Invalid plain text length: %s", plaintextLength);
      randomGenerator.nextBytes(nonce);
      int enciphered;

      try {
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);
        if (null != aad) {
          cipher.updateAAD(aad);
        }

        // doFinal encrypts and adds a GCM tag. The nonce is added later.
        enciphered =
            cipher.doFinal(
                plaintext,
                plaintextOffset,
                plaintextLength,
                ciphertextBuffer,
                ciphertextOffset + NONCE_LENGTH);

        if (enciphered != plaintextLength + GCM_TAG_LENGTH) {
          throw new RuntimeException(
              "Failed to encrypt block: expected "
                  + plaintextLength
                  + GCM_TAG_LENGTH
                  + " encrypted bytes but produced bytes "
                  + enciphered);
        }
      } catch (GeneralSecurityException e) {
        throw new RuntimeException("Failed to encrypt", e);
      }

      // Add the nonce
      System.arraycopy(nonce, 0, ciphertextBuffer, ciphertextOffset, NONCE_LENGTH);

      return enciphered + NONCE_LENGTH;
    }
  }

  public static class AesGcmDecryptor {
    private final SecretKeySpec aesKey;
    private final Cipher cipher;

    public AesGcmDecryptor(byte[] keyBytes) {
      this.aesKey = newKey(keyBytes);
      this.cipher = newCipher();
    }

    public byte[] decrypt(byte[] ciphertext, byte[] aad) {
      return decrypt(ciphertext, 0, ciphertext.length, aad);
    }

    public byte[] decrypt(
        byte[] ciphertext, int ciphertextOffset, int ciphertextLength, byte[] aad) {
      int plaintextLength = ciphertextLength - BLOCK_SIZE_DELTA;
      byte[] plaintext = new byte[plaintextLength];
      decrypt(ciphertext, ciphertextOffset, ciphertextLength, plaintext, 0, aad);
      return plaintext;
    }

    public int decrypt(
        byte[] ciphertext,
        int ciphertextOffset,
        int ciphertextLength,
        byte[] plaintextBuffer,
        int plaintextOffset,
        byte[] aad) {
      Preconditions.checkState(
          ciphertextLength - BLOCK_SIZE_DELTA >= 0,
          "Cannot decrypt cipher text of length "
              + ciphertextLength
              + " because text must longer than GCM_TAG_LENGTH + NONCE_LENGTH bytes. Text may not be encrypted"
              + " with AES GCM cipher");
      int plaintextLength;

      try {
        GCMParameterSpec spec =
            new GCMParameterSpec(GCM_TAG_LENGTH_BITS, ciphertext, ciphertextOffset, NONCE_LENGTH);
        cipher.init(Cipher.DECRYPT_MODE, aesKey, spec);
        if (null != aad) {
          cipher.updateAAD(aad);
        }
        // For java Cipher, the nonce is not part of ciphertext
        plaintextLength =
            cipher.doFinal(
                ciphertext,
                ciphertextOffset + NONCE_LENGTH,
                ciphertextLength - NONCE_LENGTH,
                plaintextBuffer,
                plaintextOffset);
      } catch (AEADBadTagException e) {
        throw new RuntimeException(
            "GCM tag check failed. Possible reasons: wrong decryption key; or corrupt/tampered"
                + " data. AES GCM doesn't differentiate between these two.",
            e);
      } catch (GeneralSecurityException e) {
        throw new RuntimeException("Failed to decrypt", e);
      }

      return plaintextLength;
    }
  }

  private static SecretKeySpec newKey(byte[] keyBytes) {
    Preconditions.checkArgument(keyBytes != null, "Invalid key: null");
    int keyLength = keyBytes.length;
    Preconditions.checkArgument(
        (keyLength == 16 || keyLength == 24 || keyLength == 32),
        "Invalid key length: %s (must be 16, 24, or 32 bytes)",
        keyLength);
    return new SecretKeySpec(keyBytes, "AES");
  }

  private static Cipher newCipher() {
    try {
      return Cipher.getInstance("AES/GCM/NoPadding");
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Failed to create GCM cipher", e);
    }
  }

  static byte[] streamBlockAAD(byte[] fileAadPrefix, int currentBlockIndex) {
    byte[] blockAAD =
        ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(currentBlockIndex).array();

    if (null == fileAadPrefix) {
      return blockAAD;
    } else {
      byte[] aad = new byte[fileAadPrefix.length + 4];
      System.arraycopy(fileAadPrefix, 0, aad, 0, fileAadPrefix.length);
      System.arraycopy(blockAAD, 0, aad, fileAadPrefix.length, 4);
      return aad;
    }
  }
}
