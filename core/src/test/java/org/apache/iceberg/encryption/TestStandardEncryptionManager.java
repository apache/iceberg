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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestStandardEncryptionManager {

  @Test
  public void testKeysAreSourcedFromConstruction() {
    StandardEncryptionManager manager =
        (StandardEncryptionManager) EncryptionTestHelpers.createEncryptionManager();
    StandardEncryptionManager.MintedKeys minted = manager.mintManifestListKey(keyMetadata());

    // A manager built from the minted keys resolves them; the original (empty) manager does not.
    List<EncryptedKey> metadataKeys =
        Lists.newArrayList(minted.manifestListKey(), minted.newKeyEncryptionKey());
    StandardEncryptionManager rebuilt =
        (StandardEncryptionManager) EncryptionTestHelpers.createEncryptionManager(metadataKeys);

    assertThat(rebuilt.encryptionKeys())
        .containsKeys(minted.manifestListKey().keyId(), minted.newKeyEncryptionKey().keyId());
    assertThat(manager.encryptionKeys()).isEmpty();
  }

  @Test
  public void testConcurrentMintingIsSafeAndDoesNotMutateKeys() throws InterruptedException {
    int threads = 8;
    int keysPerThread = 200;
    StandardEncryptionManager manager =
        (StandardEncryptionManager) EncryptionTestHelpers.createEncryptionManager();

    List<StandardEncryptionManager.MintedKeys> minted = new CopyOnWriteArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);

    for (int t = 0; t < threads; t++) {
      pool.submit(
          () -> {
            try {
              start.await();
              for (int i = 0; i < keysPerThread; i++) {
                minted.add(manager.mintManifestListKey(keyMetadata()));
                // Reading the immutable key set concurrently with minting must never throw.
                manager.encryptionKeys().forEach((id, key) -> {});
              }
            } catch (Throwable e) {
              failure.compareAndSet(null, e);
            } finally {
              done.countDown();
            }
          });
    }

    start.countDown();
    assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
    pool.shutdownNow();

    assertThat(failure.get()).isNull();
    assertThat(minted).hasSize(threads * keysPerThread);

    Set<String> manifestListKeyIds =
        minted.stream().map(keys -> keys.manifestListKey().keyId()).collect(Collectors.toSet());
    assertThat(manifestListKeyIds).hasSize(minted.size());

    // Minting never mutates the manager's immutable key set.
    assertThat(manager.encryptionKeys()).isEmpty();
  }

  private static NativeEncryptionKeyMetadata keyMetadata() {
    return new StandardKeyMetadata(new byte[16], new byte[12]);
  }
}
