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

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TestKeyRotation {

  @Test
  public void testBasicEncrypt() {
    StandardEncryptionManager sem =
        (StandardEncryptionManager) EncryptionTestHelpers.createEncryptionManager();

    String initialKekID = sem.keyEncryptionKeyID();

    sem.setTestTimeShift(TimeUnit.DAYS.toMillis(30));
    // below rotation time, key must be the same
    assertThat(sem.keyEncryptionKeyID()).isEqualTo(initialKekID);

    sem.setTestTimeShift(TimeUnit.DAYS.toMillis(800));
    // above rotation time, key must be different
    assertThat(sem.keyEncryptionKeyID()).isNotEqualTo(initialKekID);
  }
}
