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

package org.apache.iceberg.encryption.dekprovider;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.encryption.Dek;
import org.apache.iceberg.util.conf.Properties;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestAbstractPlaintextDekProvider {
  AbstractPlaintextDekProvider abstractPlaintextDekProvider;
  AbstractPlaintextDekProvider.PlaintextKekId kekId;

  @Before
  public void before() {
    abstractPlaintextDekProvider = new MockPlaintextDekProvider();
    kekId = AbstractPlaintextDekProvider.PlaintextKekId.INSTANCE;
  }

  @Test
  public void testDumpLoadKekId() {
    Map<String, String> map = new HashMap<>();
    Properties props = Properties.of("", map);
    AbstractPlaintextDekProvider.PlaintextKekId.INSTANCE.dump(props);
    AbstractPlaintextDekProvider.PlaintextKekId plaintextKekId =
        abstractPlaintextDekProvider.loadKekId(props);

    assertEquals(kekId, plaintextKekId);
  }

  @Test
  public void testGetNewDek() {
    Dek dek = abstractPlaintextDekProvider.getNewDek(kekId, 3, 3);

    assertArrayEquals(MockPlaintextDekProvider.BYTES, dek.encryptedDek());
    assertArrayEquals(MockPlaintextDekProvider.BYTES, dek.plaintextDek());
    assertArrayEquals(AbstractPlaintextDekProvider.DUMMY_IV, dek.iv());
  }

  @Test
  public void testGetPlaintextDek() {
    byte[] bytes = new byte[4];

    Dek emptyDek = Dek.builder().setEncryptedDek(bytes).setIv(bytes).build();

    Dek dek = abstractPlaintextDekProvider.getPlaintextDek(kekId, emptyDek);

    assertArrayEquals(bytes, dek.encryptedDek());
    assertArrayEquals(bytes, dek.plaintextDek());
    assertArrayEquals(bytes, dek.iv());
  }
}
