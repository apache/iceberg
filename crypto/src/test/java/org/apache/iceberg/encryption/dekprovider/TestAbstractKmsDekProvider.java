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
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestAbstractKmsDekProvider {
  @Mock MockKms mockKms;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  AbstractKmsDekProvider abstractKmsDekProvider;
  AbstractKmsDekProvider.KmsKekId kekId;
  String keyId;

  @Before
  public void before() {
    abstractKmsDekProvider = new MockKmsDekProvider(mockKms);
    keyId = "myKey";
    kekId = new AbstractKmsDekProvider.KmsKekId(keyId);
  }

  @Test
  public void testLoadKekId() {
    Map<String, String> map = new HashMap<>();
    Properties props = Properties.of("", map);
    props.setString(AbstractKmsDekProvider.KEK_ID, keyId);
    AbstractKmsDekProvider.KmsKekId kmsKekId = abstractKmsDekProvider.loadKekId(props);

    assertEquals(kekId, kmsKekId);
    verifyNoMoreInteractions(mockKms);
  }

  @Test
  public void testDumpLoadKekId() {
    Map<String, String> map = new HashMap<>();
    Properties props = Properties.of("", map);
    kekId.dump(props);
    AbstractKmsDekProvider.KmsKekId kmsKekId = abstractKmsDekProvider.loadKekId(props);

    assertEquals(kekId, kmsKekId);
    verifyNoMoreInteractions(mockKms);
  }

  @Test
  public void testGetNewDek() {
    byte[] bytes = new byte[3];

    when(mockKms.getPlaintextFromKms(any(), anyInt()))
        .thenThrow(new RuntimeException())
        .thenReturn(new AbstractKmsDekProvider.KmsGenerateDekResponse(bytes, bytes));

    Dek dek = abstractKmsDekProvider.getNewDek(kekId, 3, 3);

    assertArrayEquals(bytes, dek.encryptedDek());
    assertArrayEquals(bytes, dek.plaintextDek());
    assertArrayEquals(AbstractKmsDekProvider.DUMMY_IV, dek.iv());
    verify(mockKms, times(2)).getPlaintextFromKms(any(), anyInt());
  }

  @Test
  public void testGetPlaintextDek() {
    byte[] bytes = new byte[3];

    when(mockKms.getPlaintextFromKms(any(), any()))
        .thenThrow(new RuntimeException())
        .thenReturn(new AbstractKmsDekProvider.KmsDecryptResponse(bytes));

    Dek emptyDek =
        Dek.builder().setEncryptedDek(bytes).setIv(AbstractKmsDekProvider.DUMMY_IV).build();

    Dek dek = abstractKmsDekProvider.getPlaintextDek(kekId, emptyDek);

    assertArrayEquals(bytes, dek.encryptedDek());
    assertArrayEquals(bytes, dek.plaintextDek());
    assertArrayEquals(AbstractKmsDekProvider.DUMMY_IV, dek.iv());
    verify(mockKms, times(2)).getPlaintextFromKms(any(), any());
  }
}
