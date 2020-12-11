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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.encryption.Dek;
import org.apache.iceberg.encryption.KekId;
import org.apache.iceberg.util.conf.Properties;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.SerializableUtil.deserialize;
import static org.apache.iceberg.SerializableUtil.serialize;
import static org.junit.Assert.assertEquals;

public class TestDelegatingDekProvider {
  Map<String, DekProvider<? extends KekId>> providers;
  Mock1DekProvider mock1DekProvider;
  Mock2DekProvider mock2DekProvider;
  DelegatingDekProvider delegatingDekProvider;
  String mock1ProviderName;
  String mock2ProviderName;

  Mock1DekProvider.Mock1KekId mock1KekId;
  DelegatingDekProvider.DelegateKekId<Mock1DekProvider.Mock1KekId> delegate1KekId;

  Mock2DekProvider.Mock2KekId mock2KekId;
  DelegatingDekProvider.DelegateKekId<Mock2DekProvider.Mock2KekId> delegate2KekId;

  @Before
  public void before() {
    mock1ProviderName = "mock1";
    mock2ProviderName = "mock2";
    providers = new HashMap<>();
    mock1DekProvider = new Mock1DekProvider();
    mock2DekProvider = new Mock2DekProvider();
    providers.put(mock1ProviderName, mock1DekProvider);
    providers.put(mock2ProviderName, mock2DekProvider);
    delegatingDekProvider = new DelegatingDekProvider(mock1ProviderName, providers);

    mock1KekId = new Mock1DekProvider.Mock1KekId("one", 2);
    delegate1KekId =
        new DelegatingDekProvider.DelegateKekId<Mock1DekProvider.Mock1KekId>(
            mock1ProviderName, mock1DekProvider, mock1KekId);

    mock2KekId = new Mock2DekProvider.Mock2KekId(3, 4.0);
    delegate2KekId =
        new DelegatingDekProvider.DelegateKekId<Mock2DekProvider.Mock2KekId>(
            mock2ProviderName, mock2DekProvider, mock2KekId);
  }

  /**
   * This test checks that when a new dek is created it will get created by the delegate dek
   * provider.
   *
   * <p>Mock 1 will always create deks and ivs with 2 bytes.
   *
   * <p>Mock 2 will always creae deks and ivs with 1 byte.
   */
  @Test
  public void testGetNewDek() {
    Dek dek1 = delegatingDekProvider.getNewDek(delegate1KekId, 0, 0);
    assertEquals(Mock1DekProvider.BYTES, dek1.encryptedDek());

    Dek dek2 = delegatingDekProvider.getNewDek(delegate2KekId, 0, 0);
    assertEquals(Mock2DekProvider.BYTES, dek2.encryptedDek());
  }

  @Test
  public void testGetPlaintextDek() {
    Dek emptyDek1 = Dek.builder().setEncryptedDek(new byte[0]).setIv(new byte[0]).build();
    Dek emptyDek2 = Dek.builder().setEncryptedDek(new byte[0]).setIv(new byte[0]).build();

    Dek dek1 = delegatingDekProvider.getPlaintextDek(delegate1KekId, emptyDek1);
    assertEquals(Mock1DekProvider.BYTES, dek1.plaintextDek());

    Dek dek2 = delegatingDekProvider.getPlaintextDek(delegate2KekId, emptyDek2);
    assertEquals(Mock2DekProvider.BYTES, dek2.plaintextDek());
  }

  @Test
  public void testDelegateKekIdDumpLoad() {
    Map<String, String> map = new HashMap<>();
    Properties conf = Properties.of(map);
    delegate1KekId.dump(conf);
    DelegatingDekProvider.DelegateKekId<? extends KekId> loadedKekId =
        delegatingDekProvider.loadKekId(conf);
    assertEquals(loadedKekId, delegate1KekId);
  }

  @Test
  public void testDelegateKekIdSerializable()
      throws IOException, CloneNotSupportedException, ClassNotFoundException {
    Serializable uut = delegate1KekId;
    byte[] serialized1 = serialize(uut);
    byte[] serialized2 = serialize(uut);

    Object deserialized1 = deserialize(serialized1);
    Object deserialized2 = deserialize(serialized2);
    assertEquals(deserialized1, deserialized2);
    assertEquals(uut, deserialized1);
    assertEquals(uut, deserialized2);
  }

  @Test
  public void testSerializable()
      throws IOException, CloneNotSupportedException, ClassNotFoundException {
    Serializable uut = delegatingDekProvider;
    byte[] serialized1 = serialize(uut);
    byte[] serialized2 = serialize(uut);

    Object deserialized1 = deserialize(serialized1);
    Object deserialized2 = deserialize(serialized2);
    assertEquals(deserialized1, deserialized2);
    assertEquals(uut, deserialized1);
    assertEquals(uut, deserialized2);
  }
}
