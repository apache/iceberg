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

import java.util.Arrays;
import org.apache.iceberg.encryption.Dek;
import org.apache.iceberg.encryption.KekId;
import org.apache.iceberg.util.conf.Conf;

public abstract class AbstractPlaintextDekProvider
    extends DekProvider<AbstractPlaintextDekProvider.PlaintextKekId> {
  public static final String NAME = "plaintext";

  // visible for testing
  protected static final byte[] DUMMY_IV = new byte[0];

  protected abstract byte[] generateRandomBytes(int numBytes);

  @Override
  public Dek getNewDek(PlaintextKekId kekId, int dekLength, int ivLength) {
    byte[] encryptedDek = generateRandomBytes(dekLength);
    return new Dek(encryptedDek, Arrays.copyOf(encryptedDek, encryptedDek.length), DUMMY_IV);
  }

  @Override
  public Dek getPlaintextDek(PlaintextKekId kekId, Dek dek) {
    dek.setPlaintextDek(Arrays.copyOf(dek.encryptedDek(), dek.encryptedDek().length));
    return dek;
  }

  @Override
  public PlaintextKekId loadKekId(Conf conf) {
    return PlaintextKekId.INSTANCE;
  }

  public static class PlaintextKekId implements KekId {

    private PlaintextKekId() {
    }

    public static final PlaintextKekId INSTANCE = new PlaintextKekId();

    @Override
    public String toString() {
      return "PlaintextKekId{}";
    }

    @Override
    public void dump(Conf conf) {
    }
  }
}
