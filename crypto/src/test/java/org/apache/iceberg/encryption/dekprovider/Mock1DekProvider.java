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

import java.util.Objects;
import org.apache.iceberg.encryption.Dek;
import org.apache.iceberg.encryption.KekId;
import org.apache.iceberg.util.conf.Conf;
import org.apache.iceberg.util.conf.LoadableBuilder;

public class Mock1DekProvider extends DekProvider<Mock1DekProvider.Mock1KekId> {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String MY_FIELD = "myField1";

  public static final String MY_OTHER_FIELD = "myOtherField1";
  public static final byte[] BYTES = new byte[0];

  /** * CONSTRUCTOR ** */
  public Mock1DekProvider() {
  }

  /** * LOGIC ** */
  @Override
  public Dek getNewDek(Mock1KekId kekId, int dekLength, int ivLength) {
    return Dek.builder().setEncryptedDek(BYTES).setIv(BYTES).build();
  }

  @Override
  public Dek getPlaintextDek(Mock1KekId kekId, Dek dek) {
    dek.setPlaintextDek(BYTES);
    return dek;
  }

  @Override
  public String toString() {
    return "Mock1DekProvider{}";
  }

  @Override
  public Mock1KekId loadKekId(Conf conf) {
    String myValue = conf.propertyAsString(MY_FIELD);
    int myOtherValue = conf.propertyAsInt(MY_OTHER_FIELD);
    return new Mock1KekId(myValue, myOtherValue);
  }

  public static class Mock1KekId implements KekId {
    private final String myValue;
    private final int myOtherValue;

    public Mock1KekId(String myValue, int myOtherValue) {
      this.myValue = myValue;
      this.myOtherValue = myOtherValue;
    }

    @Override
    public void dump(Conf conf) {
      conf.setString(MY_FIELD, myValue);
      conf.setInt(MY_OTHER_FIELD, myOtherValue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (this.getClass() != o.getClass()) {
        return false;
      }
      Mock1KekId mockKekId = (Mock1KekId) o;
      return myOtherValue == mockKekId.myOtherValue && Objects.equals(myValue, mockKekId.myValue);
    }

    @Override
    public int hashCode() {
      return Objects.hash(myValue, myOtherValue);
    }

    @Override
    public String toString() {
      return "MockKekId{" + "myValue='" + myValue + '\'' + ", myOtherValue=" + myOtherValue + '}';
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return this.getClass() == o.getClass();
  }

  @Override
  public int hashCode() {
    return 1;
  }

  public static class Builder implements LoadableBuilder<Builder, Mock1DekProvider> {

    @Override
    public Builder load(Conf conf) {
      return this;
    }

    @Override
    public Mock1DekProvider build() {
      return new Mock1DekProvider();
    }
  }
}
