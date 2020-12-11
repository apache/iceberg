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

public class Mock2DekProvider extends DekProvider<Mock2DekProvider.Mock2KekId> {
  /** * PROPERTIES AND DEFAULTS ** */
  public static final String MY_FIELD = "myField2";

  public static final String MY_OTHER_FIELD = "myOtherField2";

  public static final byte[] BYTES = new byte[1];

  /** * CONSTRUCTOR ** */
  public Mock2DekProvider() {
  }

  /** * LOGIC ** */
  @Override
  public Dek getNewDek(Mock2KekId kekId, int dekLength, int ivLength) {
    return Dek.builder().setEncryptedDek(BYTES).setIv(BYTES).build();
  }

  @Override
  public Dek getPlaintextDek(Mock2KekId kekId, Dek dek) {
    dek.setPlaintextDek(BYTES);
    return dek;
  }

  @Override
  public String toString() {
    return "Mock2DekProvider{}";
  }

  @Override
  public Mock2KekId loadKekId(Conf conf) {
    int myValue = conf.propertyAsInt(MY_FIELD);
    double myOtherValue = conf.propertyAsDouble(MY_OTHER_FIELD);
    return new Mock2KekId(myValue, myOtherValue);
  }

  public static class Mock2KekId implements KekId {
    private final int myValue;
    private final double myOtherValue;

    public Mock2KekId(int myValue, double myOtherValue) {
      this.myValue = myValue;
      this.myOtherValue = myOtherValue;
    }

    @Override
    public void dump(Conf conf) {
      conf.setInt(MY_FIELD, myValue);
      conf.setDouble(MY_OTHER_FIELD, myOtherValue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Mock2KekId)) {
        return false;
      }
      Mock2KekId mock2KekId = (Mock2KekId) o;
      return myOtherValue == mock2KekId.myOtherValue && Objects.equals(myValue, mock2KekId.myValue);
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
    return 2;
  }

  public static class Builder implements LoadableBuilder<Builder, Mock2DekProvider> {

    @Override
    public Builder load(Conf conf) {
      return this;
    }

    @Override
    public Mock2DekProvider build() {
      return new Mock2DekProvider();
    }
  }
}
