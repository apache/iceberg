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

package org.apache.iceberg.nessie;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableReference {

  @Test
  public void noMarkings() {
    String path = "foo";
    TableReference pti = TableReference.parse(path);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat(pti.reference()).isNull();
    Assertions.assertThat(pti.timestamp()).isNull();
  }

  @Test
  public void branchOnly() {
    String path = "foo@bar";
    TableReference pti = TableReference.parse(path);
    Assertions.assertThat("foo").isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat("bar").isEqualTo(pti.reference());
    Assertions.assertThat(pti.timestamp()).isNull();
  }

  @Test
  public void timestampOnly() {
    String path = "foo#baz";
    Assertions.assertThatThrownBy(() -> TableReference.parse(path))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table name: # is not allowed (reference by timestamp is not supported)");
  }

  @Test
  public void branchAndTimestamp() {
    String path = "foo@bar#baz";
    Assertions.assertThatThrownBy(() -> TableReference.parse(path))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table name: # is not allowed (reference by timestamp is not supported)");
  }

  @Test
  public void twoBranches() {
    String path = "foo@bar@boo";
    Assertions.assertThatThrownBy(() -> TableReference.parse(path))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can only reference one branch in");
  }

  @Test
  public void twoTimestamps() {
    String path = "foo#baz#baa";
    Assertions.assertThatThrownBy(() -> TableReference.parse(path))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can only reference one timestamp in");
  }

  @Test
  public void strangeCharacters() {
    String branch = "bar";
    String path = "/%";
    TableReference pti = TableReference.parse(path);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat(pti.reference()).isNull();
    Assertions.assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat(branch).isEqualTo(pti.reference());
    Assertions.assertThat(pti.timestamp()).isNull();
    path = "&&";
    pti = TableReference.parse(path);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat(pti.reference()).isNull();
    Assertions.assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat(branch).isEqualTo(pti.reference());
    Assertions.assertThat(pti.timestamp()).isNull();
  }

  @Test
  public void doubleByte() {
    String branch = "bar";
    String path = "/%国";
    TableReference pti = TableReference.parse(path);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat(pti.reference()).isNull();
    Assertions.assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat(branch).isEqualTo(pti.reference());
    Assertions.assertThat(pti.timestamp()).isNull();
    path = "国.国";
    pti = TableReference.parse(path);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().toString());
    Assertions.assertThat(pti.reference()).isNull();
    Assertions.assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().toString());
    Assertions.assertThat(branch).isEqualTo(pti.reference());
    Assertions.assertThat(pti.timestamp()).isNull();
  }

  @Test
  public void whitespace() {
    String branch = "bar ";
    String path = "foo ";
    TableReference pti = TableReference.parse(path);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat(pti.reference()).isNull();
    Assertions.assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    Assertions.assertThat(path).isEqualTo(pti.tableIdentifier().name());
    Assertions.assertThat(branch).isEqualTo(pti.reference());
    Assertions.assertThat(pti.timestamp()).isNull();
  }
}
