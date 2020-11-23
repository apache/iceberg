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

import org.apache.iceberg.AssertHelpers;
import org.junit.Assert;
import org.junit.Test;

public class TestTableReference {


  @Test
  public void noMarkings() {
    String path = "foo";
    TableReference pti = TableReference.parse(path);
    Assert.assertEquals(path, pti.tableIdentifier().name());
    Assert.assertNull(pti.reference());
    Assert.assertNull(pti.timestamp());
  }

  @Test
  public void branchOnly() {
    String path = "foo@bar";
    TableReference pti = TableReference.parse(path);
    Assert.assertEquals("foo", pti.tableIdentifier().name());
    Assert.assertEquals("bar", pti.reference());
    Assert.assertNull(pti.timestamp());
  }

  @Test
  public void timestampOnly() {
    String path = "foo#baz";
    AssertHelpers.assertThrows("TableIdentifier is not parsable",
        IllegalArgumentException.class,
        "Invalid table name: # is not allowed (reference by timestamp is not supported)", () ->
            TableReference.parse(path));
  }

  @Test
  public void branchAndTimestamp() {
    String path = "foo@bar#baz";
    AssertHelpers.assertThrows("TableIdentifier is not parsable",
        IllegalArgumentException.class,
        "Invalid table name: # is not allowed (reference by timestamp is not supported)", () ->
            TableReference.parse(path));
  }

  @Test
  public void twoBranches() {
    String path = "foo@bar@boo";
    AssertHelpers.assertThrows("TableIdentifier is not parsable",
        IllegalArgumentException.class,
        "Can only reference one branch in", () ->
            TableReference.parse(path));
  }

  @Test
  public void twoTimestamps() {
    String path = "foo#baz#baa";
    AssertHelpers.assertThrows("TableIdentifier is not parsable",
        IllegalArgumentException.class,
        "Can only reference one timestamp in", () ->
            TableReference.parse(path));
  }

  @Test
  public void strangeCharacters() {
    String branch = "bar";
    String path = "/%";
    TableReference pti = TableReference.parse(path);
    Assert.assertEquals(path, pti.tableIdentifier().name());
    Assert.assertNull(pti.reference());
    Assert.assertNull(pti.timestamp());
    pti = TableReference.parse(path + "@" + branch);
    Assert.assertEquals(path, pti.tableIdentifier().name());
    Assert.assertEquals(branch, pti.reference());
    Assert.assertNull(pti.timestamp());
    path = "&&";
    pti = TableReference.parse(path);
    Assert.assertEquals(path, pti.tableIdentifier().name());
    Assert.assertNull(pti.reference());
    Assert.assertNull(pti.timestamp());
    pti = TableReference.parse(path + "@" + branch);
    Assert.assertEquals(path, pti.tableIdentifier().name());
    Assert.assertEquals(branch, pti.reference());
    Assert.assertNull(pti.timestamp());
  }

  @Test
  public void doubleByte() {
    String branch = "bar";
    String path = "/%国";
    TableReference pti = TableReference.parse(path);
    Assert.assertEquals(path, pti.tableIdentifier().name());
    Assert.assertNull(pti.reference());
    Assert.assertNull(pti.timestamp());
    pti = TableReference.parse(path + "@" + branch);
    Assert.assertEquals(path, pti.tableIdentifier().name());
    Assert.assertEquals(branch, pti.reference());
    Assert.assertNull(pti.timestamp());
    path = "国.国";
    pti = TableReference.parse(path);
    Assert.assertEquals(path, pti.tableIdentifier().toString());
    Assert.assertNull(pti.reference());
    Assert.assertNull(pti.timestamp());
    pti = TableReference.parse(path + "@" + branch);
    Assert.assertEquals(path, pti.tableIdentifier().toString());
    Assert.assertEquals(branch, pti.reference());
    Assert.assertNull(pti.timestamp());
  }

  @Test
  public void whitespace() {
    String branch = "bar ";
    String path = "foo ";
    TableReference pti = TableReference.parse(path);
    Assert.assertEquals(path, pti.tableIdentifier().name());
    Assert.assertNull(pti.reference());
    Assert.assertNull(pti.timestamp());
    pti = TableReference.parse(path + "@" + branch);
    Assert.assertEquals(path, pti.tableIdentifier().name());
    Assert.assertEquals(branch, pti.reference());
    Assert.assertNull(pti.timestamp());
  }
}
