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

import com.dremio.nessie.client.NessieClient;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.junit.Assert;
import org.junit.Test;

public class TestParsedTableIdentifier {


  @Test
  public void noMarkings() {
    String path = "foo";
    ParsedTableIdentifier pti = ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().name());
    Assert.assertNull(pti.getReference());
    Assert.assertNull(pti.getTimestamp());
  }

  @Test
  public void branchOnly() {
    String path = "foo@bar";
    ParsedTableIdentifier pti = ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assert.assertEquals("foo", pti.getTableIdentifier().name());
    Assert.assertEquals("bar", pti.getReference());
    Assert.assertNull(pti.getTimestamp());
  }

  @Test
  public void timestampOnly() {
    String path = "foo#baz";
    AssertHelpers.assertThrows("TableIdentifier is not parsable",
        IllegalArgumentException.class,
        "Currently we don't support referencing by timestamp", () ->
            ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>()));
  }

  @Test
  public void branchAndTimestamp() {
    String path = "foo@bar#baz";
    AssertHelpers.assertThrows("TableIdentifier is not parsable",
        IllegalArgumentException.class,
        "Currently we don't support referencing by timestamp", () ->
            ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>()));
  }

  @Test
  public void twoBranches() {
    String path = "foo@bar@boo";
    AssertHelpers.assertThrows("TableIdentifier is not parsable",
        IllegalArgumentException.class,
        "Can only reference one branch in", () ->
            ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>()));
  }

  @Test
  public void twoTimestamps() {
    String path = "foo#baz#baa";
    AssertHelpers.assertThrows("TableIdentifier is not parsable",
        IllegalArgumentException.class,
        "Can only reference one timestamp in", () ->
            ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>()));
  }

  @Test
  public void branchOnlyInProps() {
    String path = "foo";
    Map<String, String> map = new HashMap<>();
    map.put(NessieClient.CONF_NESSIE_REF, "bar");
    ParsedTableIdentifier pti = ParsedTableIdentifier.getParsedTableIdentifier(path, map);
    Assert.assertEquals("foo", pti.getTableIdentifier().name());
    Assert.assertEquals("bar", pti.getReference());
    Assert.assertNull(pti.getTimestamp());
  }

  @Test
  public void strangeCharacters() {
    String branch = "bar";
    String path = "/%";
    ParsedTableIdentifier pti = ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().name());
    Assert.assertNull(pti.getReference());
    Assert.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().name());
    Assert.assertEquals(branch, pti.getReference());
    Assert.assertNull(pti.getTimestamp());
    path = "&&";
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().name());
    Assert.assertNull(pti.getReference());
    Assert.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().name());
    Assert.assertEquals(branch, pti.getReference());
    Assert.assertNull(pti.getTimestamp());
  }

  @Test
  public void doubleByte() {
    String branch = "bar";
    String path = "/%国";
    ParsedTableIdentifier pti = ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().name());
    Assert.assertNull(pti.getReference());
    Assert.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().name());
    Assert.assertEquals(branch, pti.getReference());
    Assert.assertNull(pti.getTimestamp());
    path = "国.国";
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().toString());
    Assert.assertNull(pti.getReference());
    Assert.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().toString());
    Assert.assertEquals(branch, pti.getReference());
    Assert.assertNull(pti.getTimestamp());
  }

  @Test
  public void whitespace() {
    String branch = "bar ";
    String path = "foo ";
    ParsedTableIdentifier pti = ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().name());
    Assert.assertNull(pti.getReference());
    Assert.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assert.assertEquals(path, pti.getTableIdentifier().name());
    Assert.assertEquals(branch, pti.getReference());
    Assert.assertNull(pti.getTimestamp());
  }
}
