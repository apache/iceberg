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

package org.apache.iceberg.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import junit.framework.TestCase;
import org.junit.Test;

public class TestTableSQL extends TestCase {

  public void testExists() {
  }

  public void testNamespaceExists() {
  }

  public void testListNamespaces() {
  }

  public void testGetTable() {
  }

  public void testListTables() {
  }

  public void testDoCommit() {
  }

  public void testDoCommitCreate() {
  }

  public void testRenameTable() {
  }

  public void testDropTable() {
  }

  @Test
  public void testConversions() throws JsonProcessingException {
//
//    Namespace ns = Namespace.of("db", "db2", "ns2");
//    ImmutableMap<String, String> metadata = ImmutableMap.of("property", "test", "prop2", "val2");
//    String nsString = JdbcUtil.namespaceToString(ns);
//    Assert.assertEquals(ns, JdbcUtil.stringToNamespace(nsString));
  }

}
