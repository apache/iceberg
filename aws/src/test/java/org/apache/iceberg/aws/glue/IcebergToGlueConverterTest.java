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

package org.apache.iceberg.aws.glue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.DatabaseInput;

public class IcebergToGlueConverterTest {

  @Test
  public void toDatabaseName() {
    Assert.assertEquals("db", IcebergToGlueConverter.toDatabaseName(Namespace.of("db")));
  }

  @Test
  public void toDatabaseName_fail() {
    List<Namespace> badNames = Lists.newArrayList(
        Namespace.of("db", "a"),
        Namespace.of("db-1"),
        Namespace.empty(),
        Namespace.of(""),
        Namespace.of(new String(new char[600]).replace("\0", "a")));
    for (Namespace name : badNames) {
      AssertHelpers.assertThrows("bad namespace name",
          ValidationException.class,
          "Cannot convert namespace",
          () -> IcebergToGlueConverter.toDatabaseName(name)
      );
    }
  }

  @Test
  public void toDatabaseInput() {
    Map<String, String> param = new HashMap<>();
    DatabaseInput input = DatabaseInput.builder()
        .name("db")
        .parameters(param)
        .build();
    Namespace namespace = Namespace.of("db");
    Assert.assertEquals(input, IcebergToGlueConverter.toDatabaseInput(namespace, param));
  }
}
