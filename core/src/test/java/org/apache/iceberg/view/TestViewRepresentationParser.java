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

package org.apache.iceberg.view;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.TestJsonUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestViewRepresentationParser {

  private static ViewDefinition sql1 = BaseViewDefinition.builder()
      .sql("SELECT 'foo' foo")
      .schema(new Schema(optional(1, "foo", Types.StringType.get())))
      .build();

  private ViewRepresentation viewRepresentation;

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        {sql1}
    });
  }

  public TestViewRepresentationParser(ViewRepresentation viewRepresentation) {
    this.viewRepresentation = viewRepresentation;
  }

  @Test
  public void typeField() throws IOException {
    String json = TestJsonUtil.toJsonString(viewRepresentation, ViewRepresentationParser::toJson);
    JsonNode jsonNode = TestJsonUtil.fromJsonString(json);
    String actual = jsonNode.get(ViewRepresentationParser.Field.TYPE.fieldName()).asText();
    Assert.assertEquals(viewRepresentation.type().typeName(), actual);
  }

  @Test
  public void roundTrip() throws IOException {
    String json = TestJsonUtil.toJsonString(viewRepresentation, ViewRepresentationParser::toJson);
    JsonNode jsonNode = TestJsonUtil.fromJsonString(json);
    ViewRepresentation actual = ViewRepresentationParser.fromJson(jsonNode);
    Assert.assertEquals(viewRepresentation, actual);
  }
}
