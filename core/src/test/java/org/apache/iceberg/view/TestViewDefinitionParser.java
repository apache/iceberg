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

import java.util.Arrays;
import java.util.Collection;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.TestJsonUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestViewDefinitionParser extends ParserTestBase<ViewDefinition> {

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        {
            BaseViewDefinition.builder()
                .sql("SELECT 'foo' foo")
                .schema(new Schema(optional(1, "foo", Types.StringType.get())))
                .build(),
            TestJsonUtil.objectString(
                "\"type\":\"sql\"",
                "\"sql\":\"SELECT 'foo' foo\"",
                "\"dialect\":\"\"",
                "\"schema\":" + TestJsonUtil.objectString(
                    "\"type\":\"struct\"",
                    "\"schema-id\":0",
                    "\"fields\":" + TestJsonUtil.arrayString(
                        TestJsonUtil.objectString(
                            "\"id\":1",
                            "\"name\":\"foo\"",
                            "\"required\":false",
                            "\"type\":\"string\""))),
                "\"default-catalog\":\"\"",
                "\"default-namespace\":[]",
                "\"field-aliases\":[]",
                "\"field-comments\":[]")
        },
        {
            BaseViewDefinition.builder()
                .sql("SELECT 'foo' foo, 'foo2' foo2")
                .schema(new Schema(
                    optional(1, "col1", Types.StringType.get(), "Comment col1"),
                    optional(2, "col2", Types.StringType.get(), "Comment col2")))
                .defaultCatalog("cat")
                .defaultNamespace(Arrays.asList("part1", "part2"))
                .fieldAliases(Arrays.asList("col1", "col2"))
                .fieldComments(Arrays.asList("Comment col1", "Comment col2"))
                .build(),
            TestJsonUtil.objectString(
                "\"type\":\"sql\"",
                "\"sql\":\"SELECT 'foo' foo, 'foo2' foo2\"",
                "\"dialect\":\"\"",
                "\"schema\":" + TestJsonUtil.objectString(
                    "\"type\":\"struct\"",
                    "\"schema-id\":0",
                    "\"fields\":" + TestJsonUtil.arrayString(
                        TestJsonUtil.objectString(
                            "\"id\":1",
                            "\"name\":\"col1\"",
                            "\"required\":false",
                            "\"type\":\"string\"",
                            "\"doc\":\"Comment col1\""),
                        TestJsonUtil.objectString(
                            "\"id\":2",
                            "\"name\":\"col2\"",
                            "\"required\":false",
                            "\"type\":\"string\"",
                            "\"doc\":\"Comment col2\""))),
                "\"default-catalog\":\"cat\"",
                "\"default-namespace\":[\"part1\",\"part2\"]",
                "\"field-aliases\":[\"col1\",\"col2\"]",
                "\"field-comments\":[\"Comment col1\",\"Comment col2\"]")
        },
        {
            BaseViewDefinition.builder()
                .sql("SELECT 'foo' foo")
                .build(),
            TestJsonUtil.objectString(
                "\"type\":\"sql\"",
                "\"sql\":\"SELECT 'foo' foo\"",
                "\"dialect\":\"\"",
                "\"schema\":" + TestJsonUtil.objectString(
                    "\"type\":\"struct\"",
                    "\"schema-id\":0",
                    "\"fields\":[]"),
                "\"default-catalog\":\"\"",
                "\"default-namespace\":[]",
                "\"field-aliases\":[]",
                "\"field-comments\":[]")
        }
    });
  }

  public TestViewDefinitionParser(ViewDefinition entry, String json) {
    super(entry, json, ViewDefinitionParser::toJson, ViewDefinitionParser::fromJson);
  }
}
