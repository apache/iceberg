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

import java.io.IOException;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.TestJsonUtil;
import org.junit.Assert;
import org.junit.Test;

public abstract class ParserTestBase<T> {

  private final T entry;
  private final String json;
  private final JsonUtil.JsonWriter<T> writer;
  private final JsonUtil.JsonReader<T> reader;

  public ParserTestBase(
      T entry, String json, JsonUtil.JsonWriter<T> writer, JsonUtil.JsonReader<T> reader) {
    this.entry = entry;
    this.json = json;
    this.writer = writer;
    this.reader = reader;
  }

  @Test
  public void toJson() throws IOException {

    String expected = TestJsonUtil.fromJsonString(json).toString();
    Assert.assertEquals(expected, TestJsonUtil.toJsonString(entry, writer));
  }

  @Test
  public void fromJson() throws IOException {
    Assert.assertEquals(entry, TestJsonUtil.fromJsonString(json, reader));
  }
}
