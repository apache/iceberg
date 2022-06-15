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

package org.apache.iceberg.util;

import com.fasterxml.jackson.core.JsonParseException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestJsonUtil {
  @Test
  public void testParseJsonString() throws IOException {
    // minimal
    Assertions.assertThat(JsonUtil.parseJson("{}"))
        .hasToString("{}");

    Assertions.assertThat(JsonUtil.parseJson("{ \"attribute\": 123  }"))
        .hasToString("{\"attribute\":123}");

    // leading whitespace
    Assertions.assertThat(JsonUtil.parseJson("   {}"))
        .hasToString("{}");

    // trailing whitespace
    Assertions.assertThat(JsonUtil.parseJson("{}  "))
        .hasToString("{}");
  }

  @Test
  public void testRejectTrailingStringContent() {
    Assertions.assertThatThrownBy(() -> JsonUtil.parseJson("{} a"))
        .isInstanceOf(JsonParseException.class)
        .hasMessageStartingWith(
            "Unrecognized token 'a': was expecting (JSON String, Number, Array, Object or token 'null', " +
                "'true' or 'false')");

    Assertions.assertThatThrownBy(() -> JsonUtil.parseJson("{} null"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Found characters after the expected end of input");

    Assertions.assertThatThrownBy(() -> JsonUtil.parseJson("{}{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Found characters after the expected end of input");
  }

  @Test
  public void testParseJsonStream() throws IOException {
    // minimal
    Assertions.assertThat(JsonUtil.parseJson(byteStream("{}")))
        .hasToString("{}");

    Assertions.assertThat(JsonUtil.parseJson(byteStream("{ \"attribute\": 123  }")))
        .hasToString("{\"attribute\":123}");

    // leading whitespace
    Assertions.assertThat(JsonUtil.parseJson(byteStream("   {}")))
        .hasToString("{}");

    // trailing whitespace
    Assertions.assertThat(JsonUtil.parseJson(byteStream("{}  ")))
        .hasToString("{}");
  }

  @Test
  public void testRejectTrailingStreamContent() {
    Assertions.assertThatThrownBy(() -> JsonUtil.parseJson(byteStream("{} a")))
        .isInstanceOf(JsonParseException.class)
        .hasMessageStartingWith(
            "Unrecognized token 'a': was expecting (JSON String, Number, Array, Object or token 'null', " +
                "'true' or 'false')");

    Assertions.assertThatThrownBy(() -> JsonUtil.parseJson(byteStream("{} null")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Found characters after the expected end of input");

    Assertions.assertThatThrownBy(() -> JsonUtil.parseJson(byteStream("{}{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Found characters after the expected end of input");
  }

  private InputStream byteStream(String input) {
    return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
  }
}
