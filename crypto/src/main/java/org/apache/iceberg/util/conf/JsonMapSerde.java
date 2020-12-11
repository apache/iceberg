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

package org.apache.iceberg.util.conf;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonMapSerde implements MapSerde {
  private static final Logger log = LoggerFactory.getLogger(JsonMapSerde.class);
  public static final JsonMapSerde INSTANCE = new JsonMapSerde();

  @Override
  public Map<String, String> fromBytes(ByteBuffer bytes) throws IOException {
    JsonNode jsonNode = JsonUtil.mapper().readValue(bytes.array(), JsonNode.class);
    if (jsonNode.getNodeType() != JsonNodeType.OBJECT) {
      throw new IllegalArgumentException(
          String.format(
              "Can't deserialize Json as a Json Object! Got %s instead.", jsonNode.getNodeType()));
    }
    Map<String, String> map = new HashMap<>();
    for (Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> entry = it.next();
      String property = entry.getKey();
      JsonNode value = entry.getValue();
      if (value.getNodeType() != JsonNodeType.STRING) {
        throw new IllegalArgumentException(
            String.format(
                "Can't deserialize json field %s as a string! Got %s instead.",
                property, value.getNodeType()));
      }

      map.put(property, value.textValue());
    }
    return map;
  }

  @Override
  public byte[] toBytes(Map<String, String> map) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (JsonGenerator generator = JsonUtil.factory().createGenerator(byteArrayOutputStream)) {
      generator.writeStartObject();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        generator.writeStringField(entry.getKey(), entry.getValue());
      }
      generator.writeEndObject();
      generator.flush();
    }
    byteArrayOutputStream.flush();
    byteArrayOutputStream.close();
    return byteArrayOutputStream.toByteArray();
  }
}
