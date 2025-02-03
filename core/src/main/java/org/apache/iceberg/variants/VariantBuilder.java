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
package org.apache.iceberg.variants;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

/** A builder class to build a primitive/array/object variant. */
public class VariantBuilder extends VariantBuilderBase {
  public VariantBuilder() {
    super(new ByteBufferWrapper(), new Dictionary());
  }

  public VariantPrimitiveBuilder createPrimitive() {
    return new VariantPrimitiveBuilder(valueBuffer(), dict());
  }

  public VariantObjectBuilder startObject() {
    return new VariantObjectBuilder(valueBuffer(), dict());
  }

  public VariantArrayBuilder startArray() {
    return new VariantArrayBuilder(valueBuffer(), dict());
  }

  /**
   * Parses a JSON string and constructs a Variant object.
   *
   * @param json The JSON string to parse.
   * @return The constructed Variant object.
   */
  public static Variant parseJson(String json) {
    Preconditions.checkArgument(
        json != null && !json.isEmpty(), "Input JSON string cannot be null or empty.");

    try (JsonParser parser = JsonUtil.factory().createParser(json)) {
      parser.nextToken();
      VariantBuilder builder = new VariantBuilder();
      builder.parseJson(parser);
      return builder.build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void parseJson(JsonParser parser) throws IOException {
    JsonToken token = parser.currentToken();

    if (token == null) {
      throw new JsonParseException(parser, "Unexpected null token");
    }

    switch (token) {
      case START_OBJECT:
        parseObject(parser);
        break;
      case START_ARRAY:
        parseArray(parser);
        break;
      default:
        parsePrimitive(parser);
    }
  }

  private void parseObject(JsonParser parser) throws IOException {
    List<VariantBuilderBase.FieldEntry> fields = Lists.newArrayList();
    int startPos = valueBuffer().pos();

    // Store object keys to dictionary of metadata
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String key = parser.currentName();
      parser.nextToken(); // Move to the value

      int id = dict().add(key);
      fields.add(new VariantBuilderBase.FieldEntry(key, id, valueBuffer().pos() - startPos));
      parseJson(parser);
    }

    endObject(startPos, fields);
  }

  private void parseArray(JsonParser parser) throws IOException {
    List<Integer> offsets = Lists.newArrayList();
    int startPos = valueBuffer().pos();

    while (parser.nextToken() != JsonToken.END_ARRAY) {
      offsets.add(valueBuffer().pos() - startPos);
      parseJson(parser);
    }

    endArray(startPos, offsets);
  }

  private void parsePrimitive(JsonParser parser) throws IOException {
    JsonToken token = parser.currentToken();

    switch (token) {
      case VALUE_STRING:
        writeStringInternal(parser.getText());
        break;
      case VALUE_NUMBER_INT:
        try {
          writeIntegralInternal(parser.getLongValue());
        } catch (InputCoercionException ignored) {
          writeFloatValue(parser);
        }
        break;
      case VALUE_NUMBER_FLOAT:
        writeFloatValue(parser);
        break;
      case VALUE_TRUE:
        writeBooleanInternal(true);
        break;
      case VALUE_FALSE:
        writeBooleanInternal(false);
        break;
      case VALUE_NULL:
        writeNullInternal();
        break;
      default:
        throw new JsonParseException(parser, "Unexpected token " + token);
    }
  }

  /**
   * This function attempts to write floating number in decimal format to store the exact value if
   * it fits in the decimal for Variant; otherwise, write as a double value.
   *
   * @param parser instance of JSONParser with the current token to be floating number
   */
  private void writeFloatValue(JsonParser parser) throws IOException {
    String input = parser.getText();
    BigDecimal decimalValue = new BigDecimal(input);

    // Decimal values only support a scale in [0, 38] and a precision <= 38
    if (decimalValue.scale() >= 0
        && decimalValue.scale() <= MAX_DECIMAL16_PRECISION
        && decimalValue.precision() <= MAX_DECIMAL16_PRECISION) {
      writeDecimalInternal(decimalValue);
    } else {
      writeDoubleInternal(parser.getDoubleValue());
    }
  }
}
