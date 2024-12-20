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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** A builder class to build a primitive/array/object variant. */
public class VariantBuilder extends VariantBuilderBase {
  public VariantBuilder() {
    super(new VariantBuilderBase.ByteBufferWrapper(), new VariantBuilderBase.Dictionary());
  }

  public VariantPrimitiveBuilder createPrimitive() {
    VariantPrimitiveBuilder primitiveBuilder = new VariantPrimitiveBuilder(getBuffer(), getDict());
    return primitiveBuilder;
  }

  public VariantObjectBuilder startObject() {
    return new VariantObjectBuilder(getBuffer(), getDict());
  }

  public VariantArrayBuilder startArray() {
    return new VariantArrayBuilder(getBuffer(), getDict());
  }

  /**
   * Parses a JSON string and constructs a Variant object.
   *
   * @param json The JSON string to parse.
   * @return The constructed Variant object.
   * @throws IOException If an error occurs while reading or parsing the JSON.
   */
  public static Variant parseJson(String json) throws IOException {
    Preconditions.checkArgument(
        json != null && !json.isEmpty(), "Input JSON string cannot be null or empty.");

    try (JsonParser parser = new JsonFactory().createParser(json)) {
      parser.nextToken();

      VariantBuilder builder = new VariantBuilder();
      builder.buildJson(parser);

      return builder.build();
    }
  }

  private void buildJson(JsonParser parser) throws IOException {
    JsonToken token = parser.currentToken();

    if (token == null) {
      throw new JsonParseException(parser, "Unexpected null token");
    }

    switch (token) {
      case START_OBJECT:
        writeObject(parser);
        break;
      case START_ARRAY:
        writeArray(parser);
        break;
      case VALUE_STRING:
        writeStringInternal(parser.getText());
        break;
      case VALUE_NUMBER_INT:
        writeInteger(parser);
        break;
      case VALUE_NUMBER_FLOAT:
        writeFloat(parser);
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

  private void writeObject(JsonParser parser) throws IOException {
    List<VariantBuilderBase.FieldEntry> fields = Lists.newArrayList();
    int startPos = getBuffer().getPos();

    // Store object keys to dictionary of metadata
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String key = parser.currentName();
      parser.nextToken(); // Move to the value

      int id = getDict().add(key);
      fields.add(new VariantBuilderBase.FieldEntry(key, id, getBuffer().getPos() - startPos));
      buildJson(parser);
    }

    endObject(startPos, fields);
  }

  private void writeArray(JsonParser parser) throws IOException {
    List<Integer> offsets = Lists.newArrayList();
    int startPos = getBuffer().getPos();

    while (parser.nextToken() != JsonToken.END_ARRAY) {
      offsets.add(getBuffer().getPos() - startPos);
      buildJson(parser);
    }

    endArray(startPos, offsets);
  }

  private void writeInteger(JsonParser parser) throws IOException {
    try {
      writeNumericInternal(parser.getLongValue());
    } catch (InputCoercionException ignored) {
      writeFloat(parser); // Fallback for large integers
    }
  }

  private void writeFloat(JsonParser parser) throws IOException {
    if (!tryWriteDecimal(parser.getText())) {
      writeDoubleInternal(parser.getDoubleValue());
    }
  }

  /**
   * Attempts to parse a JSON number as a decimal and write it. The input must meet the following
   * criteria: - Be in a valid decimal format (integer with an optional '.'). - Not in scientific
   * notation. - Fit within the precision and scale limits of decimal types.
   *
   * @param input the input string representing the JSON number
   * @return true if the decimal is valid and written successfully; false otherwise
   */
  private boolean tryWriteDecimal(String input) {
    // Validate that the input matches a decimal format and is not in scientific notation.
    if (!input.matches("-?\\d+(\\.\\d+)?")) {
      return false;
    }

    // Parse the input string to BigDecimal.
    BigDecimal decimalValue = new BigDecimal(input);

    // Ensure the decimal value meets precision and scale limits.
    if (decimalValue.scale() <= VariantConstants.MAX_DECIMAL16_PRECISION
        && decimalValue.precision() <= VariantConstants.MAX_DECIMAL16_PRECISION) {
      writeDecimalInternal(decimalValue);
      return true;
    }

    return false;
  }
}
