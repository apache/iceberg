package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

import java.io.IOException;
import java.util.Iterator;

public class PrimaryKeySpecParser {
  private PrimaryKeySpecParser() {
  }

  private static final String SOURCE_ID = "source-id";
  private static final String LAYOUT = "layout";
  private static final String NAME = "name";

  static void toJsonFields(PrimaryKeySpec spec, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (PrimaryKeySpec.PrimaryKeyField field : spec.fields()) {
      generator.writeStartObject();
      generator.writeStringField(NAME, field.name());
      generator.writeStringField(LAYOUT, field.layout().name());
      generator.writeNumberField(SOURCE_ID, field.sourceId());
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  static PrimaryKeySpec fromJsonFields(Schema schema, JsonNode json) {
    PrimaryKeySpec.Builder builder = PrimaryKeySpec.builderFor(schema);
    buildFromJsonFields(builder, json);
    return builder.build();
  }

  private static void buildFromJsonFields(PrimaryKeySpec.Builder builder, JsonNode json) {
    Preconditions.checkArgument(json.isArray(),
              "Cannot parse primary key spec fields, not an array: %s", json);

    Iterator<JsonNode> elements = json.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Preconditions.checkArgument(element.isObject(),
              "Cannot parse primary key field, not an object: %s", element);

      String name = JsonUtil.getString(NAME, element);
      String layout = JsonUtil.getString(LAYOUT, element);
      int sourceId = JsonUtil.getInt(SOURCE_ID, element);
      builder.addColumn(sourceId, name, PrimaryKeySpec.PrimaryKeyLayout.valueOf(layout));
    }
  }
}
