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

package org.apache.iceberg.rest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.util.JsonUtil;

public class RESTSerializers {

  private RESTSerializers() {
  }

  public static void registerAll(ObjectMapper mapper) {
    SimpleModule module = new SimpleModule();
    module
        .addSerializer(TableIdentifier.class, new TableIdentifierSerializer())
        .addDeserializer(TableIdentifier.class, new TableIdentifierDeserializer())
        .addSerializer(Namespace.class, new NamespaceSerializer())
        .addDeserializer(Namespace.class, new NamespaceDeserializer())
        .addSerializer(Schema.class, new SchemaSerializer())
        .addDeserializer(Schema.class, new SchemaDeserializer())
        .addSerializer(PartitionSpec.class, new PartitionSpecSerializer())
        .addDeserializer(PartitionSpec.class, new PartitionSpecDeserializer())
        .addSerializer(SortOrder.class, new SortOrderSerializer())
        .addDeserializer(SortOrder.class, new SortOrderDeserializer());
    mapper.registerModule(module);
  }

  public static class NamespaceDeserializer extends JsonDeserializer<Namespace> {
    @Override
    public Namespace deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String[] levels = JsonUtil.getStringArray(p.getCodec().readTree(p));
      return Namespace.of(levels);
    }
  }

  public static class NamespaceSerializer extends JsonSerializer<Namespace> {
    @Override
    public void serialize(Namespace namespace, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      String[] parts = namespace.levels();
      gen.writeArray(parts, 0, parts.length);
    }
  }

  public static class TableIdentifierDeserializer extends JsonDeserializer<TableIdentifier> {
    @Override
    public TableIdentifier deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return TableIdentifierParser.fromJson(jsonNode);
    }
  }

  public static class TableIdentifierSerializer extends JsonSerializer<TableIdentifier> {
    @Override
    public void serialize(TableIdentifier identifier, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      TableIdentifierParser.toJson(identifier, gen);
    }
  }

  public static class SchemaDeserializer extends JsonDeserializer<Schema> {
    @Override
    public Schema deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      Schema schema = SchemaParser.fromJson(jsonNode);
      // Store the schema in the context so that it can be used when parsing PartitionSpec / SortOrder
      context.setAttribute("schema", schema);
      return schema;
    }
  }

  public static class SchemaSerializer extends JsonSerializer<Schema> {
    @Override
    public void serialize(Schema schema, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      SchemaParser.toJson(schema, gen);
    }
  }

  public static class PartitionSpecSerializer extends JsonSerializer<PartitionSpec> {
    @Override
    public void serialize(
        PartitionSpec partitionSpec, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      PartitionSpecParser.toJson(partitionSpec, gen);
    }
  }

  public static class PartitionSpecDeserializer extends JsonDeserializer<PartitionSpec> {
    @Override
    public PartitionSpec deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      Schema schema = (Schema) context.getAttribute("schema");
      return PartitionSpecParser.fromJson(schema, jsonNode);
    }
  }

  public static class SortOrderSerializer extends JsonSerializer<SortOrder> {
    @Override
    public void serialize(SortOrder sortOrder, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      SortOrderParser.toJson(sortOrder, gen);
    }
  }

  public static class SortOrderDeserializer extends JsonDeserializer<SortOrder> {
    @Override
    public SortOrder deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      Schema schema = (Schema) context.getAttribute("schema");
      return SortOrderParser.fromJson(schema, jsonNode);
    }
  }
}
