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

package org.apache.iceberg.rest.http;

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
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;


public class RequestResponseSerializers {

  private RequestResponseSerializers() {
  }

  public static void registerAll(ObjectMapper mapper) {
    SimpleModule module = new SimpleModule();
    module
        .addSerializer(TableIdentifier.class, new RequestResponseSerializers.TableIdentifierSerializer())
        .addDeserializer(TableIdentifier.class, new RequestResponseSerializers.TableIdentifierDeserializer())
        .addSerializer(Namespace.class, new RequestResponseSerializers.NamespaceSerializer())
        .addDeserializer(Namespace.class, new RequestResponseSerializers.NamespaceDeserializer())
        .addSerializer(Schema.class, new RequestResponseSerializers.SchemaSerializer())
        .addDeserializer(Schema.class, new RequestResponseSerializers.SchemaDeserializer())
        .addSerializer(PartitionSpec.class, new RequestResponseSerializers.PartitionSpecSerializer())
        .addDeserializer(PartitionSpec.class, new RequestResponseSerializers.PartitionSpecDeserializer())
        .addSerializer(SortOrder.class, new RequestResponseSerializers.SortOrderSerializer())
        .addDeserializer(SortOrder.class, new RequestResponseSerializers.SortOrderDeserializer())
        .addSerializer(TableMetadata.class, new RequestResponseSerializers.TableMetadataSerializer());
    mapper.registerModule(module);
  }

  public static class NamespaceDeserializer extends JsonDeserializer<Namespace> {
    @Override
    public Namespace deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return Namespace.of(jsonNode.asText().split("\\."));
    }
  }

  public static class NamespaceSerializer extends JsonSerializer<Namespace> {
    @Override
    public void serialize(Namespace namespace, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeString(namespace.toString());
    }
  }

  public static class TableIdentifierDeserializer extends JsonDeserializer<TableIdentifier> {
    @Override
    public TableIdentifier deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return TableIdentifier.parse(jsonNode.asText());
    }
  }

  public static class TableIdentifierSerializer extends JsonSerializer<TableIdentifier> {
    @Override
    public void serialize(TableIdentifier identifier, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeString(identifier.toString());
    }
  }

  public static class SchemaDeserializer extends JsonDeserializer<Schema> {
    @Override
    public Schema deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      Schema schema = SchemaParser.fromJson(jsonNode);
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
    public void serialize(PartitionSpec partitionSpec, JsonGenerator gen, SerializerProvider serializers)
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
    public SortOrder deserialize(JsonParser p, DeserializationContext context)
        throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      Schema schema = (Schema) context.getAttribute("schema");

      return SortOrderParser.fromJson(schema, jsonNode);
    }
  }

  public static class TableMetadataSerializer extends JsonSerializer<TableMetadata> {
    @Override
    public void serialize(TableMetadata metadata, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeRaw(TableMetadataParser.toJson(metadata));
    }
  }
}
