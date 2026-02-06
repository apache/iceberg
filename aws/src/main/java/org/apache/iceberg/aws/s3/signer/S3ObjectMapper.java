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
package org.apache.iceberg.aws.s3.signer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import org.apache.iceberg.rest.RESTSerializers.ErrorResponseDeserializer;
import org.apache.iceberg.rest.RESTSerializers.ErrorResponseSerializer;
import org.apache.iceberg.rest.RESTSerializers.OAuthTokenResponseDeserializer;
import org.apache.iceberg.rest.RESTSerializers.OAuthTokenResponseSerializer;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;

public class S3ObjectMapper {

  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);
  private static volatile boolean isInitialized = false;

  private S3ObjectMapper() {}

  static ObjectMapper mapper() {
    if (!isInitialized) {
      synchronized (S3ObjectMapper.class) {
        if (!isInitialized) {
          MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
          MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
          // even though using new PropertyNamingStrategy.KebabCaseStrategy() is deprecated
          // and PropertyNamingStrategies.KebabCaseStrategy.INSTANCE (introduced in jackson 2.14) is
          // recommended, we can't use it because Spark still relies on jackson 2.13.x stuff
          MAPPER.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
          MAPPER.registerModule(initModule());
          isInitialized = true;
        }
      }
    }

    return MAPPER;
  }

  public static SimpleModule initModule() {
    return new SimpleModule()
        .addSerializer(ErrorResponse.class, new ErrorResponseSerializer())
        .addDeserializer(ErrorResponse.class, new ErrorResponseDeserializer())
        .addSerializer(OAuthTokenResponse.class, new OAuthTokenResponseSerializer())
        .addDeserializer(OAuthTokenResponse.class, new OAuthTokenResponseDeserializer())
        .addSerializer(S3SignRequest.class, new S3SignRequestSerializer<>())
        .addSerializer(ImmutableS3SignRequest.class, new S3SignRequestSerializer<>())
        .addDeserializer(S3SignRequest.class, new S3SignRequestDeserializer<>())
        .addDeserializer(ImmutableS3SignRequest.class, new S3SignRequestDeserializer<>())
        .addSerializer(S3SignResponse.class, new S3SignResponseSerializer<>())
        .addSerializer(ImmutableS3SignResponse.class, new S3SignResponseSerializer<>())
        .addDeserializer(S3SignResponse.class, new S3SignResponseDeserializer<>())
        .addDeserializer(ImmutableS3SignResponse.class, new S3SignResponseDeserializer<>());
  }

  public static class S3SignRequestSerializer<T extends S3SignRequest> extends JsonSerializer<T> {
    @Override
    public void serialize(T request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      S3SignRequestParser.toJson(request, gen);
    }
  }

  public static class S3SignRequestDeserializer<T extends S3SignRequest>
      extends JsonDeserializer<T> {
    @Override
    public T deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return (T) S3SignRequestParser.fromJson(jsonNode);
    }
  }

  public static class S3SignResponseSerializer<T extends S3SignResponse> extends JsonSerializer<T> {
    @Override
    public void serialize(T request, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      S3SignResponseParser.toJson(request, gen);
    }
  }

  public static class S3SignResponseDeserializer<T extends S3SignResponse>
      extends JsonDeserializer<T> {
    @Override
    public T deserialize(JsonParser p, DeserializationContext context) throws IOException {
      JsonNode jsonNode = p.getCodec().readTree(p);
      return (T) S3SignResponseParser.fromJson(jsonNode);
    }
  }
}
