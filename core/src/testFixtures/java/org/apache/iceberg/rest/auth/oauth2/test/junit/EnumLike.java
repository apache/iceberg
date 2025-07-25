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
package org.apache.iceberg.rest.auth.oauth2.test.junit;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtil;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junitpioneer.jupiter.cartesian.CartesianArgumentsSource;
import org.junitpioneer.jupiter.cartesian.CartesianParameterArgumentsProvider;

/**
 * A JUnit 5 cartesian test parameter annotation that provides a stream of values from an
 * "Enum-like" type.
 *
 * <p>This annotation can be used with any type that exposes a set of public static final constants,
 * as long as their {@code toString()} method returns the constant's canonical string
 * representation.
 *
 * <p>This is the case for many Nimbus SDK types, such as {@link GrantType} and {@link
 * ClientAuthenticationMethod}.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@CartesianArgumentsSource(EnumLike.EnumLikeMethodArgumentsProvider.class)
public @interface EnumLike {

  String[] includes() default {};

  String[] excludes() default {};

  class EnumLikeMethodArgumentsProvider implements CartesianParameterArgumentsProvider<Object> {

    private static final ConcurrentMap<Class<?>, List<Object>> CONSTANTS_CACHE =
        Maps.newConcurrentMap();

    @Override
    public Stream<Object> provideArguments(ExtensionContext context, Parameter parameter) {
      EnumLike ann = parameter.getAnnotation(EnumLike.class);
      return enumLikeConstants(parameter.getType()).stream()
          .filter(constant -> applyIncludes(ann, constant))
          .filter(constant -> applyExcludes(ann, constant));
    }

    private List<Object> enumLikeConstants(Class<?> type) {
      return CONSTANTS_CACHE.computeIfAbsent(type, this::computeEnumLikeConstants);
    }

    private boolean applyIncludes(EnumLike ann, Object constant) {
      return ann.includes().length == 0
          || Stream.of(ann.includes()).anyMatch(name -> matchByName(name, constant));
    }

    private boolean applyExcludes(EnumLike ann, Object constant) {
      return ann.excludes().length == 0
          || Stream.of(ann.excludes()).noneMatch(name -> matchByName(name, constant));
    }

    private boolean matchByName(String name, Object constant) {
      // This relies on the toString() method of the constant to return its canonical string
      // representation.
      return name.equals(constant.toString());
    }

    private List<Object> computeEnumLikeConstants(Class<?> type) {
      if (type.equals(GrantType.class)) {
        return List.copyOf(ConfigUtil.SUPPORTED_GRANT_TYPES);
      } else if (type.equals(ClientAuthenticationMethod.class)) {
        return ConfigUtil.SUPPORTED_CLIENT_AUTH_METHODS.stream().map(Object.class::cast).toList();
      } else {
        return Arrays.stream(type.getFields())
            .filter(f -> constantField(type, f))
            .map(this::constantValue)
            .toList();
      }
    }

    private boolean constantField(Class<?> type, Field field) {
      return field.getModifiers() == (Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL)
          && field.getType().equals(type);
    }

    private Object constantValue(Field field) {
      try {
        return field.get(null);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
