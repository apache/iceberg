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
package org.apache.iceberg.flink;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.TimeUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class FlinkConfParser {

  private final Map<String, String> tableProperties;
  private final Map<String, String> options;
  private final ReadableConfig readableConfig;

  FlinkConfParser(Table table, Map<String, String> options, ReadableConfig readableConfig) {
    this.tableProperties = table.properties();
    this.options = options;
    this.readableConfig = readableConfig;
  }

  public BooleanConfParser booleanConf() {
    return new BooleanConfParser();
  }

  public IntConfParser intConf() {
    return new IntConfParser();
  }

  public LongConfParser longConf() {
    return new LongConfParser();
  }

  public DoubleConfParser doubleConf() {
    return new DoubleConfParser();
  }

  public <E extends Enum<E>> EnumConfParser<E> enumConfParser(Class<E> enumClass) {
    return new EnumConfParser<>(enumClass);
  }

  public StringConfParser stringConf() {
    return new StringConfParser();
  }

  public DurationConfParser durationConf() {
    return new DurationConfParser();
  }

  class BooleanConfParser extends ConfParser<BooleanConfParser, Boolean> {
    private Boolean defaultValue;

    @Override
    protected BooleanConfParser self() {
      return this;
    }

    public BooleanConfParser defaultValue(boolean value) {
      this.defaultValue = value;
      return self();
    }

    public BooleanConfParser defaultValue(String value) {
      this.defaultValue = Boolean.parseBoolean(value);
      return self();
    }

    public boolean parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(Boolean::parseBoolean, defaultValue);
    }
  }

  class IntConfParser extends ConfParser<IntConfParser, Integer> {
    private Integer defaultValue;

    @Override
    protected IntConfParser self() {
      return this;
    }

    public IntConfParser defaultValue(int value) {
      this.defaultValue = value;
      return self();
    }

    public int parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(Integer::parseInt, defaultValue);
    }

    public Integer parseOptional() {
      return parse(Integer::parseInt, null);
    }
  }

  class LongConfParser extends ConfParser<LongConfParser, Long> {
    private Long defaultValue;

    @Override
    protected LongConfParser self() {
      return this;
    }

    public LongConfParser defaultValue(long value) {
      this.defaultValue = value;
      return self();
    }

    public long parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(Long::parseLong, defaultValue);
    }

    public Long parseOptional() {
      return parse(Long::parseLong, null);
    }
  }

  class DoubleConfParser extends ConfParser<DoubleConfParser, Double> {
    private Double defaultValue;

    @Override
    protected DoubleConfParser self() {
      return this;
    }

    public DoubleConfParser defaultValue(double value) {
      this.defaultValue = value;
      return self();
    }

    public double parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(Double::parseDouble, defaultValue);
    }

    public Double parseOptional() {
      return parse(Double::parseDouble, null);
    }
  }

  class StringConfParser extends ConfParser<StringConfParser, String> {
    private String defaultValue;

    @Override
    protected StringConfParser self() {
      return this;
    }

    public StringConfParser defaultValue(String value) {
      this.defaultValue = value;
      return self();
    }

    public String parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(Function.identity(), defaultValue);
    }

    public String parseOptional() {
      return parse(Function.identity(), null);
    }
  }

  class EnumConfParser<E extends Enum<E>> extends ConfParser<EnumConfParser<E>, E> {
    private E defaultValue;
    private final Class<E> enumClass;

    EnumConfParser(Class<E> enumClass) {
      this.enumClass = enumClass;
    }

    @Override
    protected EnumConfParser<E> self() {
      return this;
    }

    public EnumConfParser<E> defaultValue(E value) {
      this.defaultValue = value;
      return self();
    }

    public E parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(s -> Enum.valueOf(enumClass, s), defaultValue);
    }

    public E parseOptional() {
      return parse(s -> Enum.valueOf(enumClass, s), null);
    }
  }

  class DurationConfParser extends ConfParser<DurationConfParser, Duration> {
    private Duration defaultValue;

    @Override
    protected DurationConfParser self() {
      return this;
    }

    public DurationConfParser defaultValue(Duration value) {
      this.defaultValue = value;
      return self();
    }

    public Duration parse() {
      Preconditions.checkArgument(defaultValue != null, "Default value cannot be null");
      return parse(TimeUtils::parseDuration, defaultValue);
    }

    public Duration parseOptional() {
      return parse(TimeUtils::parseDuration, null);
    }
  }

  abstract class ConfParser<ThisT, T> {
    private final List<String> optionNames = Lists.newArrayList();
    private String tablePropertyName;
    private ConfigOption<T> configOption;

    protected abstract ThisT self();

    public ThisT option(String name) {
      this.optionNames.add(name);
      return self();
    }

    public ThisT flinkConfig(ConfigOption<T> newConfigOption) {
      this.configOption = newConfigOption;
      return self();
    }

    public ThisT tableProperty(String name) {
      this.tablePropertyName = name;
      return self();
    }

    protected T parse(Function<String, T> conversion, T defaultValue) {
      if (!optionNames.isEmpty()) {
        for (String optionName : optionNames) {
          String optionValue = options.get(optionName);
          if (optionValue != null) {
            return conversion.apply(optionValue);
          }
        }
      }

      if (configOption != null) {
        T propertyValue = readableConfig.get(configOption);
        if (propertyValue != null) {
          return propertyValue;
        }
      }

      if (tablePropertyName != null) {
        String propertyValue = tableProperties.get(tablePropertyName);
        if (propertyValue != null) {
          return conversion.apply(propertyValue);
        }
      }

      return defaultValue;
    }
  }
}
