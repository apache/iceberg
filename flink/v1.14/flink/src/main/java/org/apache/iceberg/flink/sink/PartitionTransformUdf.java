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
 * KIND, either express or implied.  See the Licenet ideajoinet ideajoin for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Literals;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

public class PartitionTransformUdf {

  private final String funcName;
  private final String srcColumn;
  private final int width;

  PartitionTransformUdf(String funcName, String srcColumn, int width) {
    this.funcName = funcName;
    this.srcColumn = srcColumn;
    this.width = width;
  }

  public String getFuncName() {
    return funcName;
  }

  public String getSrcColumn() {
    return srcColumn;
  }

  public int getWidth() {
    return width;
  }

  public static Builder newBuilder(String funcString) {
    return new Builder(funcString);
  }

  public static class Builder {
    private static final Pattern funcNamePattern =
        Pattern.compile("^`\\w+`\\.`\\w+`\\.`(\\w+)`\\((?:(\\d+), )?`(\\w+)`\\)$");
    private final String funcString;

    Builder(String funcString) {
      this.funcString = funcString;
    }

    public PartitionTransformUdf build() {
      Matcher matcher = funcNamePattern.matcher(funcString);
      ValidationException.check(matcher.matches(), "Invalid function format");

      int width = 0;
      if (matcher.group(2) != null) {
        width = Integer.parseInt(matcher.group(2));
      }
      return new PartitionTransformUdf(matcher.group(1), matcher.group(3), width);
    }
  }

  public static class Truncate extends ScalarFunction {
    public String eval(int num, @DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
      Type type = TypeUtil.fromJavaType(obj);
      Transform<Object, Object> truncate = Transforms.truncate(type, num);
      Object value = truncate.apply(Literals.fromJavaType(obj).to(type).value());
      return truncate.toHumanString(value);
    }
  }

  public static class Bucket extends ScalarFunction {
    public String eval(int num, @DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
      Type type = TypeUtil.fromJavaType(obj);
      Transform<Object, Integer> bucket = Transforms.bucket(type, num);
      Integer value = bucket.apply(Literals.fromJavaType(obj).to(type).value());
      return bucket.toHumanString(value);
    }
  }

  public static class Year extends ScalarFunction {
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
      Type type = TypeUtil.fromJavaType(obj);
      Transform<Object, Integer> year = Transforms.year(type);
      Integer value = year.apply(Literals.fromJavaType(obj).to(type).value());
      return year.toHumanString(value);
    }
  }

  public static class Month extends ScalarFunction {
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
      Type type = TypeUtil.fromJavaType(obj);
      Transform<Object, Integer> month = Transforms.month(type);
      Integer value = month.apply(Literals.fromJavaType(obj).to(type).value());
      return month.toHumanString(value);
    }
  }

  public static class Day extends ScalarFunction {
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
      Type type = TypeUtil.fromJavaType(obj);
      Transform<Object, Integer> day = Transforms.day(type);
      Integer value = day.apply(Literals.fromJavaType(obj).to(type).value());
      return day.toHumanString(value);
    }
  }

  public static class Hour extends ScalarFunction {
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
      Type type = TypeUtil.fromJavaType(obj);
      Transform<Object, Integer> hour = Transforms.hour(type);
      Integer value = hour.apply(Literals.fromJavaType(obj).to(type).value());
      return hour.toHumanString(value);
    }
  }
}
