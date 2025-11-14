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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestSchemaUpdate {

  @Test
  public void testAddColumn() {
    SchemaUpdate.Consumer updateConsumer = new SchemaUpdate.Consumer();
    updateConsumer.addColumn("parent", "name", Types.StringType.get());
    assertThat(updateConsumer.addColumns()).hasSize(1);
    assertThat(updateConsumer.updateTypes()).isEmpty();
    assertThat(updateConsumer.makeOptionals()).isEmpty();

    SchemaUpdate.AddColumn addColumn = updateConsumer.addColumns().iterator().next();
    assertThat(addColumn.parentName()).isEqualTo("parent");
    assertThat(addColumn.name()).isEqualTo("name");
    assertThat(addColumn.type()).isEqualTo(Types.StringType.get());
    assertThat(addColumn.defaultValue()).isNull();
  }

  @Test
  public void testAddColumnWithDefaultValue() {
    SchemaUpdate.Consumer updateConsumer = new SchemaUpdate.Consumer();
    Literal<String> defaultValue = Expressions.lit("default_value");
    updateConsumer.addColumn("parent", "name", Types.StringType.get(), defaultValue);
    assertThat(updateConsumer.addColumns()).hasSize(1);
    assertThat(updateConsumer.updateTypes()).isEmpty();
    assertThat(updateConsumer.makeOptionals()).isEmpty();

    SchemaUpdate.AddColumn addColumn = updateConsumer.addColumns().iterator().next();
    assertThat(addColumn.parentName()).isEqualTo("parent");
    assertThat(addColumn.name()).isEqualTo("name");
    assertThat(addColumn.type()).isEqualTo(Types.StringType.get());
    assertThat(addColumn.defaultValue()).isEqualTo(defaultValue);
    assertThat(addColumn.defaultValue().value()).isEqualTo("default_value");
  }

  @Test
  public void testAddColumnWithNumericDefaultValue() {
    SchemaUpdate.Consumer updateConsumer = new SchemaUpdate.Consumer();
    Literal<Integer> defaultValue = Expressions.lit(42);
    updateConsumer.addColumn(null, "age", Types.IntegerType.get(), defaultValue);

    SchemaUpdate.AddColumn addColumn = updateConsumer.addColumns().iterator().next();
    assertThat(addColumn.parentName()).isNull();
    assertThat(addColumn.name()).isEqualTo("age");
    assertThat(addColumn.type()).isEqualTo(Types.IntegerType.get());
    assertThat(addColumn.defaultValue()).isEqualTo(defaultValue);
    assertThat(addColumn.defaultValue().value()).isEqualTo(42);
  }

  @Test
  public void testAddColumnWithBooleanDefaultValue() {
    SchemaUpdate.Consumer updateConsumer = new SchemaUpdate.Consumer();
    Literal<Boolean> defaultValue = Expressions.lit(true);
    updateConsumer.addColumn(null, "active", Types.BooleanType.get(), defaultValue);

    SchemaUpdate.AddColumn addColumn = updateConsumer.addColumns().iterator().next();
    assertThat(addColumn.name()).isEqualTo("active");
    assertThat(addColumn.type()).isEqualTo(Types.BooleanType.get());
    assertThat(addColumn.defaultValue()).isEqualTo(defaultValue);
    assertThat(addColumn.defaultValue().value()).isEqualTo(true);
  }

  @Test
  public void testUpdateType() {
    SchemaUpdate.Consumer updateConsumer = new SchemaUpdate.Consumer();
    updateConsumer.updateType("name", Types.LongType.get());
    assertThat(updateConsumer.addColumns()).isEmpty();

    assertThat(updateConsumer.updateTypes()).hasSize(1);
    assertThat(updateConsumer.makeOptionals()).isEmpty();

    SchemaUpdate.UpdateType updateType = updateConsumer.updateTypes().iterator().next();
    assertThat(updateType.name()).isEqualTo("name");
    assertThat(updateType.type()).isEqualTo(Types.LongType.get());
  }

  @Test
  public void testMakeOptional() {
    SchemaUpdate.Consumer updateConsumer = new SchemaUpdate.Consumer();
    updateConsumer.makeOptional("name");
    assertThat(updateConsumer.addColumns()).isEmpty();

    assertThat(updateConsumer.updateTypes()).isEmpty();
    assertThat(updateConsumer.makeOptionals()).hasSize(1);

    SchemaUpdate.MakeOptional makeOptional = updateConsumer.makeOptionals().iterator().next();
    assertThat(makeOptional.name()).isEqualTo("name");
  }
}
