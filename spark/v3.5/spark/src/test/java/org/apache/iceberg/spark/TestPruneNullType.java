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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestPruneNullType {

  @Test
  public void testPruneTopLevelNullType() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("null", DataTypes.NullType, true, Metadata.empty())
            });

    StructType expectedSchema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty())
            });

    DataType actualSchema = PruneNullType.prune(schema);
    assertThat(actualSchema).isEqualTo(expectedSchema);
  }

  @Test
  public void testPruneNestedNullType() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "nested_null",
                  new StructType(
                      new StructField[] {
                        new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("null", DataTypes.NullType, true, Metadata.empty())
                      }),
                  true,
                  Metadata.empty())
            });

    StructType expectedSchema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "nested_null",
                  new StructType(
                      new StructField[] {
                        new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
                      }),
                  true,
                  Metadata.empty())
            });

    DataType actualSchema = PruneNullType.prune(schema);
    assertThat(actualSchema).isEqualTo(expectedSchema);
  }

  @Test
  public void testPruneTopLevelSingleNullType() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("null", DataTypes.NullType, true, Metadata.empty())
            });

    StructType expectedSchema = new StructType(new StructField[] {});

    DataType actualSchema = PruneNullType.prune(schema);
    assertThat(actualSchema).isEqualTo(expectedSchema);
  }

  @Test
  public void testPruneListNullType() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "list_of_null", new ArrayType(DataTypes.NullType, false), true, Metadata.empty())
            });

    StructType expectedSchema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty())
            });

    DataType actualSchema = PruneNullType.prune(schema);
    assertThat(actualSchema).isEqualTo(expectedSchema);
  }

  @Test
  public void testPruneListNestedNullType() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "list_of_null",
                  new ArrayType(
                      new StructType(
                          new StructField[] {
                            new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
                            new StructField("null", DataTypes.NullType, true, Metadata.empty())
                          }),
                      false),
                  true,
                  Metadata.empty())
            });

    StructType expectedSchema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "list_of_null",
                  new ArrayType(
                      new StructType(
                          new StructField[] {
                            new StructField("int", DataTypes.IntegerType, false, Metadata.empty())
                          }),
                      false),
                  true,
                  Metadata.empty())
            });
    DataType actualSchema = PruneNullType.prune(schema);
    assertThat(actualSchema).isEqualTo(expectedSchema);
  }

  @Test
  public void testPruneMapNullType() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "map_of_null",
                  new MapType(DataTypes.IntegerType, DataTypes.NullType, true),
                  false,
                  Metadata.empty())
            });

    StructType expectedSchema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty())
            });

    assertThatThrownBy(() -> PruneNullType.prune(schema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create a map with a with a NullType value");
  }

  @Test
  public void testPruneMapNestedNullType() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "map_of_null",
                  new MapType(
                      DataTypes.IntegerType,
                      new StructType(
                          new StructField[] {
                            new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
                            new StructField("null", DataTypes.NullType, true, Metadata.empty())
                          }),
                      true),
                  false,
                  Metadata.empty())
            });

    StructType expectedSchema =
        new StructType(
            new StructField[] {
              new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField(
                  "map_of_null",
                  new MapType(
                      DataTypes.IntegerType,
                      new StructType(
                          new StructField[] {
                            new StructField("int", DataTypes.IntegerType, false, Metadata.empty()),
                          }),
                      true),
                  false,
                  Metadata.empty())
            });

    DataType actualSchema = PruneNullType.prune(schema);
    assertThat(actualSchema).isEqualTo(expectedSchema);
  }
}
