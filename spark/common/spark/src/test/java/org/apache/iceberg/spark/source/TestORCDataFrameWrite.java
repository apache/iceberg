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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Test;

public class TestORCDataFrameWrite extends DataFrameWriteTestBase {
  @Override
  protected void configureTable(Table table) {
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.toString())
        .commit();
  }

  @Test
  @Override
  public void testUnknownListType() {
    assertThatThrownBy(super::testUnknownListType)
        .isInstanceOf(SparkException.class)
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot create ListType with unknown element type");
  }

  @Test
  @Override
  public void testUnknownMapType() {
    assertThatThrownBy(super::testUnknownMapType)
        .isInstanceOf(SparkException.class)
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot create MapType with unknown value type");
  }
}
