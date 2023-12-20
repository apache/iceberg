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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.PathIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestPathIdentifier extends TestBase {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  @TempDir private Path temp;
  private File tableLocation;
  private PathIdentifier identifier;
  private SparkCatalog sparkCatalog;

  @BeforeEach
  public void before() throws IOException {
    tableLocation = temp.toFile();
    identifier = new PathIdentifier(tableLocation.getAbsolutePath());
    sparkCatalog = new SparkCatalog();
    sparkCatalog.initialize("test", new CaseInsensitiveStringMap(ImmutableMap.of()));
  }

  @AfterEach
  public void after() {
    tableLocation.delete();
    sparkCatalog = null;
  }

  @Test
  public void testPathIdentifier() throws TableAlreadyExistsException, NoSuchTableException {
    SparkTable table =
        (SparkTable)
            sparkCatalog.createTable(
                identifier, SparkSchemaUtil.convert(SCHEMA), new Transform[0], ImmutableMap.of());

    assertThat(table.table().location()).isEqualTo(tableLocation.getAbsolutePath());
    assertThat(table.table()).isInstanceOf(BaseTable.class);
    assertThat(((BaseTable) table.table()).operations()).isInstanceOf(HadoopTableOperations.class);

    assertThat(sparkCatalog.loadTable(identifier)).isEqualTo(table);
    assertThat(sparkCatalog.dropTable(identifier)).isTrue();
  }
}
