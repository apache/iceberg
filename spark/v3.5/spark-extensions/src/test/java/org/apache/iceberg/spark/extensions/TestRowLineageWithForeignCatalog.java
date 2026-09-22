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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsDeleteV2;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies that the row lineage rules leave tables from other v2 catalogs alone. The rules run on
 * every resolved row-level command, not only on Iceberg tables.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestRowLineageWithForeignCatalog extends ExtensionsTestBase {

  private static final String FOREIGN_CATALOG = "foreign_catalog";
  private static final String FOREIGN_TABLE = FOREIGN_CATALOG + ".ns.foreign_table";

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.properties()
      }
    };
  }

  @AfterEach
  public void removeTables() {
    spark.conf().unset("spark.sql.catalog." + FOREIGN_CATALOG);
  }

  @TestTemplate
  public void deleteFromTableInAnotherCatalog() {
    spark.conf().set("spark.sql.catalog." + FOREIGN_CATALOG, ForeignCatalog.class.getName());
    sql("CREATE TABLE %s (id bigint, data string) USING foreign", FOREIGN_TABLE);

    assertThatCode(() -> sql("DELETE FROM %s WHERE id = 1", FOREIGN_TABLE))
        .doesNotThrowAnyException();
  }

  /** A minimal v2 catalog whose tables are not Iceberg tables. */
  public static class ForeignCatalog implements TableCatalog {
    private final Map<String, Table> tables = Maps.newHashMap();
    private String catalogName;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
      this.catalogName = name;
    }

    @Override
    public String name() {
      return catalogName;
    }

    @Override
    public Identifier[] listTables(String[] namespace) {
      return new Identifier[0];
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
      Table table = tables.get(ident.toString());
      if (table == null) {
        throw new NoSuchTableException(ident);
      }

      return table;
    }

    @Override
    public Table createTable(
        Identifier ident, StructType schema, Transform[] partitions, Map<String, String> props) {
      Table table = new ForeignTable(ident.name(), schema);
      tables.put(ident.toString(), table);
      return table;
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) {
      throw new UnsupportedOperationException("Cannot alter " + ident);
    }

    @Override
    public boolean dropTable(Identifier ident) {
      return tables.remove(ident.toString()) != null;
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) {
      throw new UnsupportedOperationException("Cannot rename " + oldIdent);
    }
  }

  /** An empty v2 table that accepts deletes by filter. */
  public static class ForeignTable implements Table, SupportsDeleteV2, SupportsRead {
    private final String tableName;
    private final StructType tableSchema;

    ForeignTable(String tableName, StructType tableSchema) {
      this.tableName = tableName;
      this.tableSchema = tableSchema;
    }

    @Override
    public String name() {
      return tableName;
    }

    @Override
    public StructType schema() {
      return tableSchema;
    }

    @Override
    public Set<TableCapability> capabilities() {
      return ImmutableSet.of(TableCapability.BATCH_READ);
    }

    @Override
    public void deleteWhere(Predicate[] predicates) {}

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
      return () ->
          new Scan() {
            @Override
            public StructType readSchema() {
              return tableSchema;
            }

            @Override
            public Batch toBatch() {
              return new Batch() {
                @Override
                public InputPartition[] planInputPartitions() {
                  return new InputPartition[0];
                }

                @Override
                public PartitionReaderFactory createReaderFactory() {
                  throw new UnsupportedOperationException("Cannot read " + tableName);
                }
              };
            }
          };
    }
  }
}
