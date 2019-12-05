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

package org.apache.iceberg.hadoop;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.junit.Assert;
import org.junit.Test;

public class TestHadoopCatalog extends HadoopTableTestBase {
  @Test
  public void testBasicCatalog() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    catalog.dropTable(testTable);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testDropTable() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    catalog.dropTable(testTable);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testRenameTable() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    TableIdentifier testTable = TableIdentifier.of("db", "tbl1");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    AssertHelpers.assertThrows("should throw exception", UnsupportedOperationException.class,
        "Cannot rename Hadoop tables", () -> {
          catalog.renameTable(testTable, TableIdentifier.of("db", "tbl2"));
        }
    );
  }

  @Test
  public void testListTables() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of("db", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns1", "tbl3");

    Lists.newArrayList(tbl1, tbl2, tbl3).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    List<TableIdentifier> tbls1 = catalog.listTables(Namespace.of("db"));
    Set<String> tblSet = Sets.newHashSet(tbls1.stream().map(t -> t.name()).iterator());
    Assert.assertEquals(tblSet.size(), 2);
    Assert.assertTrue(tblSet.contains("tbl1"));
    Assert.assertTrue(tblSet.contains("tbl2"));

    List<TableIdentifier> tbls2 = catalog.listTables(Namespace.of("db", "ns1"));
    Assert.assertEquals(tbls2.size(), 1);
    Assert.assertTrue(tbls2.get(0).name().equals("tbl3"));

    AssertHelpers.assertThrows("should throw exception", NotFoundException.class,
        "Unknown namespace", () -> {
        catalog.listTables(Namespace.of("db", "ns1", "ns2"));
      });
  }

  @Test
  public void testCreateNamespace() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");

    Lists.newArrayList(tbl1, tbl2).forEach(t ->
        catalog.createNamespace(t.namespace())
    );

    String metaLocation1 = warehousePath + "/" + "db/ns1/ns2";
    FileSystem fs1 = Util.getFs(new Path(metaLocation1), conf);
    Assert.assertTrue(fs1.isDirectory(new Path(metaLocation1)));

    String metaLocation2 = warehousePath + "/" + "db/ns2/ns3";
    FileSystem fs2 = Util.getFs(new Path(metaLocation2), conf);
    Assert.assertTrue(fs2.isDirectory(new Path(metaLocation2)));

  }

  @Test
  public void testListNamespace() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    List<Namespace> nsp1 = catalog.listNamespaces(Namespace.of("db"));

    Set<String> tblSet = Sets.newHashSet(nsp1.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(tblSet.size(), 3);
    Assert.assertTrue(tblSet.contains("db.ns1"));
    Assert.assertTrue(tblSet.contains("db.ns2"));
    Assert.assertTrue(tblSet.contains("db.ns3"));

    List<Namespace> nsp2 = catalog.listNamespaces(Namespace.of("db", "ns1"));
    Assert.assertEquals(nsp2.size(), 1);
    Assert.assertTrue(nsp2.get(0).toString().equals("db.ns1.ns2"));

    List<Namespace> nsp3 = catalog.listNamespaces();
    Set<String> tblSet2 = Sets.newHashSet(nsp3.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(tblSet2.size(), 1);
    Assert.assertTrue(tblSet2.contains("db"));
    AssertHelpers.assertThrows("should throw exception", NotFoundException.class,
        "Unknown namespace", () -> {
          catalog.listNamespaces(Namespace.of("db", "db2", "ns2"));
        });
  }

  @Test
  public void testLoadNamespaceMeta() throws IOException {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    catalog.loadNamespaceMetadata(Namespace.of("db"));

    AssertHelpers.assertThrows("should throw exception", NotFoundException.class,
        "Unknown namespace", () -> {
          catalog.loadNamespaceMetadata(Namespace.of("db", "db2", "ns2"));
        });
  }

  @Test
  public void testDropNamespace() throws IOException {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of("db1", "ns1", "tbl1");

    Lists.newArrayList(tbl1, tbl2).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "This Namespace have tables, cannot drop it", () -> {
          catalog.dropNamespace(Namespace.of("db"));
        });
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "This Namespace have sub Namespace, cannot drop it", () -> {
          catalog.dropNamespace(Namespace.of("db1"));
        });
    catalog.dropTable(tbl1);
    catalog.dropNamespace(Namespace.of("db"));
    String metaLocation = warehousePath + "/" + "db";
    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

}
