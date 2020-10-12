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

package org.apache.iceberg.nessie;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestCatalogBranch extends BaseTestIceberg {

  public TestCatalogBranch() {
    super("main");
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  public void testBasicBranch() throws NessieNotFoundException, NessieConflictException {
    TableIdentifier foobar = TableIdentifier.of("foo", "bar");
    TableIdentifier foobaz = TableIdentifier.of("foo", "baz");
    Table bar = createTable(foobar, 1); // table 1
    createTable(foobaz, 1); // table 2
    catalog.refresh();
    createBranch("test", catalog.getHash());

    hadoopConfig.set(NessieClient.CONF_NESSIE_REF, "test");

    NessieCatalog newCatalog = new NessieCatalog(hadoopConfig);
    String initialMetadataLocation = getContent(newCatalog, foobar);
    Assert.assertEquals(initialMetadataLocation, getContent(catalog, foobar));
    Assert.assertEquals(getContent(newCatalog, foobaz), getContent(catalog, foobaz));
    bar.updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assert.assertNotEquals(getContent(catalog, foobar), getContent(newCatalog, foobar));

    // points to the previous metadata location
    Assert.assertEquals(initialMetadataLocation, getContent(newCatalog, foobar));
    initialMetadataLocation = getContent(newCatalog, foobaz);


    newCatalog.loadTable(foobaz).updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assert.assertNotEquals(getContent(catalog, foobaz), getContent(newCatalog, foobaz));

    // points to the previous metadata location
    Assert.assertEquals(initialMetadataLocation, getContent(catalog, foobaz));

    String mainHash = tree.getReferenceByName("main").getHash();
    tree.assignBranch("main", mainHash, newCatalog.getHash());
    Assert.assertEquals(getContent(newCatalog, foobar),
                            getContent(catalog, foobar));
    Assert.assertEquals(getContent(newCatalog, foobaz),
                            getContent(catalog, foobaz));
    catalog.dropTable(foobar);
    catalog.dropTable(foobaz);
    newCatalog.refresh();
    catalog.getTreeApi().deleteBranch("test", newCatalog.getHash());
  }

}
