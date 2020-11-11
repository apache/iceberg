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
import com.dremio.nessie.model.Branch;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

/**
 * test tag operations with a default tag set by server.
 */
public class TestDefaultCatalogBranch extends BaseTestIceberg {

  public TestDefaultCatalogBranch() {
    super("main");
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  public void testBasicBranch() throws NessieNotFoundException, NessieConflictException {
    TableIdentifier foobar = TableIdentifier.of("foo", "bar");
    TableIdentifier foobaz = TableIdentifier.of("foo", "baz");
    createTable(foobar, 1); // table 1
    createTable(foobaz, 1); // table 2

    catalog.refresh();
    tree.createReference(Branch.of("FORWARD", catalog.currentHash()));
    hadoopConfig.set(NessieClient.CONF_NESSIE_REF, "FORWARD");
    NessieCatalog forwardCatalog = initCatalog("FORWARD");
    forwardCatalog.loadTable(foobaz).updateSchema().addColumn("id1", Types.LongType.get()).commit();
    forwardCatalog.loadTable(foobar).updateSchema().addColumn("id1", Types.LongType.get()).commit();
    Assert.assertNotEquals(metadataLocation(forwardCatalog, foobar),
                               metadataLocation(catalog, foobar));
    Assert.assertNotEquals(metadataLocation(forwardCatalog, foobaz),
                               metadataLocation(catalog, foobaz));

    System.out.println(metadataLocation(forwardCatalog, foobar));
    System.out.println(metadataLocation(catalog, foobar));

    forwardCatalog.refresh();
    tree.assignBranch("main",
        tree.getReferenceByName("main").getHash(),
        Branch.of("main", forwardCatalog.currentHash()));

    catalog.refresh();

    System.out.println(metadataLocation(forwardCatalog, foobar));
    System.out.println(metadataLocation(catalog, foobar));

    Assert.assertEquals(metadataLocation(forwardCatalog, foobar),
                            metadataLocation(catalog, foobar));
    Assert.assertEquals(metadataLocation(forwardCatalog, foobaz),
                            metadataLocation(catalog, foobaz));

    catalog.dropTable(foobar);
    catalog.dropTable(foobaz);
    tree.deleteBranch("FORWARD", tree.getReferenceByName("FORWARD").getHash());
  }

}
