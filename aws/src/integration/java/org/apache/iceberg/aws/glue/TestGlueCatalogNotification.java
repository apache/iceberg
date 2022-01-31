/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.aws.glue;

import com.google.common.collect.Iterables;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.sns.SNSListener;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.junit.Test;

public class TestGlueCatalogNotification extends GlueTestBase{
  @Test
  public void testNotifyOnCreateSnapshotEvent() {
    Listeners.register(new SNSListener(testARN, sns), CreateSnapshotEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
  }

  @Test
  public void testNotifyOnScanEvent() {
    Listeners.register(new SNSListener(testARN, sns), ScanEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
    table.refresh();

    table.newScan().planFiles();
  }

  @Test
  public void testNotifyOnIncrementalScan() {
    Listeners.register(new SNSListener(testARN, sns), IncrementalScanEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
    table.newAppend().appendFile(testDataFile).commit();
    table.refresh();

    Iterable<Snapshot> snapshots = table.snapshots();
    table.newScan().appendsBetween(Iterables.get(snapshots, 0).snapshotId(), Iterables.get(snapshots, 1).snapshotId()).planFiles();
  }

  @Test
  public void testNotifyOnAllEvents() {
    SNSListener snsListener = new SNSListener(testARN, sns);
    Listeners.register(snsListener, CreateSnapshotEvent.class);
    Listeners.register(snsListener, ScanEvent.class);
    Listeners.register(snsListener, IncrementalScanEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
    table.newScan().planFiles();

    table.newAppend().appendFile(testDataFile).commit();
    table.refresh();

    Iterable<Snapshot> snapshots = table.snapshots();
    table.newScan().appendsBetween(Iterables.get(snapshots, 0).snapshotId(), Iterables.get(snapshots, 1).snapshotId()).planFiles();
  }
}
