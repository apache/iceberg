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

package org.apache.iceberg;

import org.apache.iceberg.util.SnapshotUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshotUtil extends TableTestBase {
    @Parameterized.Parameters(name = "formatVersion = {0}")
    public static Object[] parameters() {
        return new Object[]{1, 2};
    }

    public TestSnapshotUtil(int formatVersion) {
        super(formatVersion);
    }

    @Test
    public void testSnapshotUtil() {
        Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());
        Snapshot oldestSnapshot = SnapshotUtil.oldestSnapshot(table);
        Assert.assertNull("Table snapshot is should be null", oldestSnapshot);

        table.newFastAppend()
                .appendFile(FILE_A)
                .commit();
        Snapshot firstSnapshot = table.currentSnapshot();

        table.newFastAppend()
                .appendFile(FILE_B)
                .commit();
        Snapshot secondSnapshot = table.currentSnapshot();

        table.newFastAppend()
                .appendFile(FILE_C)
                .commit();
        Snapshot thirdSnapshot = table.currentSnapshot();

        oldestSnapshot = SnapshotUtil.oldestSnapshot(table);
        Assert.assertEquals("Table oldest snapshot should be first snapshot", firstSnapshot.snapshotId(), oldestSnapshot.snapshotId());

        table.expireSnapshots().expireOlderThan(System.currentTimeMillis()).retainLast(2).commit();
        table.refresh();
        oldestSnapshot = SnapshotUtil.oldestSnapshot(table);

        Assert.assertEquals("Table current snapshot should be third snapshot", thirdSnapshot.snapshotId(), table.currentSnapshot().snapshotId());
        Assert.assertEquals("Table oldest snapshot should be second snapshot", secondSnapshot.snapshotId(), oldestSnapshot.snapshotId());
    }

}
