package org.apache.iceberg.hadoop;

import com.google.common.collect.Lists;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PrimaryKeySpec;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class TestHadoopMutableTable extends HadoopMutableTableBase {
  @Test
  public void testCreateTable() throws Exception {
    PartitionSpec expectedSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
            .bucket("data", 16)
            .build();

    PrimaryKeySpec expectedPkSpec = PrimaryKeySpec.builderFor(TABLE_SCHEMA)
            .addColumn("id")
            .build();

    Assert.assertEquals("Table schema should match schema with reassigned ids",
            TABLE_SCHEMA.asStruct(), table.schema().asStruct());
    Assert.assertEquals("Table delta schema should match schema with reassigned ids",
            DELTA_TABLE_SCHEMA.asStruct(), table.deltaTable().schema().asStruct());
    Assert.assertEquals("Table partition spec should match with reassigned ids",
            expectedSpec, table.spec());
    Assert.assertEquals("Table primary key spec should match with reassigned ids",
            expectedPkSpec, table.pkSpec());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should not create any scan tasks", 0, tasks.size());

    Assert.assertTrue("Table location should exist", tableDir.exists());
    Assert.assertTrue("Should create metadata folder",
            metadataDir.exists() && metadataDir.isDirectory());
    Assert.assertTrue("Should create v1 metadata",
            version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
            version(2).exists());
    Assert.assertTrue("Should create version hint file",
            versionHintFile.exists());
    Assert.assertEquals("Should write the current version to the hint file",
            1, readVersionHint());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testSchemaUpdate() throws Exception {
    Assert.assertTrue("Should create v1 metadata",
            version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
            version(2).exists());

    table.updateSchema()
            .addColumn("n", Types.IntegerType.get())
            .commit();

    Assert.assertTrue("Should create v2 for the update",
            version(2).exists() && version(2).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
            2, readVersionHint());

    Assert.assertEquals("Table schema should match schema with reassigned ids",
            UPDATED_SCHEMA.asStruct(), table.schema().asStruct());
    Assert.assertEquals("Table delta schema should match schema with reassigned ids",
            DELTA_TABLE_UPDATE_SCHEMA.asStruct(), table.deltaTable().schema().asStruct());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should not create any scan tasks", 0, tasks.size());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testAppendDelta() throws Exception {
    // first append
    table.newDeltaAppend()
            .appendFile(DELTA_FILE_A)
            .commit();
  }

}
