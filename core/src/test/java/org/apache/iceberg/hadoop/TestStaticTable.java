package org.apache.iceberg.hadoop;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Test;

public class TestStaticTable extends HadoopTableTestBase {

  private Table getStaticTable() {
    return TABLES.load(((HasTableOperations) table).operations().current().metadataFileLocation());
  }

  private Table getStaticTable(MetadataTableType type) {
    return TABLES.load(((HasTableOperations) table).operations().current().metadataFileLocation() + "#" + type);
  }

  @Test
  public void testLoadFromMetadata() {
    Table staticTable = getStaticTable();
    Assert.assertTrue("Loading a metadata file based table should return StaticTableOperations",
        ((HasTableOperations) staticTable).operations() instanceof StaticTableOperations);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCannotBeAddedTo(){
    Table staticTable = getStaticTable();
    staticTable.newOverwrite().addFile(FILE_A).commit();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCannotBeDeletedFrom(){
    table.newAppend().appendFile(FILE_A).commit();
    Table staticTable = getStaticTable();
    staticTable.newDelete().deleteFile(FILE_A).commit();
  }

  @Test
  public void testHasSameProperties(){
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    table.newOverwrite().deleteFile(FILE_B).addFile(FILE_C).commit();
    Table staticTable = getStaticTable();
    Assert.assertTrue("Same history?",
        table.history().containsAll(staticTable.history()));
    Assert.assertTrue("Same snapshot?",
        table.currentSnapshot().snapshotId() ==  staticTable.currentSnapshot().snapshotId());
    Assert.assertTrue("Same properties?",
        Maps.difference(table.properties(), staticTable.properties()).areEqual());
  }

  @Test
  public void testImmutable() {
    table.newAppend().appendFile(FILE_A).commit();
    Table staticTable = getStaticTable();
    long originalSnapshot = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(FILE_B).commit();
    table.newOverwrite().deleteFile(FILE_B).addFile(FILE_C).commit();

    Assert.assertEquals("Snapshot unchanged after table modified",
        staticTable.currentSnapshot().snapshotId(), originalSnapshot);
  }

  @Test
  public void testMetadataTables() {
    for (MetadataTableType type: MetadataTableType.values()) {
      String enumName = type.name().replace("_","").toLowerCase();
      Assert.assertTrue("Should be able to get MetadataTable of type : " + type,
          getStaticTable(type).getClass().getName().toLowerCase().contains(enumName));
    }
  }


}
