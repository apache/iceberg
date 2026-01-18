package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestBranchSchemaUpdate extends TestBase {

  @TempDir private File tableDir;

  private static final Schema INITIAL_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();

  private static final DataFile FILE_A =
      DataFiles.builder(UNPARTITIONED)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(UNPARTITIONED)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  @Test
  public void testSchemaUpdateOnBranchStoresSchemaId() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test_schema_update_on_branch", INITIAL_SCHEMA, UNPARTITIONED, 2);

    // Create initial snapshot on main
    table.newAppend().appendFile(FILE_A).commit();
    long mainSnapshotId = table.currentSnapshot().snapshotId();

    // Create a branch
    table.manageSnapshots().createBranch("feature-branch", mainSnapshotId).commit();

    // Update schema on the branch
    table.updateSchema("feature-branch").addColumn("newColumn", Types.StringType.get()).commit();

    // Verify the branch has a schemaId
    SnapshotRef branchRef = table.refs().get("feature-branch");
    assertThat(branchRef).isNotNull();
    assertThat(branchRef.schemaId()).isNotNull();
    assertThat(branchRef.schemaId()).isGreaterThan(0);

    // Verify main branch doesn't have the new column
    assertThat(table.schema().findField("newColumn")).isNull();

    // Verify the schema associated with the branch has the new column
    Schema branchSchema = table.ops().current().schemasById().get(branchRef.schemaId());
    assertThat(branchSchema.findField("newColumn")).isNotNull();
  }

  @Test
  public void testMultipleSchemaUpdatesOnBranch() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test_multiple_schema_updates", INITIAL_SCHEMA, UNPARTITIONED, 2);

    table.newAppend().appendFile(FILE_A).commit();
    long mainSnapshotId = table.currentSnapshot().snapshotId();

    table.manageSnapshots().createBranch("feature-branch", mainSnapshotId).commit();

    // First schema update on branch
    table.updateSchema("feature-branch").addColumn("newColumn", Types.StringType.get()).commit();

    Integer firstSchemaId = table.refs().get("feature-branch").schemaId();
    assertThat(firstSchemaId).isNotNull();

    // Second schema update on branch
    table.updateSchema("feature-branch").addColumn("anotherColumn", Types.LongType.get()).commit();

    Integer secondSchemaId = table.refs().get("feature-branch").schemaId();
    assertThat(secondSchemaId).isNotNull();
    assertThat(secondSchemaId).isGreaterThan(firstSchemaId);

    // Verify the latest schema has both new columns
    Schema latestBranchSchema = table.ops().current().schemasById().get(secondSchemaId);
    assertThat(latestBranchSchema.findField("newColumn")).isNotNull();
    assertThat(latestBranchSchema.findField("anotherColumn")).isNotNull();

    // Verify main schema is unchanged
    assertThat(table.schema().findField("newColumn")).isNull();
    assertThat(table.schema().findField("anotherColumn")).isNull();
  }

  @Test
  public void testSnapshotUtilSchemaResolutionForBranch() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test_snapshot_util_schema_resolution", INITIAL_SCHEMA, UNPARTITIONED, 2);

    table.newAppend().appendFile(FILE_A).commit();
    long mainSnapshotId = table.currentSnapshot().snapshotId();

    table.manageSnapshots().createBranch("feature-branch", mainSnapshotId).commit();

    // Update schema on the branch
    table.updateSchema("feature-branch").addColumn("branchColumn", Types.StringType.get()).commit();

    // Verify SnapshotUtil returns the correct schema for the branch
    Schema branchSchema = SnapshotUtil.schemaFor(table, "feature-branch");
    assertThat(branchSchema.findField("branchColumn")).isNotNull();

    // Verify SnapshotUtil returns the main schema for main branch
    Schema mainSchema = SnapshotUtil.schemaFor(table, SnapshotRef.MAIN_BRANCH);
    assertThat(mainSchema.findField("branchColumn")).isNull();
  }

  @Test
  public void testSnapshotUtilSchemaResolutionForBranchWithoutSchemaId() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test_snapshot_util_no_schema_id", INITIAL_SCHEMA, UNPARTITIONED, 2);

    table.newAppend().appendFile(FILE_A).commit();
    long mainSnapshotId = table.currentSnapshot().snapshotId();

    // Create a branch without schema update
    table.manageSnapshots().createBranch("feature-branch", mainSnapshotId).commit();

    // Branch should use main schema when schemaId is not set
    Schema branchSchema = SnapshotUtil.schemaFor(table, "feature-branch");
    Schema mainSchema = SnapshotUtil.schemaFor(table, SnapshotRef.MAIN_BRANCH);

    assertThat(branchSchema.asStruct()).isEqualTo(mainSchema.asStruct());
  }

  @Test
  public void testTableMetadataUpdateSchemaWithBranch() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test_table_metadata_update_schema", INITIAL_SCHEMA, UNPARTITIONED, 2);

    table.newAppend().appendFile(FILE_A).commit();
    table
        .manageSnapshots()
        .createBranch("feature-branch", table.currentSnapshot().snapshotId())
        .commit();

    // Update schema on branch
    table.updateSchema("feature-branch").addColumn("newColumn", Types.StringType.get()).commit();

    TableMetadata metadata = table.ops().current();

    // Verify branch ref has schemaId
    SnapshotRef branchRef = metadata.ref("feature-branch");
    assertThat(branchRef.schemaId()).isNotNull();

    // Verify the schema is in the schemas map
    assertThat(metadata.schemasById()).containsKey(branchRef.schemaId());

    // Verify main schema is unchanged
    assertThat(metadata.schema().findField("newColumn")).isNull();
  }

  @Test
  public void testSnapshotProducerUsesBranchSchemaId() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir,
            "test_snapshot_producer_uses_branch_schema_id",
            INITIAL_SCHEMA,
            UNPARTITIONED,
            2);

    table.newAppend().appendFile(FILE_A).commit();
    table
        .manageSnapshots()
        .createBranch("feature-branch", table.currentSnapshot().snapshotId())
        .commit();

    // Update schema on the branch
    table.updateSchema("feature-branch").addColumn("newColumn", Types.StringType.get()).commit();

    Integer branchSchemaId = table.refs().get("feature-branch").schemaId();

    // Create a new snapshot on the branch
    table.newAppend().appendFile(FILE_B).toBranch("feature-branch").commit();

    // Get the latest snapshot on the branch
    Snapshot branchSnapshot = table.snapshot("feature-branch");

    // Verify the snapshot uses the branch's schema ID
    assertThat(branchSnapshot.schemaId()).isEqualTo(branchSchemaId);
  }

  @Test
  public void testMainBranchSchemaUpdateDoesNotAffectBranch() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test_main_branch_schema_update", INITIAL_SCHEMA, UNPARTITIONED, 2);

    table.newAppend().appendFile(FILE_A).commit();
    table
        .manageSnapshots()
        .createBranch("feature-branch", table.currentSnapshot().snapshotId())
        .commit();

    // Update schema on the branch
    table.updateSchema("feature-branch").addColumn("branchColumn", Types.StringType.get()).commit();

    Integer branchSchemaId = table.refs().get("feature-branch").schemaId();

    // Update schema on main
    table.updateSchema().addColumn("mainColumn", Types.StringType.get()).commit();

    // Verify branch still has its own schema
    assertThat(table.refs().get("feature-branch").schemaId()).isEqualTo(branchSchemaId);

    Schema branchSchema = table.ops().current().schemasById().get(branchSchemaId);
    assertThat(branchSchema.findField("branchColumn")).isNotNull();
    assertThat(branchSchema.findField("mainColumn")).isNull();

    // Verify main has its own schema
    assertThat(table.schema().findField("mainColumn")).isNotNull();
    assertThat(table.schema().findField("branchColumn")).isNull();
  }

  @Test
  public void testNullBranchDefaultsToMain() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test_null_branch_defaults_to_main", INITIAL_SCHEMA, UNPARTITIONED, 2);

    // When no branch is specified, it should update the main schema
    table.updateSchema().addColumn("newColumn", Types.StringType.get()).commit();

    assertThat(table.schema().findField("newColumn")).isNotNull();
    assertThat(table.refs().get(SnapshotRef.MAIN_BRANCH)).isNull();
  }
}
