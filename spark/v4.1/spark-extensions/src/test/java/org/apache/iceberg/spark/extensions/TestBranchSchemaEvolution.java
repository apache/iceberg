package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBranchSchemaEvolution extends ExtensionsTestBase {

  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties()
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      }
    };
  }

  @TestTemplate
  public void testCreateBranchAndEvolveSchema() throws NoSuchTableException {
    // Insert initial data on main branch
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    // Create a feature branch
    sql("ALTER TABLE %s CREATE BRANCH feature_branch", tableName);

    // Add a new column to the feature branch table
    sql("ALTER TABLE %s.branch_feature_branch ADD COLUMN new_col INT", tableName);

    table.refresh();

    // Verify main branch schema hasn't changed
    assertThat(table.schema().findField("new_col"))
        .as("Main branch should not have new_col")
        .isNull();

    // Verify branch ref has schema ID
    SnapshotRef branchRef = table.refs().get("feature_branch");
    assertThat(branchRef).isNotNull();
    assertThat(branchRef.schemaId()).as("Branch should have a schema ID").isNotNull();

    // Verify the branch schema has the new column
    Schema branchSchema = table.schemas().get(branchRef.schemaId());
    assertThat(branchSchema.findField("new_col"))
        .as("Branch schema should have new_col")
        .isNotNull();
  }

  @TestTemplate
  public void testWriteToBranchWithEvolvedSchema() throws NoSuchTableException {
    // Insert initial data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    // Create branch
    sql("ALTER TABLE %s CREATE BRANCH feature_branch", tableName);

    // Add column to branch
    sql("ALTER TABLE %s.branch_feature_branch ADD COLUMN age INT", tableName);

    // Insert data to branch with new schema
    sql("INSERT INTO %s.branch_feature_branch VALUES (3, 'c', 25), (4, 'd', 30)", tableName);

    // Read from branch and verify new column is present
    List<Object[]> branchData =
        sql("SELECT * FROM %s.branch_feature_branch ORDER BY id", tableName);
    assertThat(branchData).hasSize(4);

    // Verify the new rows have the age column
    assertThat(branchData.get(2)).hasSize(3); // id, data, age
    assertThat(branchData.get(2)[2]).isEqualTo(25);
    assertThat(branchData.get(3)[2]).isEqualTo(30);

    // Verify main branch doesn't have the age column
    List<Object[]> mainData = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(mainData).hasSize(2);
    assertThat(mainData.get(0)).hasSize(2); // id, data only
  }

  @TestTemplate
  public void testReadFromBranchWithEvolvedSchema() throws NoSuchTableException {
    // Insert initial data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    // Create branch
    sql("ALTER TABLE %s CREATE BRANCH test_branch", tableName);

    // Add multiple columns to branch
    sql("ALTER TABLE %s.branch_test_branch ADD COLUMN age INT", tableName);
    sql("ALTER TABLE %s.branch_test_branch ADD COLUMN city STRING", tableName);

    // Insert data to branch
    sql(
        "INSERT INTO %s.branch_test_branch VALUES (3, 'c', 25, 'NYC'), (4, 'd', 30, 'SF')",
        tableName);

    // Read using branch version syntax
    List<Object[]> branchData =
        sql("SELECT * FROM %s VERSION AS OF 'test_branch' ORDER BY id", tableName);
    assertThat(branchData).hasSize(4);
    assertThat(branchData.get(2)).hasSize(4); // id, data, age, city

    // Read specific columns from branch
    List<Object[]> specificCols =
        sql("SELECT id, age, city FROM %s.branch_test_branch WHERE id > 2 ORDER BY id", tableName);
    assertThat(specificCols).hasSize(2);
    assertThat(specificCols.get(0)).containsExactly(3, 25, "NYC");
    assertThat(specificCols.get(1)).containsExactly(4, 30, "SF");

    // Verify main branch query fails with new columns
    assertThatThrownBy(() -> sql("SELECT age FROM %s", tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("age");
  }

  @TestTemplate
  public void testMultipleSchemaEvolutionsOnBranch() throws NoSuchTableException {
    // Insert initial data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    // Create branch
    sql("ALTER TABLE %s CREATE BRANCH multi_evolve_branch", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    // First evolution: Add column
    sql("ALTER TABLE %s.branch_multi_evolve_branch ADD COLUMN age INT", tableName);
    table.refresh();
    Integer firstSchemaId = table.refs().get("multi_evolve_branch").schemaId();

    // Second evolution: Add another column
    sql("ALTER TABLE %s.branch_multi_evolve_branch ADD COLUMN city STRING", tableName);
    table.refresh();
    Integer secondSchemaId = table.refs().get("multi_evolve_branch").schemaId();

    // Third evolution: Rename column
    sql("ALTER TABLE %s.branch_multi_evolve_branch RENAME COLUMN data TO name", tableName);
    table.refresh();
    Integer thirdSchemaId = table.refs().get("multi_evolve_branch").schemaId();

    // Verify each evolution created a new schema
    assertThat(firstSchemaId).isNotNull();
    assertThat(secondSchemaId).isGreaterThan(firstSchemaId);
    assertThat(thirdSchemaId).isGreaterThan(secondSchemaId);

    // Verify final schema has all changes
    Schema finalSchema = table.schemas().get(thirdSchemaId);
    assertThat(finalSchema.findField("age")).isNotNull();
    assertThat(finalSchema.findField("city")).isNotNull();
    assertThat(finalSchema.findField("name")).isNotNull();
    assertThat(finalSchema.findField("data")).isNull();

    // Insert and verify data with final schema
    sql("INSERT INTO %s.branch_multi_evolve_branch VALUES (3, 'Charlie', 25, 'NYC')", tableName);
    List<Object[]> data =
        sql("SELECT * FROM %s.branch_multi_evolve_branch WHERE id = 3", tableName);
    assertThat(data).hasSize(1);
    assertThat(data.get(0)).hasSize(4);
  }

  @TestTemplate
  public void testMainBranchSchemaUnaffectedByBranchEvolution() throws NoSuchTableException {
    // Insert initial data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Schema originalMainSchema = table.schema();

    // Create branch and evolve schema
    sql("ALTER TABLE %s CREATE BRANCH isolated_branch", tableName);
    sql("ALTER TABLE %s.branch_isolated_branch ADD COLUMN branch_col INT", tableName);
    sql("ALTER TABLE %s.branch_isolated_branch ADD COLUMN another_col STRING", tableName);

    // Verify main schema unchanged
    table.refresh();
    assertThat(table.schema().asStruct()).isEqualTo(originalMainSchema.asStruct());
    assertThat(table.schema().findField("branch_col")).isNull();
    assertThat(table.schema().findField("another_col")).isNull();

    // Write to main branch with original schema
    sql("INSERT INTO %s VALUES (3, 'c'), (4, 'd')", tableName);

    // Verify main branch data structure
    List<Object[]> mainData = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(mainData).hasSize(4);
    assertThat(mainData.get(0)).hasSize(2); // Original schema
  }

  @TestTemplate
  public void testBranchSchemaIsolation() throws NoSuchTableException {
    // Insert initial data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    // Create two branches
    sql("ALTER TABLE %s CREATE BRANCH branch_a", tableName);
    sql("ALTER TABLE %s CREATE BRANCH branch_b", tableName);

    // Evolve each branch differently
    sql("ALTER TABLE %s.branch_branch_a ADD COLUMN col_a INT", tableName);
    sql("ALTER TABLE %s.branch_branch_b ADD COLUMN col_b STRING", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    // Verify branch A has only col_a
    Integer schemaIdA = table.refs().get("branch_a").schemaId();
    Schema schemaA = table.schemas().get(schemaIdA);
    assertThat(schemaA.findField("col_a")).isNotNull();
    assertThat(schemaA.findField("col_b")).isNull();

    // Verify branch B has only col_b
    Integer schemaIdB = table.refs().get("branch_b").schemaId();
    Schema schemaB = table.schemas().get(schemaIdB);
    assertThat(schemaB.findField("col_b")).isNotNull();
    assertThat(schemaB.findField("col_a")).isNull();

    // Write and read from each branch
    sql("INSERT INTO %s.branch_branch_a VALUES (3, 'c', 100)", tableName);
    sql("INSERT INTO %s.branch_branch_b VALUES (4, 'd', 'test')", tableName);

    List<Object[]> dataA = sql("SELECT * FROM %s.branch_branch_a WHERE id = 3", tableName);
    assertThat(dataA.get(0)).hasSize(3); // id, data, col_a

    List<Object[]> dataB = sql("SELECT * FROM %s.branch_branch_b WHERE id = 4", tableName);
    assertThat(dataB.get(0)).hasSize(3); // id, data, col_b
  }

  @TestTemplate
  public void testMainBranchEvolutionDoesNotAffectExistingBranches() throws NoSuchTableException {
    // Insert initial data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    // Create branch
    sql("ALTER TABLE %s CREATE BRANCH stable_branch", tableName);
    sql("ALTER TABLE %s.branch_stable_branch ADD COLUMN branch_feature INT", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Integer branchSchemaId = table.refs().get("stable_branch").schemaId();

    // Evolve main branch
    sql("ALTER TABLE %s ADD COLUMN main_feature STRING", tableName);

    // Verify branch schema unchanged
    table.refresh();
    assertThat(table.refs().get("stable_branch").schemaId()).isEqualTo(branchSchemaId);

    Schema branchSchema = table.schemas().get(branchSchemaId);
    assertThat(branchSchema.findField("branch_feature")).isNotNull();
    assertThat(branchSchema.findField("main_feature")).isNull();

    // Verify main schema has new column
    assertThat(table.schema().findField("main_feature")).isNotNull();
    assertThat(table.schema().findField("branch_feature")).isNull();
  }

  @TestTemplate
  public void testDropColumnOnBranch() throws NoSuchTableException {
    // Create table with multiple columns
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, data STRING, temp STRING, status STRING) USING iceberg",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, 'a', 'temp1', 'active'), (2, 'b', 'temp2', 'inactive')",
        tableName);

    // Create branch
    sql("ALTER TABLE %s CREATE BRANCH cleanup_branch", tableName);

    // Drop column on branch
    sql("ALTER TABLE %s.branch_cleanup_branch DROP COLUMN temp", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    // Verify branch schema doesn't have temp column
    Integer branchSchemaId = table.refs().get("cleanup_branch").schemaId();
    Schema branchSchema = table.schemas().get(branchSchemaId);
    assertThat(branchSchema.findField("temp")).isNull();
    assertThat(branchSchema.findField("status")).isNotNull();

    // Verify main branch still has temp column
    assertThat(table.schema().findField("temp")).isNotNull();

    // Write to branch without temp column
    sql("INSERT INTO %s.branch_cleanup_branch VALUES (3, 'c', 'pending')", tableName);

    List<Object[]> branchData =
        sql("SELECT * FROM %s.branch_cleanup_branch WHERE id = 3", tableName);
    assertThat(branchData).hasSize(1);
    assertThat(branchData.get(0)).hasSize(3); // id, data, status
  }

  @TestTemplate
  public void testChangeColumnTypeOnBranch() throws NoSuchTableException {
    // Create table
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("CREATE TABLE %s (id INT, count INT, data STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100, 'a'), (2, 200, 'b')", tableName);

    // Create branch
    sql("ALTER TABLE %s CREATE BRANCH type_change_branch", tableName);

    // Change column type on branch (INT to BIGINT is a valid promotion)
    sql("ALTER TABLE %s.branch_type_change_branch ALTER COLUMN count TYPE BIGINT", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    // Verify branch schema has BIGINT
    Integer branchSchemaId = table.refs().get("type_change_branch").schemaId();
    Schema branchSchema = table.schemas().get(branchSchemaId);
    assertThat(branchSchema.findField("count").type()).isEqualTo(Types.LongType.get());

    // Verify main schema still has INT
    assertThat(table.schema().findField("count").type()).isEqualTo(Types.IntegerType.get());

    // Write large values to branch
    sql("INSERT INTO %s.branch_type_change_branch VALUES (3, 999999999999, 'c')", tableName);

    List<Object[]> branchData =
        sql("SELECT count FROM %s.branch_type_change_branch WHERE id = 3", tableName);
    assertThat(branchData.get(0)[0]).isEqualTo(999999999999L);
  }

  @TestTemplate
  public void testComplexSchemaEvolutionWorkflow() throws NoSuchTableException {
    // Simulate a realistic workflow: develop feature on branch, iterate schema, merge

    // 1. Setup: Create table and add initial data
    sql("INSERT INTO %s VALUES (1, 'initial'), (2, 'data')", tableName);

    // 2. Create feature branch
    sql("ALTER TABLE %s CREATE BRANCH feature_dev", tableName);

    // 3. First iteration: Add columns
    sql("ALTER TABLE %s.branch_feature_dev ADD COLUMN version INT", tableName);
    sql("ALTER TABLE %s.branch_feature_dev ADD COLUMN metadata STRING", tableName);

    // 4. Test with data
    sql("INSERT INTO %s.branch_feature_dev VALUES (3, 'test', 1, 'meta1')", tableName);

    // 5. Second iteration: Realize we need more fields
    sql("ALTER TABLE %s.branch_feature_dev ADD COLUMN created_at BIGINT", tableName);
    sql("ALTER TABLE %s.branch_feature_dev ADD COLUMN tags STRING", tableName);

    // 6. More testing
    sql(
        "INSERT INTO %s.branch_feature_dev VALUES (4, 'test2', 1, 'meta2', 1234567890, 'tag1,tag2')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    // Verify branch has all evolutions
    Integer branchSchemaId = table.refs().get("feature_dev").schemaId();
    Schema branchSchema = table.schemas().get(branchSchemaId);
    assertThat(branchSchema.columns()).hasSize(6); // id, data, version, metadata, created_at, tags

    // Verify main unchanged
    assertThat(table.schema().columns()).hasSize(2); // id, data

    // Verify branch data integrity
    List<Object[]> branchData =
        sql("SELECT * FROM %s.branch_feature_dev WHERE id > 2 ORDER BY id", tableName);
    assertThat(branchData).hasSize(2);
    assertThat(branchData.get(1)).hasSize(6);
    assertThat(branchData.get(1)[5]).isEqualTo("tag1,tag2");
  }

  @TestTemplate
  public void testQueryOptimizationWithBranchSchema() throws NoSuchTableException {
    // Test that Spark can optimize queries correctly with branch-specific schemas

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    // Create branch with indexed column
    sql("ALTER TABLE %s CREATE BRANCH optimized_branch", tableName);
    sql("ALTER TABLE %s.branch_optimized_branch ADD COLUMN indexed_col INT", tableName);

    // Insert data with new column
    sql(
        "INSERT INTO %s.branch_optimized_branch VALUES (4, 'd', 100), (5, 'e', 200), (6, 'f', 100)",
        tableName);

    // Query with filter on new column - should work efficiently
    List<Object[]> filtered =
        sql(
            "SELECT id, data FROM %s.branch_optimized_branch WHERE indexed_col = 100 ORDER BY id",
            tableName);
    assertThat(filtered).hasSize(2);
    assertThat(filtered.get(0)[0]).isEqualTo(4);
    assertThat(filtered.get(1)[0]).isEqualTo(6);

    // Aggregate query on new column
    List<Object[]> aggregated =
        sql(
            "SELECT indexed_col, COUNT(*) as cnt FROM %s.branch_optimized_branch WHERE indexed_col IS NOT NULL GROUP BY indexed_col ORDER BY indexed_col",
            tableName);
    assertThat(aggregated).hasSize(2);
    assertThat(aggregated.get(0)).containsExactly(100, 2L);
    assertThat(aggregated.get(1)).containsExactly(200, 1L);
  }

  @TestTemplate
  public void testSameColumnAddedToBranchAndMain() throws NoSuchTableException {
    // Test what happens when both branch and main add the same column
    // Do they share the same schema or have different schemas?

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    // Create a branch
    sql("ALTER TABLE %s CREATE BRANCH test_branch", tableName);

    // Add column 'new_col' to branch first
    sql("ALTER TABLE %s.branch_test_branch ADD COLUMN new_col STRING", tableName);

    // Add the same column 'new_col' to main
    sql("ALTER TABLE %s ADD COLUMN new_col STRING", tableName);

    // Load table and check schema IDs
    Table table = validationCatalog.loadTable(tableIdent);

    // Get branch schema ID
    SnapshotRef branchRef = table.refs().get("test_branch");
    assertThat(branchRef).isNotNull();
    Integer branchSchemaId = branchRef.schemaId();

    // Get main schema ID (from table.schema() since no new snapshot was created)
    Integer mainSchemaId = table.schema().schemaId();

    // Get the schemas
    Schema branchSchema = table.schemas().get(branchSchemaId);
    Schema mainSchema = table.schema();

    // Verify both schemas have the new_col column
    assertThat(branchSchema.findField("new_col")).isNotNull();
    assertThat(mainSchema.findField("new_col")).isNotNull();

    // Branch and main independently evolved, so they should have DIFFERENT schema IDs
    // Each schema evolution assigns a new unique schema ID
    assertThat(branchSchemaId).isNotEqualTo(mainSchemaId);

    // The schemas have the same column names but DIFFERENT field IDs
    // Because branch added 'new_col' first (got field ID 3), then main added it (got field ID 4)
    // This is expected behavior - independent schema evolutions get independent field IDs
    assertThat(branchSchema.findField("new_col").fieldId())
        .isNotEqualTo(mainSchema.findField("new_col").fieldId());

    // The column names are the same
    assertThat(branchSchema.findField("new_col").name())
        .isEqualTo(mainSchema.findField("new_col").name());
    assertThat(branchSchema.findField("new_col").type())
        .isEqualTo(mainSchema.findField("new_col").type());

    // Verify we can write to both with the new column
    sql("INSERT INTO %s.branch_test_branch VALUES (3, 'c', 'branch_value')", tableName);
    sql("INSERT INTO %s VALUES (4, 'd', 'main_value')", tableName);

    // Verify reads work correctly
    List<Object[]> branchData =
        sql("SELECT * FROM %s.branch_test_branch WHERE id >= 3 ORDER BY id", tableName);
    assertThat(branchData).hasSize(1);
    assertThat(branchData.get(0)[2]).isEqualTo("branch_value");

    List<Object[]> mainData = sql("SELECT * FROM %s WHERE id >= 3 ORDER BY id", tableName);
    assertThat(mainData).hasSize(1);
    assertThat(mainData.get(0)[2]).isEqualTo("main_value");
  }
}
