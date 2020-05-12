package org.apache.iceberg.hadoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class HadoopMutableTableBase {
  // Schema passed to create tables
  static final Schema SCHEMA = new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get())
  );

  // This is the actual schema for the table, with column IDs reassigned
  static final Schema TABLE_SCHEMA = new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get())
  );

  static final Schema DELTA_TABLE_SCHEMA = new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()),
          required(3, PrimaryKeySpec.OFFSET_COLUMN, Types.LongType.get()),
          required(4, PrimaryKeySpec.DEL_COLUMN, Types.BooleanType.get())
  );

  static final Schema UPDATED_SCHEMA = new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()),
          optional(3, "n", Types.IntegerType.get())
  );

  static final Schema DELTA_TABLE_UPDATE_SCHEMA = new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()),
          optional(3, "n", Types.IntegerType.get()),
          required(4, PrimaryKeySpec.OFFSET_COLUMN, Types.LongType.get()),
          required(5, PrimaryKeySpec.DEL_COLUMN, Types.BooleanType.get())
  );

  // Partition spec used to create tables
  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
          .bucket("data", 16)
          .build();

  static final PrimaryKeySpec PK_SPEC = PrimaryKeySpec.builderFor(SCHEMA)
          .addColumn("id")
          .build();

  static final HadoopTables TABLES = new HadoopTables(new Configuration());

  static final DeltaFile DELTA_FILE_A = DeltaFiles.builder(PK_SPEC)
          .withPath("/path/delta/delta-1.avro")
          .withFileSizeInBytes(0)
          .withRowCount(2)
          .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  File tableDir = null;
  String tableLocation = null;
  File metadataDir = null;
  File versionHintFile = null;
  Table table = null;

  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    this.tableLocation = tableDir.toURI().toString();
    this.metadataDir = new File(tableDir, "metadata");
    this.versionHintFile = new File(metadataDir, "version-hint.text");
    this.table = TABLES.create(SCHEMA, SPEC, PK_SPEC, tableLocation);
  }

  List<File> listManifestFiles() {
    return Lists.newArrayList(metadataDir.listFiles((dir, name) ->
            !name.startsWith("snap") && com.google.common.io.Files.getFileExtension(name).equalsIgnoreCase("avro")));
  }

  List<File> listMetadataJsonFiles() {
    return Lists.newArrayList(metadataDir.listFiles((dir, name) ->
            name.endsWith(".metadata.json") || name.endsWith(".metadata.json.gz")));
  }

  File version(int versionNumber) {
    return new File(metadataDir, "v" + versionNumber + getFileExtension(TableMetadataParser.Codec.NONE));
  }

  TableMetadata readMetadataVersion(int version) {
    return TableMetadataParser.read(new TestTables.TestTableOperations("table", tableDir).io(),
            localInput(version(version)));
  }

  int readVersionHint() throws IOException {
    return Integer.parseInt(com.google.common.io.Files.readFirstLine(versionHintFile, StandardCharsets.UTF_8));
  }

  void replaceVersionHint(int version) throws IOException {
    // remove the checksum that will no longer match
    new File(metadataDir, ".version-hint.text.crc").delete();
    com.google.common.io.Files.write(String.valueOf(version), versionHintFile, StandardCharsets.UTF_8);
  }

  /*
   * Rewrites all current metadata files to gz compressed with extension .metadata.json.gz.
   * Assumes source files are not compressed.
   */
  void rewriteMetadataAsGzipWithOldExtension() throws IOException {
    List<File> metadataJsonFiles = listMetadataJsonFiles();
    for (File file : metadataJsonFiles) {
      try (FileInputStream input = new FileInputStream(file)) {
        try (GZIPOutputStream gzOutput = new GZIPOutputStream(new FileOutputStream(file.getAbsolutePath() + ".gz"))) {
          int bb;
          while ((bb = input.read()) != -1) {
            gzOutput.write(bb);
          }
        }
      }
      // delete original metadata file
      file.delete();
    }
  }
}
