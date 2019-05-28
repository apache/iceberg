package org.apache.iceberg.spark.data;

import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.IOException;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestOrcWrite {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  @Test
  public void splitOffsets() throws IOException {
    Iterable<InternalRow> rows = RandomData.generateSpark(SCHEMA, 1, 0L);
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    FileAppender<InternalRow> writer = ORC.write(Files.localOutput(testFile))
        .createWriterFunc(SparkOrcWriter::new)
        .schema(SCHEMA)
        .build();

    writer.addAll(rows);
    writer.close();
    Assert.assertNotNull("Split offsets not present", writer.splitOffsets());
  }
}
