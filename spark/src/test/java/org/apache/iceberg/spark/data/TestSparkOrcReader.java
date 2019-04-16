package org.apache.iceberg.spark.data;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;

/**
 * @author Edgar Rodriguez-Diaz
 * @since
 */
public class TestSparkOrcReader extends AvroDataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    final Iterable<InternalRow> expected = RandomData
        .generateSpark(schema, 100, 0L);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<InternalRow> writer = ORC.write(Files.localOutput(testFile))
        .createWriterFunc(SparkOrcWriter::new)
        .schema(schema)
        .build()) {
      writer.addAll(expected);
    }
  }
}
