package org.apache.iceberg.spark.data;

import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;
import static org.apache.iceberg.spark.data.TestHelpers.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
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

    final File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<InternalRow> writer = ORC.write(Files.localOutput(testFile))
        .createWriterFunc(SparkOrcWriter::new)
        .schema(schema)
        .build()) {
      writer.addAll(expected);
    }

    try (CloseableIterable<InternalRow> reader = ORC.read(Files.localInput(testFile))
        .schema(schema)
        .createReaderFunc(SparkOrcReader::new)
        .build()) {
      final Iterator<InternalRow> actualRows = reader.iterator();
      final Iterator<InternalRow> expectedRows = expected.iterator();
      while (expectedRows.hasNext()) {
        Assert.assertTrue("Should have expected number of rows", actualRows.hasNext());
        assertEquals(schema, expectedRows.next(), actualRows.next());
      }
      Assert.assertFalse("Should not have extra rows", actualRows.hasNext());
    }
  }
}
