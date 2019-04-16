package org.apache.iceberg.orc;

import java.io.IOException;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

/**
 * Write data value of a schema.
 * @author Edgar Rodriguez-Diaz
 * @since
 */
public interface OrcValueWriter<T> {

  /**
   * Writes the data.
   * @param value the data value to write.
   * @param output the VectorizedRowBatch to which the output will be written.
   * @throws IOException if there's any IO error while writing the data value.
   */
  void write(T value, VectorizedRowBatch output) throws IOException;
}
