package org.apache.iceberg.orc;

/**
 * @author Edgar Rodriguez-Diaz
 * @since
 */
public interface OrcValueReader<T> {

  /**
   * Reads
   * @param reuse
   * @param row
   * @return
   */
  T read(Object reuse, int row);

}
