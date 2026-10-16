package org.apache.iceberg.mumbling;

import org.apache.iceberg.ManifestBitmap;

/**
 * Mutation methods for ManifestBitmap.
 */
public interface MutableBitmap extends ManifestBitmap {
  boolean set(int position);
}
