package com.netflix.iceberg;

import com.netflix.iceberg.encryption.EncryptionTypes;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestPersistEncryptionMetadata extends TableTestBase {

  @Test
  public void testTableMetadataIOWithEncryptionMetadata() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    Snapshot pending = table.newFastAppend()
        .appendFile(DataFiles.builder()
            .copy(FILE_A)
            .withFileEncryptionMetadata(ENCRYPTION)
            .build())
        .apply();

    validateSnapshot(base.currentSnapshot(), pending, FILE_A);

    try (ManifestReader reader = ManifestReader.read(
        Files.localInput(pending.manifests().iterator().next()));
         CloseableIterable<ManifestEntry> files = reader.entries()) {
      EncryptionTypes.FileEncryptionMetadata writtenEncryptionMetadata = files
          .iterator()
          .next()
          .file()
          .fileEncryptionMetadata();
      Assert.assertNotNull("Encryption metadata was not read.", writtenEncryptionMetadata);
      Assert.assertArrayEquals("Written encrypted key is not correct.",
          ByteBuffers.copy(ENCRYPTION.encryptedKey()),
          ByteBuffers.copy(writtenEncryptionMetadata.encryptedKey()));
      Assert.assertArrayEquals("Written encrypted key is not correct.",
          ByteBuffers.copy(ENCRYPTION.iv()),
          ByteBuffers.copy(writtenEncryptionMetadata.iv()));
      Assert.assertEquals(
          "Key algorithm was incorrect.",
          writtenEncryptionMetadata.keyAlgorithm(),
          ENCRYPTION.keyAlgorithm());
      Assert.assertEquals(
          "Key name was incorrect.",
          writtenEncryptionMetadata.keyDescription().keyName(),
          ENCRYPTION.keyDescription().keyName());
      Assert.assertEquals(
          "Key version was incorrect.",
          writtenEncryptionMetadata.keyDescription().keyVersion(),
          ENCRYPTION.keyDescription().keyVersion());
    }
  }
}
