package ai.pipestream.wiremock.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for BlobGenerator utility.
 */
class BlobGeneratorTest {

    @Test
    @DisplayName("Should generate text blob of specified size")
    void testGenerateTextBlob() {
        int size = 1024;
        byte[] blob = BlobGenerator.generateTextBlob(size, "Test Document");

        assertNotNull(blob);
        assertEquals(size, blob.length);

        // Should start with title
        String content = new String(blob, StandardCharsets.UTF_8);
        assertTrue(content.startsWith("Test Document"));
    }

    @Test
    @DisplayName("Should generate PDF-like blob with correct magic bytes")
    void testGeneratePdfBlob() {
        int size = 2048;
        byte[] blob = BlobGenerator.generatePdfBlob(size, 42);

        assertNotNull(blob);
        assertEquals(size, blob.length);

        // Check PDF magic bytes
        String header = new String(blob, 0, 9, StandardCharsets.US_ASCII);
        assertEquals("%PDF-1.4\n", header);

        // Check PDF footer
        String footer = new String(blob, size - 7, 7, StandardCharsets.US_ASCII);
        assertEquals("\n%%EOF\n", footer);
    }

    @Test
    @DisplayName("Should generate PNG-like blob with correct magic bytes")
    void testGeneratePngBlob() {
        int size = 1024;
        byte[] blob = BlobGenerator.generatePngBlob(size, 42);

        assertNotNull(blob);
        assertEquals(size, blob.length);

        // Check PNG magic bytes
        assertEquals((byte) 0x89, blob[0]);
        assertEquals((byte) 0x50, blob[1]); // P
        assertEquals((byte) 0x4E, blob[2]); // N
        assertEquals((byte) 0x47, blob[3]); // G
    }

    @Test
    @DisplayName("Should generate JPEG-like blob with correct magic bytes")
    void testGenerateJpegBlob() {
        int size = 1024;
        byte[] blob = BlobGenerator.generateJpegBlob(size, 42);

        assertNotNull(blob);
        assertEquals(size, blob.length);

        // Check JPEG magic bytes
        assertEquals((byte) 0xFF, blob[0]);
        assertEquals((byte) 0xD8, blob[1]);
        assertEquals((byte) 0xFF, blob[2]);

        // Check JPEG end marker
        assertEquals((byte) 0xFF, blob[size - 2]);
        assertEquals((byte) 0xD9, blob[size - 1]);
    }

    @Test
    @DisplayName("Should generate binary blob of specified size")
    void testGenerateBinaryBlob() {
        int size = 5000;
        byte[] blob = BlobGenerator.generateBinaryBlob(size, 12345);

        assertNotNull(blob);
        assertEquals(size, blob.length);
    }

    @Test
    @DisplayName("Should generate deterministic blob with same seed")
    void testDeterministicGeneration() {
        long seed = 42;
        int size = 1024;

        byte[] blob1 = BlobGenerator.generateBinaryBlob(size, seed);
        byte[] blob2 = BlobGenerator.generateBinaryBlob(size, seed);

        assertArrayEquals(blob1, blob2);
    }

    @Test
    @DisplayName("Should generate different blobs with different seeds")
    void testDifferentSeeds() {
        int size = 1024;

        byte[] blob1 = BlobGenerator.generateBinaryBlob(size, 1);
        byte[] blob2 = BlobGenerator.generateBinaryBlob(size, 2);

        assertFalse(java.util.Arrays.equals(blob1, blob2));
    }

    @Test
    @DisplayName("Should generate HTML blob with proper structure")
    void testGenerateHtmlBlob() {
        int size = 500;
        byte[] blob = BlobGenerator.generateHtmlBlob(size);

        String content = new String(blob, StandardCharsets.UTF_8);
        assertTrue(content.startsWith("<!DOCTYPE html>"));
        assertTrue(content.contains("<html>"));
        assertTrue(content.contains("</html>"));
    }

    @Test
    @DisplayName("Should generate JSON blob with valid structure")
    void testGenerateJsonBlob() {
        int size = 500;
        byte[] blob = BlobGenerator.generateJsonBlob(size);

        String content = new String(blob, StandardCharsets.UTF_8);
        assertTrue(content.startsWith("{"));
        assertTrue(content.endsWith("}"));
        assertTrue(content.contains("\"title\""));
    }

    @Test
    @DisplayName("Should generate XML blob with valid structure")
    void testGenerateXmlBlob() {
        int size = 500;
        byte[] blob = BlobGenerator.generateXmlBlob(size);

        String content = new String(blob, StandardCharsets.UTF_8);
        assertTrue(content.startsWith("<?xml version"));
        assertTrue(content.contains("<document>"));
        assertTrue(content.endsWith("</document>"));
    }

    @Test
    @DisplayName("Should generate Markdown blob with headers")
    void testGenerateMarkdownBlob() {
        int size = 500;
        byte[] blob = BlobGenerator.generateMarkdownBlob(size);

        String content = new String(blob, StandardCharsets.UTF_8);
        assertTrue(content.startsWith("# Test Document"));
        assertTrue(content.contains("## "));
    }

    @Test
    @DisplayName("Should generate blob based on MIME type")
    void testGenerateTestBlobByMimeType() {
        // Text
        byte[] textBlob = BlobGenerator.generateTestBlob(100, "text/plain");
        assertTrue(new String(textBlob, StandardCharsets.UTF_8).length() > 0);

        // PDF
        byte[] pdfBlob = BlobGenerator.generateTestBlob(1000, "application/pdf");
        assertEquals('%', (char) pdfBlob[0]);

        // JSON
        byte[] jsonBlob = BlobGenerator.generateTestBlob(500, "application/json");
        assertEquals('{', (char) jsonBlob[0]);

        // Unknown type defaults to binary
        byte[] binaryBlob = BlobGenerator.generateTestBlob(100, "application/unknown");
        assertEquals(100, binaryBlob.length);
    }

    @Test
    @DisplayName("Should calculate correct SHA-256 checksum")
    void testCalculateChecksum() {
        byte[] content = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        String checksum = BlobGenerator.calculateChecksum(content);

        assertNotNull(checksum);
        assertEquals(64, checksum.length()); // SHA-256 is 32 bytes = 64 hex chars

        // Same content should produce same checksum
        String checksum2 = BlobGenerator.calculateChecksum(content);
        assertEquals(checksum, checksum2);
    }

    @Test
    @DisplayName("Should convert bytes to ByteString")
    void testToByteString() {
        byte[] content = new byte[]{1, 2, 3, 4, 5};
        var byteString = BlobGenerator.toByteString(content);

        assertNotNull(byteString);
        assertEquals(5, byteString.size());
        assertArrayEquals(content, byteString.toByteArray());
    }

    @Test
    @DisplayName("Should handle large blob generation")
    void testLargeBlobGeneration() {
        int size = 10 * 1024 * 1024; // 10MB
        byte[] blob = BlobGenerator.generateBinaryBlob(size, 42);

        assertNotNull(blob);
        assertEquals(size, blob.length);
    }

    @Test
    @DisplayName("Should generate compressed gzip blob")
    void testGenerateGzipBlob() {
        int size = 10000;
        byte[] blob = BlobGenerator.generateGzipBlob(size, 42);

        assertNotNull(blob);
        // Compressed blob should be smaller than original
        assertTrue(blob.length < size);

        // Check gzip magic bytes
        assertEquals((byte) 0x1f, blob[0]);
        assertEquals((byte) 0x8b, blob[1]);
    }
}
