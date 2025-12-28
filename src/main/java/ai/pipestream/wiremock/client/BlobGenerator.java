package ai.pipestream.wiremock.client;

import com.google.protobuf.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for generating test blob data of various types and sizes.
 * <p>
 * Supports generating blobs that simulate real file content for testing:
 * <ul>
 *   <li><b>Text files</b>: Plain text, markdown, JSON, XML</li>
 *   <li><b>Binary files</b>: PDF-like, image-like, compressed</li>
 *   <li><b>Configurable sizes</b>: From 1KB to 100MB+</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * // Generate a 1MB PDF-like blob
 * byte[] pdfBlob = BlobGenerator.generateTestBlob(1024 * 1024, "application/pdf");
 *
 * // Generate text content
 * byte[] textBlob = BlobGenerator.generateTextBlob(5000, "Sample document");
 *
 * // Generate with specific seed for reproducibility
 * byte[] deterministicBlob = BlobGenerator.generateBinaryBlob(1024, 42);
 * }</pre>
 */
public final class BlobGenerator {

    private static final Random DEFAULT_RANDOM = new Random(12345); // Deterministic for testing

    // PDF magic bytes and basic structure
    private static final byte[] PDF_HEADER = "%PDF-1.4\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PDF_FOOTER = "\n%%EOF\n".getBytes(StandardCharsets.US_ASCII);

    // PNG magic bytes
    private static final byte[] PNG_HEADER = new byte[]{
            (byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A
    };

    // JPEG magic bytes
    private static final byte[] JPEG_HEADER = new byte[]{
            (byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0
    };

    private BlobGenerator() {
        // Utility class
    }

    /**
     * Generate a test blob based on MIME type and size.
     *
     * @param size     Size in bytes
     * @param mimeType MIME type to simulate
     * @return Generated blob content
     */
    public static byte[] generateTestBlob(int size, String mimeType) {
        return generateTestBlob(size, mimeType, DEFAULT_RANDOM.nextLong());
    }

    /**
     * Generate a test blob with a specific seed for reproducibility.
     *
     * @param size     Size in bytes
     * @param mimeType MIME type to simulate
     * @param seed     Random seed for reproducibility
     * @return Generated blob content
     */
    public static byte[] generateTestBlob(int size, String mimeType, long seed) {
        if (mimeType == null) {
            return generateBinaryBlob(size, seed);
        }

        return switch (mimeType.toLowerCase()) {
            case "text/plain" -> generateTextBlob(size, "Test document content");
            case "text/html" -> generateHtmlBlob(size);
            case "text/markdown" -> generateMarkdownBlob(size);
            case "application/json" -> generateJsonBlob(size);
            case "application/xml", "text/xml" -> generateXmlBlob(size);
            case "application/pdf" -> generatePdfBlob(size, seed);
            case "image/png" -> generatePngBlob(size, seed);
            case "image/jpeg", "image/jpg" -> generateJpegBlob(size, seed);
            case "application/gzip", "application/x-gzip" -> generateGzipBlob(size, seed);
            default -> generateBinaryBlob(size, seed);
        };
    }

    /**
     * Generate a plain text blob with lorem ipsum-like content.
     *
     * @param size  Approximate size in bytes
     * @param title Optional title for the document
     * @return Text content as bytes
     */
    public static byte[] generateTextBlob(int size, String title) {
        StringBuilder sb = new StringBuilder();
        if (title != null && !title.isEmpty()) {
            sb.append(title).append("\n\n");
        }

        String[] words = {
                "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
                "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
                "data", "processing", "pipeline", "document", "content", "analysis", "extraction"
        };

        Random random = new Random(42);
        while (sb.length() < size) {
            // Generate a sentence
            int sentenceLength = 8 + random.nextInt(12);
            for (int i = 0; i < sentenceLength && sb.length() < size; i++) {
                if (i == 0) {
                    String word = words[random.nextInt(words.length)];
                    sb.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1));
                } else {
                    sb.append(" ").append(words[random.nextInt(words.length)]);
                }
            }
            sb.append(". ");

            // Occasional paragraph break
            if (random.nextInt(5) == 0) {
                sb.append("\n\n");
            }
        }

        return sb.substring(0, Math.min(sb.length(), size)).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generate an HTML document blob.
     *
     * @param size Approximate size in bytes
     * @return HTML content as bytes
     */
    public static byte[] generateHtmlBlob(int size) {
        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html>\n<html>\n<head>\n<title>Test Document</title>\n</head>\n<body>\n");
        sb.append("<h1>Test Document</h1>\n");

        int contentSize = size - 100; // Reserve space for closing tags
        while (sb.length() < contentSize) {
            sb.append("<p>");
            sb.append(new String(generateTextBlob(Math.min(200, contentSize - sb.length()), null)));
            sb.append("</p>\n");
        }

        sb.append("</body>\n</html>");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generate a Markdown document blob.
     *
     * @param size Approximate size in bytes
     * @return Markdown content as bytes
     */
    public static byte[] generateMarkdownBlob(int size) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Test Document\n\n");
        sb.append("## Introduction\n\n");

        int contentSize = size - 50;
        int section = 1;
        while (sb.length() < contentSize) {
            if (sb.length() % 500 == 0) {
                sb.append("\n## Section ").append(section++).append("\n\n");
            }
            sb.append(new String(generateTextBlob(Math.min(300, contentSize - sb.length()), null)));
            sb.append("\n\n");
        }

        return sb.substring(0, Math.min(sb.length(), size)).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generate a JSON document blob.
     *
     * @param size Approximate size in bytes
     * @return JSON content as bytes
     */
    public static byte[] generateJsonBlob(int size) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"title\": \"Test Document\",\n");
        sb.append("  \"version\": \"1.0\",\n");
        sb.append("  \"content\": [\n");

        int contentSize = size - 50;
        int item = 0;
        while (sb.length() < contentSize) {
            if (item > 0) sb.append(",\n");
            sb.append("    {\n");
            sb.append("      \"id\": ").append(item).append(",\n");
            sb.append("      \"text\": \"Item ").append(item).append(" content data\"\n");
            sb.append("    }");
            item++;
        }

        sb.append("\n  ]\n}");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generate an XML document blob.
     *
     * @param size Approximate size in bytes
     * @return XML content as bytes
     */
    public static byte[] generateXmlBlob(int size) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sb.append("<document>\n");
        sb.append("  <title>Test Document</title>\n");

        int contentSize = size - 50;
        int item = 0;
        while (sb.length() < contentSize) {
            sb.append("  <item id=\"").append(item).append("\">\n");
            sb.append("    <content>Item ").append(item).append(" content data</content>\n");
            sb.append("  </item>\n");
            item++;
        }

        sb.append("</document>");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generate a PDF-like binary blob.
     * This creates content with PDF magic bytes to simulate a real PDF.
     *
     * @param size Size in bytes
     * @param seed Random seed
     * @return PDF-like content
     */
    public static byte[] generatePdfBlob(int size, long seed) {
        byte[] content = new byte[size];
        Random random = new Random(seed);

        // Copy PDF header
        System.arraycopy(PDF_HEADER, 0, content, 0, Math.min(PDF_HEADER.length, size));

        // Fill middle with pseudo-random content simulating PDF objects
        int startPos = PDF_HEADER.length;
        int endPos = size - PDF_FOOTER.length;

        for (int i = startPos; i < endPos; i++) {
            // Mix of ASCII and binary to simulate PDF content
            if (random.nextInt(10) < 7) {
                content[i] = (byte) (32 + random.nextInt(95)); // Printable ASCII
            } else {
                content[i] = (byte) random.nextInt(256);
            }
        }

        // Copy PDF footer if there's room
        if (size > PDF_HEADER.length + PDF_FOOTER.length) {
            System.arraycopy(PDF_FOOTER, 0, content, size - PDF_FOOTER.length, PDF_FOOTER.length);
        }

        return content;
    }

    /**
     * Generate a PNG-like binary blob.
     *
     * @param size Size in bytes
     * @param seed Random seed
     * @return PNG-like content
     */
    public static byte[] generatePngBlob(int size, long seed) {
        byte[] content = new byte[size];
        Random random = new Random(seed);

        // Copy PNG header
        System.arraycopy(PNG_HEADER, 0, content, 0, Math.min(PNG_HEADER.length, size));

        // Fill with random binary data
        for (int i = PNG_HEADER.length; i < size; i++) {
            content[i] = (byte) random.nextInt(256);
        }

        return content;
    }

    /**
     * Generate a JPEG-like binary blob.
     *
     * @param size Size in bytes
     * @param seed Random seed
     * @return JPEG-like content
     */
    public static byte[] generateJpegBlob(int size, long seed) {
        byte[] content = new byte[size];
        Random random = new Random(seed);

        // Copy JPEG header
        System.arraycopy(JPEG_HEADER, 0, content, 0, Math.min(JPEG_HEADER.length, size));

        // Fill with random binary data
        for (int i = JPEG_HEADER.length; i < size; i++) {
            content[i] = (byte) random.nextInt(256);
        }

        // JPEG end marker
        if (size >= 2) {
            content[size - 2] = (byte) 0xFF;
            content[size - 1] = (byte) 0xD9;
        }

        return content;
    }

    /**
     * Generate a gzip-compressed blob.
     *
     * @param size Size in bytes (approximate, compression affects actual size)
     * @param seed Random seed
     * @return Gzip compressed content
     */
    public static byte[] generateGzipBlob(int size, long seed) {
        try {
            byte[] originalContent = generateTextBlob(size, "Compressed content");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
                gzos.write(originalContent);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            // Fallback to binary if compression fails
            return generateBinaryBlob(size, seed);
        }
    }

    /**
     * Generate a pure binary blob with random content.
     *
     * @param size Size in bytes
     * @param seed Random seed for reproducibility
     * @return Binary content
     */
    public static byte[] generateBinaryBlob(int size, long seed) {
        byte[] content = new byte[size];
        new Random(seed).nextBytes(content);
        return content;
    }

    /**
     * Generate binary blob with default seed.
     *
     * @param size Size in bytes
     * @return Binary content
     */
    public static byte[] generateBinaryBlob(int size) {
        return generateBinaryBlob(size, DEFAULT_RANDOM.nextLong());
    }

    /**
     * Calculate SHA-256 checksum of content.
     *
     * @param content Content bytes
     * @return Hex-encoded checksum
     */
    public static String calculateChecksum(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content);
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /**
     * Convert bytes to ByteString for protobuf usage.
     *
     * @param content Content bytes
     * @return ByteString
     */
    public static ByteString toByteString(byte[] content) {
        return ByteString.copyFrom(content);
    }
}
