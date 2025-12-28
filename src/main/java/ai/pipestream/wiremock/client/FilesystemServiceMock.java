package ai.pipestream.wiremock.client;

import ai.pipestream.repository.filesystem.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.time.Instant;
import java.util.*;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring FilesystemService mocks in WireMock.
 * <p>
 * This mock supports testing of blob hydration scenarios including:
 * <ul>
 *   <li><b>GetFilesystemNode</b>: Returns nodes with optional blob data</li>
 *   <li><b>GetNodeByPath</b>: Returns nodes by path with blob data</li>
 *   <li><b>Blob hydration</b>: Simulates returning inline blob bytes for parser modules</li>
 * </ul>
 * <p>
 * This class implements {@link ServiceMockInitializer} and will be automatically
 * discovered and initialized at server startup by the {@link ServiceMockRegistry}.
 */
public class FilesystemServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(FilesystemServiceMock.class);

    private static final String SERVICE_NAME = FilesystemServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService filesystemService;

    // Track registered blobs for dynamic responses
    private final Map<String, BlobData> registeredBlobs = new HashMap<>();

    /**
     * Create a helper for the given WireMock client.
     *
     * @param wireMock The WireMock client instance (connected to WireMock server)
     */
    public FilesystemServiceMock(WireMock wireMock) {
        this.filesystemService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    /**
     * Default constructor for ServiceLoader discovery.
     */
    public FilesystemServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.filesystemService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default FilesystemService stubs");

        // Register default test blobs of various sizes
        registerTestBlob("test-blob-small", "text/plain", 1024);
        registerTestBlob("test-blob-medium", "application/pdf", 1024 * 1024); // 1MB
        registerTestBlob("test-blob-large", "application/pdf", 10 * 1024 * 1024); // 10MB

        // Register test nodes with blobs
        registerNodeWithBlob("test-node-1", "test-drive", "/test/document.pdf",
                "test-blob-small", "text/plain");
        registerNodeWithBlob("test-node-2", "test-drive", "/test/large-document.pdf",
                "test-blob-medium", "application/pdf");

        // Set up default stubs
        setupDefaultStubs();

        LOG.info("Added {} blob stubs for FilesystemService", registeredBlobs.size());
    }

    // ============================================
    // Blob Registration Methods
    // ============================================

    /**
     * Register a test blob with generated content.
     *
     * @param blobId   Unique blob identifier
     * @param mimeType MIME type of the blob
     * @param size     Size in bytes
     */
    public void registerTestBlob(String blobId, String mimeType, int size) {
        byte[] content = BlobGenerator.generateTestBlob(size, mimeType);
        registerBlob(blobId, mimeType, content);
    }

    /**
     * Register a blob with specific content.
     *
     * @param blobId   Unique blob identifier
     * @param mimeType MIME type of the blob
     * @param content  Blob content bytes
     */
    public void registerBlob(String blobId, String mimeType, byte[] content) {
        BlobData blobData = new BlobData(blobId, mimeType, content);
        registeredBlobs.put(blobId, blobData);
        LOG.debug("Registered blob: {} ({} bytes, {})", blobId, content.length, mimeType);
    }

    /**
     * Register a node with an associated blob.
     *
     * @param nodeId   Node identifier
     * @param driveId  Drive identifier
     * @param path     Node path
     * @param blobId   Associated blob identifier
     * @param mimeType MIME type
     */
    public void registerNodeWithBlob(String nodeId, String driveId, String path,
                                      String blobId, String mimeType) {
        if (!registeredBlobs.containsKey(blobId)) {
            LOG.warn("Blob {} not found, creating empty blob", blobId);
            registerBlob(blobId, mimeType, new byte[0]);
        }

        BlobData blobData = registeredBlobs.get(blobId);
        mockGetFilesystemNode(nodeId, driveId, path, blobData);
    }

    // ============================================
    // GetFilesystemNode Mocks
    // ============================================

    /**
     * Set up default stubs for filesystem operations.
     */
    private void setupDefaultStubs() {
        // Default GetFilesystemNode response - returns a simple node
        Node defaultNode = Node.newBuilder()
                .setDocumentId("default-node")
                .setName("default-document.txt")
                .setPath("/default-document.txt")
                .setContentType("text/plain")
                .setType(Node.NodeType.NODE_TYPE_FILE)
                .setSizeBytes(1024)
                .build();

        GetFilesystemNodeResponse defaultResponse = GetFilesystemNodeResponse.newBuilder()
                .setNode(defaultNode)
                .build();

        filesystemService.stubFor(
                method("GetFilesystemNode")
                        .willReturn(message(defaultResponse))
        );
    }

    /**
     * Mock GetFilesystemNode to return a node with inline blob data.
     *
     * @param nodeId   Node identifier
     * @param driveId  Drive identifier
     * @param path     Node path
     * @param blobData Blob data to include
     */
    public void mockGetFilesystemNode(String nodeId, String driveId, String path, BlobData blobData) {
        GetFilesystemNodeRequest request = GetFilesystemNodeRequest.newBuilder()
                .setDocumentId(nodeId)
                .build();

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        String filename = path.substring(path.lastIndexOf('/') + 1);

        Node node = Node.newBuilder()
                .setDocumentId(nodeId)
                .setName(filename)
                .setPath(path)
                .setContentType(blobData.mimeType)
                .setType(Node.NodeType.NODE_TYPE_FILE)
                .setSizeBytes(blobData.content.length)
                .setCreatedAt(timestamp)
                .build();

        GetFilesystemNodeResponse response = GetFilesystemNodeResponse.newBuilder()
                .setNode(node)
                .build();

        filesystemService.stubFor(
                method("GetFilesystemNode")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetFilesystemNode to return NOT_FOUND error.
     *
     * @param nodeId  Node identifier
     * @param driveId Drive identifier
     */
    public void mockGetFilesystemNodeNotFound(String nodeId, String driveId) {
        GetFilesystemNodeRequest request = GetFilesystemNodeRequest.newBuilder()
                .setDocumentId(nodeId)
                .build();

        filesystemService.stubFor(
                method("GetFilesystemNode")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "Node not found: " + nodeId)
        );
    }

    /**
     * Mock GetFilesystemNode to return UNAVAILABLE error (simulates storage failure).
     *
     * @param nodeId  Node identifier
     * @param driveId Drive identifier
     */
    public void mockGetFilesystemNodeUnavailable(String nodeId, String driveId) {
        GetFilesystemNodeRequest request = GetFilesystemNodeRequest.newBuilder()
                .setDocumentId(nodeId)
                .build();

        filesystemService.stubFor(
                method("GetFilesystemNode")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.UNAVAILABLE, "Storage service unavailable")
        );
    }

    // ============================================
    // GetNodeByPath Mocks
    // ============================================

    /**
     * Mock GetNodeByPath to return a node with blob data.
     *
     * @param driveId  Drive identifier
     * @param path     Node path
     * @param blobData Blob data
     */
    public void mockGetNodeByPath(String driveId, String path, BlobData blobData) {
        GetNodeByPathRequest request = GetNodeByPathRequest.newBuilder()
                .setDrive(driveId)
                .setPath(path)
                .build();

        String filename = path.substring(path.lastIndexOf('/') + 1);
        String nodeId = UUID.randomUUID().toString();

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        Node node = Node.newBuilder()
                .setDocumentId(nodeId)
                .setName(filename)
                .setPath(path)
                .setContentType(blobData.mimeType)
                .setType(Node.NodeType.NODE_TYPE_FILE)
                .setSizeBytes(blobData.content.length)
                .setCreatedAt(timestamp)
                .build();

        GetNodeByPathResponse response = GetNodeByPathResponse.newBuilder()
                .setNode(node)
                .build();

        filesystemService.stubFor(
                method("GetNodeByPath")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetNodeByPath to return NOT_FOUND error.
     *
     * @param driveId Drive identifier
     * @param path    Node path
     */
    public void mockGetNodeByPathNotFound(String driveId, String path) {
        GetNodeByPathRequest request = GetNodeByPathRequest.newBuilder()
                .setDrive(driveId)
                .setPath(path)
                .build();

        filesystemService.stubFor(
                method("GetNodeByPath")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "Path not found: " + path)
        );
    }

    // ============================================
    // Drive Mocks
    // ============================================

    /**
     * Mock GetDrive to return drive information.
     *
     * @param driveId   Drive identifier
     * @param driveName Drive name
     * @param accountId Account identifier
     */
    public void mockGetDrive(String driveId, String driveName, String accountId) {
        GetDriveRequest request = GetDriveRequest.newBuilder()
                .setName(driveId)
                .build();

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        Drive drive = Drive.newBuilder()
                .setName(driveName)
                .setAccountId(accountId)
                .setCreatedAt(timestamp)
                .build();

        GetDriveResponse response = GetDriveResponse.newBuilder()
                .setDrive(drive)
                .build();

        filesystemService.stubFor(
                method("GetDrive")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetDrive to return NOT_FOUND error.
     *
     * @param driveId Drive identifier
     */
    public void mockGetDriveNotFound(String driveId) {
        GetDriveRequest request = GetDriveRequest.newBuilder()
                .setName(driveId)
                .build();

        filesystemService.stubFor(
                method("GetDrive")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "Drive not found: " + driveId)
        );
    }

    // ============================================
    // Utility Methods
    // ============================================

    /**
     * Get a registered blob by ID.
     *
     * @param blobId Blob identifier
     * @return Optional containing the blob data, or empty if not found
     */
    public Optional<BlobData> getBlob(String blobId) {
        return Optional.ofNullable(registeredBlobs.get(blobId));
    }

    /**
     * Get all registered blob IDs.
     *
     * @return Set of blob IDs
     */
    public Set<String> getRegisteredBlobIds() {
        return Collections.unmodifiableSet(registeredBlobs.keySet());
    }

    /**
     * Reset all WireMock stubs for the filesystem service.
     */
    public void reset() {
        filesystemService.resetAll();
        registeredBlobs.clear();
    }

    // ============================================
    // Inner Classes
    // ============================================

    /**
     * Data class for blob information.
     */
    public static class BlobData {
        public final String blobId;
        public final String mimeType;
        public final byte[] content;

        public BlobData(String blobId, String mimeType, byte[] content) {
            this.blobId = blobId;
            this.mimeType = mimeType;
            this.content = content;
        }

        public int size() {
            return content.length;
        }

        public ByteString toByteString() {
            return ByteString.copyFrom(content);
        }
    }
}
