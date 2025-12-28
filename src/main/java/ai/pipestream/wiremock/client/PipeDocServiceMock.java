package ai.pipestream.wiremock.client;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.FileStorageReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring PipeDocService mocks in WireMock.
 * <p>
 * This mock supports testing of document hydration scenarios for the Kafka Sidecar and Engine including:
 * <ul>
 *   <li><b>GetPipeDocByReference</b>: Return PipeDoc for a given DocumentReference</li>
 *   <li><b>GetBlob</b>: Return blob binary data for a given FileStorageReference (Level 2 hydration)</li>
 *   <li><b>Document/blob not found</b>: Return NOT_FOUND status for missing documents/blobs</li>
 *   <li><b>Transient failures</b>: Return UNAVAILABLE for retry testing</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * PipeDocServiceMock mock = new PipeDocServiceMock(wireMock);
 *
 * // Register a document that can be retrieved
 * mock.registerPipeDoc("doc-123", "account-456", samplePipeDoc);
 *
 * // Register a blob for Level 2 hydration
 * FileStorageReference blobRef = FileStorageReference.newBuilder()
 *     .setDriveName("default")
 *     .setObjectKey("doc-123/intake/blob-uuid.bin")
 *     .build();
 * mock.registerBlob(blobRef, ByteString.copyFromUtf8("Blob content"));
 *
 * // Mock a not found scenario
 * mock.mockGetByReferenceNotFound("missing-doc", "account-456");
 * mock.mockGetBlobNotFound(blobRef);
 *
 * // Mock an unavailable scenario for retry testing
 * mock.mockGetByReferenceUnavailable();
 * mock.mockGetBlobUnavailable();
 * }</pre>
 * <p>
 * This class implements {@link ServiceMockInitializer} and will be automatically
 * discovered and initialized at server startup by the {@link ServiceMockRegistry}.
 */
public class PipeDocServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(PipeDocServiceMock.class);

    private static final String SERVICE_NAME = PipeDocServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService pipeDocService;

    // Track registered documents for dynamic responses
    private final Map<String, RegisteredDoc> registeredDocs = new HashMap<>();
    
    // Track registered blobs for dynamic responses (keyed by storage reference)
    private final Map<String, RegisteredBlob> registeredBlobs = new HashMap<>();

    /**
     * Create a helper for the given WireMock client.
     *
     * @param wireMock The WireMock client instance (connected to WireMock server)
     */
    public PipeDocServiceMock(WireMock wireMock) {
        this.pipeDocService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    /**
     * Default constructor for ServiceLoader discovery.
     */
    public PipeDocServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.pipeDocService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default PipeDocService stubs");

        // Register some default test documents
        PipeDoc testDoc1 = PipeDoc.newBuilder()
                .setDocId("test-doc-1")
                .build();
        registerPipeDoc("test-doc-1", "test-account", testDoc1, "node-1", "test-drive");

        PipeDoc testDoc2 = PipeDoc.newBuilder()
                .setDocId("test-doc-2")
                .build();
        registerPipeDoc("test-doc-2", "test-account", testDoc2, "node-2", "test-drive");

        // Set up default stubs for unknown documents and blobs
        setupDefaultStubs();
        
        // Register some default test blobs
        FileStorageReference testBlobRef1 = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test-blob-1.bin")
                .build();
        registerBlob(testBlobRef1, ByteString.copyFromUtf8("Test blob content 1"));
        
        FileStorageReference testBlobRef2 = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test-blob-2.bin")
                .build();
        registerBlob(testBlobRef2, ByteString.copyFromUtf8("Test blob content 2"));

        LOG.info("Added {} document stubs and {} blob stubs for PipeDocService", 
                registeredDocs.size(), registeredBlobs.size());
    }

    // ============================================
    // Document Registration Methods
    // ============================================

    /**
     * Register a PipeDoc that can be retrieved by reference.
     *
     * @param docId     Document ID
     * @param accountId Account ID
     * @param pipeDoc   The PipeDoc to return
     * @param nodeId    Node ID for the response
     * @param drive     Drive name for the response
     */
    public void registerPipeDoc(String docId, String accountId, PipeDoc pipeDoc, String nodeId, String drive) {
        RegisteredDoc regDoc = new RegisteredDoc(docId, accountId, pipeDoc, nodeId, drive);
        registeredDocs.put(makeKey(docId, accountId), regDoc);
        mockGetByReference(regDoc);
        LOG.debug("Registered PipeDoc: docId={}, accountId={}", docId, accountId);
    }

    /**
     * Register a PipeDoc with default node and drive values.
     *
     * @param docId     Document ID
     * @param accountId Account ID
     * @param pipeDoc   The PipeDoc to return
     */
    public void registerPipeDoc(String docId, String accountId, PipeDoc pipeDoc) {
        registerPipeDoc(docId, accountId, pipeDoc, "default-node", "default-drive");
    }

    // ============================================
    // GetPipeDocByReference Mocks
    // ============================================

    /**
     * Set up default stubs for unknown documents and blobs.
     */
    private void setupDefaultStubs() {
        // Default response for any unmatched GetPipeDocByReference request - returns empty response
        // Individual document registrations will have higher priority
        GetPipeDocByReferenceResponse defaultResponse = GetPipeDocByReferenceResponse.newBuilder()
                .build();

        pipeDocService.stubFor(
                method("GetPipeDocByReference")
                        .willReturn(message(defaultResponse))
        );
        
        // Default response for any unmatched GetBlob request - returns NOT_FOUND
        // Individual blob registrations will have higher priority
        pipeDocService.stubFor(
                method("GetBlob")
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "Blob not found")
        );
    }

    /**
     * Mock GetPipeDocByReference for a registered document.
     *
     * @param regDoc Registered document info
     */
    private void mockGetByReference(RegisteredDoc regDoc) {
        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId(regDoc.docId)
                .setAccountId(regDoc.accountId)
                .build();

        GetPipeDocByReferenceRequest request = GetPipeDocByReferenceRequest.newBuilder()
                .setDocumentRef(docRef)
                .build();

        GetPipeDocByReferenceResponse response = GetPipeDocByReferenceResponse.newBuilder()
                .setPipedoc(regDoc.pipeDoc)
                .setNodeId(regDoc.nodeId)
                .setDrive(regDoc.drive)
                .setSizeBytes(regDoc.pipeDoc.getSerializedSize())
                .setRetrievedAtEpochMs(System.currentTimeMillis())
                .build();

        pipeDocService.stubFor(
                method("GetPipeDocByReference")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetPipeDocByReference to return a specific PipeDoc for any request.
     * This is useful when you want to control exactly what document is returned.
     *
     * @param pipeDoc The PipeDoc to return
     * @param nodeId  Node ID
     * @param drive   Drive name
     */
    public void mockGetByReferenceReturns(PipeDoc pipeDoc, String nodeId, String drive) {
        GetPipeDocByReferenceResponse response = GetPipeDocByReferenceResponse.newBuilder()
                .setPipedoc(pipeDoc)
                .setNodeId(nodeId)
                .setDrive(drive)
                .setSizeBytes(pipeDoc.getSerializedSize())
                .setRetrievedAtEpochMs(System.currentTimeMillis())
                .build();

        pipeDocService.stubFor(
                method("GetPipeDocByReference")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetPipeDocByReference to return NOT_FOUND for a specific document reference.
     *
     * @param docId     Document ID
     * @param accountId Account ID
     */
    public void mockGetByReferenceNotFound(String docId, String accountId) {
        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId(docId)
                .setAccountId(accountId)
                .build();

        GetPipeDocByReferenceRequest request = GetPipeDocByReferenceRequest.newBuilder()
                .setDocumentRef(docRef)
                .build();

        pipeDocService.stubFor(
                method("GetPipeDocByReference")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND,
                                "Document not found: docId=" + docId + ", accountId=" + accountId)
        );
    }

    /**
     * Mock GetPipeDocByReference to return UNAVAILABLE for any request.
     * Used for testing retry behavior.
     */
    public void mockGetByReferenceUnavailable() {
        pipeDocService.stubFor(
                method("GetPipeDocByReference")
                        .willReturn(WireMockGrpc.Status.UNAVAILABLE,
                                "PipeDoc service temporarily unavailable")
        );
    }

    /**
     * Mock GetPipeDocByReference to return UNAVAILABLE for a specific document.
     *
     * @param docId     Document ID
     * @param accountId Account ID
     */
    public void mockGetByReferenceUnavailable(String docId, String accountId) {
        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId(docId)
                .setAccountId(accountId)
                .build();

        GetPipeDocByReferenceRequest request = GetPipeDocByReferenceRequest.newBuilder()
                .setDocumentRef(docRef)
                .build();

        pipeDocService.stubFor(
                method("GetPipeDocByReference")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.UNAVAILABLE,
                                "PipeDoc service temporarily unavailable")
        );
    }

    /**
     * Mock GetPipeDocByReference to return INTERNAL error.
     * Used for testing error handling.
     *
     * @param errorMessage Error message
     */
    public void mockGetByReferenceError(String errorMessage) {
        pipeDocService.stubFor(
                method("GetPipeDocByReference")
                        .willReturn(WireMockGrpc.Status.INTERNAL, errorMessage)
        );
    }

    // ============================================
    // GetBlob Mocks
    // ============================================

    /**
     * Register a blob that can be retrieved by storage reference.
     *
     * @param storageRef The file storage reference (drive name, object key, optional version ID)
     * @param blobData   The blob binary data
     */
    public void registerBlob(FileStorageReference storageRef, ByteString blobData) {
        RegisteredBlob regBlob = new RegisteredBlob(storageRef, blobData);
        String key = makeBlobKey(storageRef);
        registeredBlobs.put(key, regBlob);
        mockGetBlob(regBlob);
        LOG.debug("Registered blob: drive={}, objectKey={}", 
                storageRef.getDriveName(), storageRef.getObjectKey());
    }

    /**
     * Mock GetBlob for a registered blob.
     *
     * @param regBlob Registered blob info
     */
    private void mockGetBlob(RegisteredBlob regBlob) {
        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(regBlob.storageRef)
                .build();

        GetBlobResponse response = GetBlobResponse.newBuilder()
                .setData(regBlob.blobData)
                .build();

        pipeDocService.stubFor(
                method("GetBlob")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetBlob to return a specific blob for any request.
     * This is useful when you want to control exactly what blob is returned.
     *
     * @param blobData The blob binary data to return
     */
    public void mockGetBlobReturns(ByteString blobData) {
        GetBlobResponse response = GetBlobResponse.newBuilder()
                .setData(blobData)
                .build();

        pipeDocService.stubFor(
                method("GetBlob")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetBlob to return NOT_FOUND for a specific storage reference.
     *
     * @param storageRef The file storage reference
     */
    public void mockGetBlobNotFound(FileStorageReference storageRef) {
        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(storageRef)
                .build();

        pipeDocService.stubFor(
                method("GetBlob")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND,
                                "Blob not found: drive=" + storageRef.getDriveName() + 
                                ", objectKey=" + storageRef.getObjectKey())
        );
    }

    /**
     * Mock GetBlob to return UNAVAILABLE for any request.
     * Used for testing retry behavior.
     */
    public void mockGetBlobUnavailable() {
        pipeDocService.stubFor(
                method("GetBlob")
                        .willReturn(WireMockGrpc.Status.UNAVAILABLE,
                                "Blob service temporarily unavailable")
        );
    }

    /**
     * Mock GetBlob to return UNAVAILABLE for a specific storage reference.
     *
     * @param storageRef The file storage reference
     */
    public void mockGetBlobUnavailable(FileStorageReference storageRef) {
        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(storageRef)
                .build();

        pipeDocService.stubFor(
                method("GetBlob")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.UNAVAILABLE,
                                "Blob service temporarily unavailable")
        );
    }

    /**
     * Mock GetBlob to return INTERNAL error.
     * Used for testing error handling.
     *
     * @param errorMessage Error message
     */
    public void mockGetBlobError(String errorMessage) {
        pipeDocService.stubFor(
                method("GetBlob")
                        .willReturn(WireMockGrpc.Status.INTERNAL, errorMessage)
        );
    }

    // ============================================
    // SavePipeDoc Mocks
    // ============================================

    /**
     * Mock SavePipeDoc to return a successful response for any request.
     * The response includes the nodeId, drive, s3Key, and metadata.
     *
     * @param nodeId   The node ID to return in the response
     * @param drive    The drive name to return in the response
     * @param s3Key    The S3 object key to return in the response
     */
    public void mockSavePipeDoc(String nodeId, String drive, String s3Key) {
        SavePipeDocResponse response = SavePipeDocResponse.newBuilder()
                .setNodeId(nodeId)
                .setDrive(drive)
                .setS3Key(s3Key)
                .setSizeBytes(1024)
                .setChecksum("sha256:mock-checksum-" + UUID.randomUUID().toString().substring(0, 8))
                .build();

        pipeDocService.stubFor(
                method("SavePipeDoc")
                        .willReturn(message(response))
        );
        LOG.debug("Mocked SavePipeDoc to return nodeId={}, drive={}, s3Key={}", nodeId, drive, s3Key);
    }

    /**
     * Mock SavePipeDoc to return a successful response with just the nodeId.
     * Other fields will have default values.
     *
     * @param nodeId The node ID to return in the response
     */
    public void mockSavePipeDoc(String nodeId) {
        mockSavePipeDoc(nodeId, "default-drive", "pipedocs/" + nodeId + "/" + UUID.randomUUID().toString());
    }

    /**
     * Mock SavePipeDoc for a specific PipeDoc request.
     * This allows matching on the exact PipeDoc content being saved.
     *
     * @param expectedDoc The expected PipeDoc to match
     * @param nodeId      The node ID to return
     * @param drive       The drive name to return
     * @param s3Key       The S3 key to return
     */
    public void mockSavePipeDocWithRequest(PipeDoc expectedDoc, String nodeId, String drive, String s3Key) {
        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(expectedDoc)
                .setDrive(drive)
                .build();

        SavePipeDocResponse response = SavePipeDocResponse.newBuilder()
                .setNodeId(nodeId)
                .setDrive(drive)
                .setS3Key(s3Key)
                .setSizeBytes(expectedDoc.getSerializedSize())
                .setChecksum("sha256:mock-checksum-" + UUID.randomUUID().toString().substring(0, 8))
                .build();

        pipeDocService.stubFor(
                method("SavePipeDoc")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
        LOG.debug("Mocked SavePipeDoc for specific doc: docId={}, nodeId={}", expectedDoc.getDocId(), nodeId);
    }

    /**
     * Mock SavePipeDoc for a specific connector ID.
     * Useful for testing connector-specific save behavior.
     *
     * @param connectorId The connector ID to match
     * @param nodeId      The node ID to return
     * @param drive       The drive name to return
     */
    public void mockSavePipeDocForConnector(String connectorId, String nodeId, String drive) {
        // Note: WireMock gRPC doesn't support partial matching well, so we use a general response
        // and the test should use the same connectorId
        SavePipeDocResponse response = SavePipeDocResponse.newBuilder()
                .setNodeId(nodeId)
                .setDrive(drive)
                .setS3Key("pipedocs/" + connectorId + "/" + nodeId + "/" + UUID.randomUUID().toString())
                .setSizeBytes(1024)
                .setChecksum("sha256:mock-checksum")
                .build();

        pipeDocService.stubFor(
                method("SavePipeDoc")
                        .willReturn(message(response))
        );
        LOG.debug("Mocked SavePipeDoc for connector: connectorId={}, nodeId={}", connectorId, nodeId);
    }

    /**
     * Mock SavePipeDoc to return UNAVAILABLE error.
     * Used for testing retry behavior.
     */
    public void mockSavePipeDocUnavailable() {
        pipeDocService.stubFor(
                method("SavePipeDoc")
                        .willReturn(WireMockGrpc.Status.UNAVAILABLE,
                                "PipeDoc save service temporarily unavailable")
        );
    }

    /**
     * Mock SavePipeDoc to return INTERNAL error.
     * Used for testing error handling.
     *
     * @param errorMessage The error message
     */
    public void mockSavePipeDocError(String errorMessage) {
        pipeDocService.stubFor(
                method("SavePipeDoc")
                        .willReturn(WireMockGrpc.Status.INTERNAL, errorMessage)
        );
    }

    /**
     * Mock SavePipeDoc to return RESOURCE_EXHAUSTED error.
     * Used for testing storage quota scenarios.
     *
     * @param errorMessage The error message
     */
    public void mockSavePipeDocStorageFull(String errorMessage) {
        pipeDocService.stubFor(
                method("SavePipeDoc")
                        .willReturn(WireMockGrpc.Status.RESOURCE_EXHAUSTED, errorMessage)
        );
    }

    /**
     * Mock SavePipeDoc to return ALREADY_EXISTS error.
     * Used for testing duplicate document scenarios.
     *
     * @param docId The document ID that already exists
     */
    public void mockSavePipeDocAlreadyExists(String docId) {
        pipeDocService.stubFor(
                method("SavePipeDoc")
                        .willReturn(WireMockGrpc.Status.ALREADY_EXISTS,
                                "Document already exists: " + docId)
        );
    }

    // ============================================
    // Utility Methods
    // ============================================

    /**
     * Create a composite key for document lookup.
     */
    private String makeKey(String docId, String accountId) {
        return docId + "::" + accountId;
    }

    /**
     * Create a composite key for blob lookup.
     */
    private String makeBlobKey(FileStorageReference storageRef) {
        String versionId = storageRef.hasVersionId() ? storageRef.getVersionId() : "";
        return storageRef.getDriveName() + "::" + storageRef.getObjectKey() + "::" + versionId;
    }

    /**
     * Get a registered document.
     *
     * @param docId     Document ID
     * @param accountId Account ID
     * @return The registered document or null if not found
     */
    public RegisteredDoc getRegisteredDoc(String docId, String accountId) {
        return registeredDocs.get(makeKey(docId, accountId));
    }

    /**
     * Check if a document is registered.
     *
     * @param docId     Document ID
     * @param accountId Account ID
     * @return true if the document is registered
     */
    public boolean hasRegisteredDoc(String docId, String accountId) {
        return registeredDocs.containsKey(makeKey(docId, accountId));
    }

    /**
     * Get the count of registered documents.
     *
     * @return Number of registered documents
     */
    public int getRegisteredDocCount() {
        return registeredDocs.size();
    }

    /**
     * Get a registered blob.
     *
     * @param storageRef The file storage reference
     * @return The registered blob or null if not found
     */
    public RegisteredBlob getRegisteredBlob(FileStorageReference storageRef) {
        return registeredBlobs.get(makeBlobKey(storageRef));
    }

    /**
     * Check if a blob is registered.
     *
     * @param storageRef The file storage reference
     * @return true if the blob is registered
     */
    public boolean hasRegisteredBlob(FileStorageReference storageRef) {
        return registeredBlobs.containsKey(makeBlobKey(storageRef));
    }

    /**
     * Get the count of registered blobs.
     *
     * @return Number of registered blobs
     */
    public int getRegisteredBlobCount() {
        return registeredBlobs.size();
    }

    /**
     * Reset all WireMock stubs for the PipeDoc service.
     */
    public void reset() {
        pipeDocService.resetAll();
        registeredDocs.clear();
        registeredBlobs.clear();
    }

    // ============================================
    // Inner Classes
    // ============================================

    /**
     * Represents a registered document.
     */
    public static class RegisteredDoc {
        public final String docId;
        public final String accountId;
        public final PipeDoc pipeDoc;
        public final String nodeId;
        public final String drive;

        public RegisteredDoc(String docId, String accountId, PipeDoc pipeDoc, String nodeId, String drive) {
            this.docId = docId;
            this.accountId = accountId;
            this.pipeDoc = pipeDoc;
            this.nodeId = nodeId;
            this.drive = drive;
        }
    }

    /**
     * Represents a registered blob.
     */
    public static class RegisteredBlob {
        public final FileStorageReference storageRef;
        public final ByteString blobData;

        public RegisteredBlob(FileStorageReference storageRef, ByteString blobData) {
            this.storageRef = storageRef;
            this.blobData = blobData;
        }
    }
}
