package ai.pipestream.wiremock.client;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
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
 * This mock supports testing of document hydration scenarios for the Kafka Sidecar including:
 * <ul>
 *   <li><b>GetPipeDocByReference</b>: Return PipeDoc for a given DocumentReference</li>
 *   <li><b>Document not found</b>: Return NOT_FOUND status for missing documents</li>
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
 * // Mock a not found scenario
 * mock.mockGetByReferenceNotFound("missing-doc", "account-456");
 *
 * // Mock an unavailable scenario for retry testing
 * mock.mockGetByReferenceUnavailable();
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

        // Set up default stub that returns NOT_FOUND for unknown documents
        setupDefaultStubs();

        LOG.info("Added {} document stubs for PipeDocService", registeredDocs.size());
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
     * Set up default stubs for unknown documents.
     */
    private void setupDefaultStubs() {
        // Default response for any unmatched request - returns empty response
        // Individual document registrations will have higher priority
        GetPipeDocByReferenceResponse defaultResponse = GetPipeDocByReferenceResponse.newBuilder()
                .build();

        pipeDocService.stubFor(
                method("GetPipeDocByReference")
                        .willReturn(message(defaultResponse))
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
    // Utility Methods
    // ============================================

    /**
     * Create a composite key for document lookup.
     */
    private String makeKey(String docId, String accountId) {
        return docId + "::" + accountId;
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
     * Reset all WireMock stubs for the PipeDoc service.
     */
    public void reset() {
        pipeDocService.resetAll();
        registeredDocs.clear();
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
}
