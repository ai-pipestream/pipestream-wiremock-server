package ai.pipestream.wiremock.client;

import ai.pipestream.data.v1.PipeDoc;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PipeDocServiceMock.
 */
class PipeDocServiceMockTest {

    private static WireMockServer wireMockServer;
    private static WireMock wireMock;
    private PipeDocServiceMock pipeDocMock;

    @BeforeAll
    static void setUp() {
        // Start WireMock server with gRPC extension
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new GrpcExtensionFactory())
                .withRootDirectory("build/resources/test/wiremock"));
        wireMockServer.start();

        wireMock = new WireMock(wireMockServer.port());
    }

    @AfterAll
    static void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @BeforeEach
    void resetMocks() {
        pipeDocMock = new PipeDocServiceMock(wireMock);
    }

    @Test
    @DisplayName("Should register PipeDoc and retrieve it")
    void testRegisterPipeDoc() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-123")
                .build();

        pipeDocMock.registerPipeDoc("doc-123", "account-456", doc, "node-1", "drive-1");

        assertTrue(pipeDocMock.hasRegisteredDoc("doc-123", "account-456"));
        assertEquals(1, pipeDocMock.getRegisteredDocCount());

        PipeDocServiceMock.RegisteredDoc regDoc = pipeDocMock.getRegisteredDoc("doc-123", "account-456");
        assertNotNull(regDoc);
        assertEquals("doc-123", regDoc.docId);
        assertEquals("account-456", regDoc.accountId);
        assertEquals("node-1", regDoc.nodeId);
        assertEquals("drive-1", regDoc.drive);
    }

    @Test
    @DisplayName("Should register PipeDoc with defaults")
    void testRegisterPipeDocWithDefaults() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-456")
                .build();

        pipeDocMock.registerPipeDoc("doc-456", "account-789", doc);

        assertTrue(pipeDocMock.hasRegisteredDoc("doc-456", "account-789"));

        PipeDocServiceMock.RegisteredDoc regDoc = pipeDocMock.getRegisteredDoc("doc-456", "account-789");
        assertEquals("default-node", regDoc.nodeId);
        assertEquals("default-drive", regDoc.drive);
    }

    @Test
    @DisplayName("Should return false for unregistered document")
    void testUnregisteredDoc() {
        assertFalse(pipeDocMock.hasRegisteredDoc("nonexistent", "account-123"));
        assertNull(pipeDocMock.getRegisteredDoc("nonexistent", "account-123"));
    }

    @Test
    @DisplayName("Should reset all registrations")
    void testReset() {
        PipeDoc doc1 = PipeDoc.newBuilder().setDocId("doc-1").build();
        PipeDoc doc2 = PipeDoc.newBuilder().setDocId("doc-2").build();

        pipeDocMock.registerPipeDoc("doc-1", "account-1", doc1);
        pipeDocMock.registerPipeDoc("doc-2", "account-1", doc2);

        assertEquals(2, pipeDocMock.getRegisteredDocCount());

        pipeDocMock.reset();

        assertEquals(0, pipeDocMock.getRegisteredDocCount());
        assertFalse(pipeDocMock.hasRegisteredDoc("doc-1", "account-1"));
    }

    @Test
    @DisplayName("Should get service name")
    void testGetServiceName() {
        assertEquals("ai.pipestream.repository.pipedoc.v1.PipeDocService", pipeDocMock.getServiceName());
    }

    @Test
    @DisplayName("Should initialize defaults without error")
    void testInitializeDefaults() {
        PipeDocServiceMock freshMock = new PipeDocServiceMock();
        assertDoesNotThrow(() -> freshMock.initializeDefaults(wireMock));

        // Verify default documents were registered
        assertTrue(freshMock.hasRegisteredDoc("test-doc-1", "test-account"));
        assertTrue(freshMock.hasRegisteredDoc("test-doc-2", "test-account"));
        assertEquals(2, freshMock.getRegisteredDocCount());
    }

    @Test
    @DisplayName("Should mock not found scenario without error")
    void testMockNotFound() {
        assertDoesNotThrow(() ->
            pipeDocMock.mockGetByReferenceNotFound("missing-doc", "account-123")
        );
    }

    @Test
    @DisplayName("Should mock unavailable scenario without error")
    void testMockUnavailable() {
        assertDoesNotThrow(() -> pipeDocMock.mockGetByReferenceUnavailable());
        assertDoesNotThrow(() ->
            pipeDocMock.mockGetByReferenceUnavailable("doc-123", "account-456")
        );
    }

    @Test
    @DisplayName("Should mock error scenario without error")
    void testMockError() {
        assertDoesNotThrow(() ->
            pipeDocMock.mockGetByReferenceError("Internal server error")
        );
    }

    @Test
    @DisplayName("Should mock returns specific PipeDoc")
    void testMockReturns() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("custom-doc")
                .build();

        assertDoesNotThrow(() ->
            pipeDocMock.mockGetByReferenceReturns(doc, "custom-node", "custom-drive")
        );
    }
}
