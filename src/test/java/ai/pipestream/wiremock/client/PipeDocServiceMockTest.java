package ai.pipestream.wiremock.client;

import ai.pipestream.data.v1.FileStorageReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PipeDocServiceMock.
 */
class PipeDocServiceMockTest {

    private static WireMockServer wireMockServer;
    private static WireMock wireMock;
    private static ManagedChannel channel;
    private static PipeDocServiceGrpc.PipeDocServiceBlockingStub stub;
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
        
        // Create gRPC channel for integration tests
        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
        stub = PipeDocServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
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

    // ============================================
    // GetBlob Tests
    // ============================================

    @Test
    @DisplayName("Should register blob and retrieve it")
    void testRegisterBlob() {
        FileStorageReference storageRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test-blob.bin")
                .build();
        ByteString blobData = ByteString.copyFromUtf8("Test blob content");

        pipeDocMock.registerBlob(storageRef, blobData);

        assertTrue(pipeDocMock.hasRegisteredBlob(storageRef));
        assertEquals(1, pipeDocMock.getRegisteredBlobCount());

        PipeDocServiceMock.RegisteredBlob regBlob = pipeDocMock.getRegisteredBlob(storageRef);
        assertNotNull(regBlob);
        assertEquals(storageRef, regBlob.storageRef);
        assertEquals(blobData, regBlob.blobData);
    }

    @Test
    @DisplayName("Should register blob with version ID")
    void testRegisterBlobWithVersion() {
        FileStorageReference storageRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test-blob.bin")
                .setVersionId("version-123")
                .build();
        ByteString blobData = ByteString.copyFromUtf8("Versioned blob content");

        pipeDocMock.registerBlob(storageRef, blobData);

        assertTrue(pipeDocMock.hasRegisteredBlob(storageRef));
        PipeDocServiceMock.RegisteredBlob regBlob = pipeDocMock.getRegisteredBlob(storageRef);
        assertEquals("version-123", regBlob.storageRef.getVersionId());
    }

    @Test
    @DisplayName("Should return false for unregistered blob")
    void testUnregisteredBlob() {
        FileStorageReference storageRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("nonexistent.bin")
                .build();

        assertFalse(pipeDocMock.hasRegisteredBlob(storageRef));
        assertNull(pipeDocMock.getRegisteredBlob(storageRef));
    }

    @Test
    @DisplayName("Should get blob via gRPC")
    void testGetBlobViaGrpc() {
        FileStorageReference storageRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test-blob.bin")
                .build();
        ByteString expectedData = ByteString.copyFromUtf8("Test blob content for gRPC");

        pipeDocMock.registerBlob(storageRef, expectedData);

        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(storageRef)
                .build();

        GetBlobResponse response = stub.getBlob(request);

        assertNotNull(response);
        assertEquals(expectedData, response.getData());
    }

    @Test
    @DisplayName("Should return NOT_FOUND for unregistered blob via gRPC")
    void testGetBlobNotFoundViaGrpc() {
        FileStorageReference storageRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("nonexistent.bin")
                .build();

        pipeDocMock.mockGetBlobNotFound(storageRef);

        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(storageRef)
                .build();

        io.grpc.StatusRuntimeException exception = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.getBlob(request)
        );

        assertEquals(io.grpc.Status.Code.NOT_FOUND, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("Blob not found"));
    }

    @Test
    @DisplayName("Should return UNAVAILABLE for blob via gRPC")
    void testGetBlobUnavailableViaGrpc() {
        FileStorageReference storageRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("unavailable-blob.bin")
                .build();

        pipeDocMock.mockGetBlobUnavailable(storageRef);

        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(storageRef)
                .build();

        io.grpc.StatusRuntimeException exception = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.getBlob(request)
        );

        assertEquals(io.grpc.Status.Code.UNAVAILABLE, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("temporarily unavailable"));
    }

    @Test
    @DisplayName("Should mock GetBlob to return specific blob data")
    void testMockGetBlobReturns() {
        ByteString blobData = ByteString.copyFromUtf8("Mocked blob data");

        pipeDocMock.mockGetBlobReturns(blobData);

        FileStorageReference anyRef = FileStorageReference.newBuilder()
                .setDriveName("any-drive")
                .setObjectKey("any-key.bin")
                .build();

        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(anyRef)
                .build();

        GetBlobResponse response = stub.getBlob(request);

        assertNotNull(response);
        assertEquals(blobData, response.getData());
    }

    @Test
    @DisplayName("Should mock GetBlob error scenario")
    void testMockGetBlobError() {
        pipeDocMock.mockGetBlobError("Internal blob service error");

        FileStorageReference storageRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("error-blob.bin")
                .build();

        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(storageRef)
                .build();

        io.grpc.StatusRuntimeException exception = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.getBlob(request)
        );

        assertEquals(io.grpc.Status.Code.INTERNAL, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("Internal blob service error"));
    }

    @Test
    @DisplayName("Should reset all blob registrations")
    void testResetBlobs() {
        FileStorageReference storageRef1 = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("blob-1.bin")
                .build();
        FileStorageReference storageRef2 = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("blob-2.bin")
                .build();

        pipeDocMock.registerBlob(storageRef1, ByteString.copyFromUtf8("Blob 1"));
        pipeDocMock.registerBlob(storageRef2, ByteString.copyFromUtf8("Blob 2"));

        assertEquals(2, pipeDocMock.getRegisteredBlobCount());

        pipeDocMock.reset();

        assertEquals(0, pipeDocMock.getRegisteredBlobCount());
        assertFalse(pipeDocMock.hasRegisteredBlob(storageRef1));
        assertFalse(pipeDocMock.hasRegisteredBlob(storageRef2));
    }

    @Test
    @DisplayName("Should handle large blob data")
    void testLargeBlob() {
        // Create a 1MB blob
        byte[] largeData = new byte[1024 * 1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        ByteString largeBlob = ByteString.copyFrom(largeData);

        FileStorageReference storageRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("large-blob.bin")
                .build();

        pipeDocMock.registerBlob(storageRef, largeBlob);

        GetBlobRequest request = GetBlobRequest.newBuilder()
                .setStorageRef(storageRef)
                .build();

        GetBlobResponse response = stub.getBlob(request);

        assertNotNull(response);
        assertEquals(largeBlob.size(), response.getData().size());
        assertEquals(largeBlob, response.getData());
    }

    @Test
    @DisplayName("Should handle different blob types (PDF, text, binary)")
    void testDifferentBlobTypes() {
        // PDF blob (starts with %PDF)
        ByteString pdfBlob = ByteString.copyFromUtf8("%PDF-1.4\nTest PDF content");
        FileStorageReference pdfRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test.pdf")
                .build();
        pipeDocMock.registerBlob(pdfRef, pdfBlob);

        // Text blob
        ByteString textBlob = ByteString.copyFromUtf8("Plain text content");
        FileStorageReference textRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test.txt")
                .build();
        pipeDocMock.registerBlob(textRef, textBlob);

        // Binary blob
        ByteString binaryBlob = ByteString.copyFrom(new byte[]{0x00, 0x01, 0x02, 0x03, (byte) 0xFF});
        FileStorageReference binaryRef = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test.bin")
                .build();
        pipeDocMock.registerBlob(binaryRef, binaryBlob);

        // Verify all can be retrieved
        assertEquals(pdfBlob, stub.getBlob(GetBlobRequest.newBuilder().setStorageRef(pdfRef).build()).getData());
        assertEquals(textBlob, stub.getBlob(GetBlobRequest.newBuilder().setStorageRef(textRef).build()).getData());
        assertEquals(binaryBlob, stub.getBlob(GetBlobRequest.newBuilder().setStorageRef(binaryRef).build()).getData());
    }

    @Test
    @DisplayName("Should initialize defaults with test blobs")
    void testInitializeDefaultsWithBlobs() {
        PipeDocServiceMock freshMock = new PipeDocServiceMock();
        freshMock.initializeDefaults(wireMock);

        // Verify default blobs were registered
        FileStorageReference testBlobRef1 = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test-blob-1.bin")
                .build();
        FileStorageReference testBlobRef2 = FileStorageReference.newBuilder()
                .setDriveName("test-drive")
                .setObjectKey("test-blob-2.bin")
                .build();

        assertTrue(freshMock.hasRegisteredBlob(testBlobRef1));
        assertTrue(freshMock.hasRegisteredBlob(testBlobRef2));
        assertTrue(freshMock.getRegisteredBlobCount() >= 2);
    }

    // ============================================
    // SavePipeDoc Tests
    // ============================================

    @Test
    @DisplayName("Should mock SavePipeDoc with full response")
    void testMockSavePipeDocFull() {
        pipeDocMock.mockSavePipeDoc("node-123", "my-drive", "pipedocs/test/doc.bin");

        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId("test-doc-id")
                .build();

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .setDrive("my-drive")
                .build();

        SavePipeDocResponse response = stub.savePipeDoc(request);

        assertNotNull(response);
        assertEquals("node-123", response.getNodeId());
        assertEquals("my-drive", response.getDrive());
        assertEquals("pipedocs/test/doc.bin", response.getS3Key());
        assertTrue(response.getSizeBytes() > 0);
        assertTrue(response.getChecksum().startsWith("sha256:"));
    }

    @Test
    @DisplayName("Should mock SavePipeDoc with just nodeId")
    void testMockSavePipeDocSimple() {
        pipeDocMock.mockSavePipeDoc("simple-node-id");

        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId("simple-doc")
                .build();

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .build();

        SavePipeDocResponse response = stub.savePipeDoc(request);

        assertNotNull(response);
        assertEquals("simple-node-id", response.getNodeId());
        assertEquals("default-drive", response.getDrive());
        assertTrue(response.getS3Key().contains("simple-node-id"));
    }

    @Test
    @DisplayName("Should mock SavePipeDoc for specific PipeDoc request")
    void testMockSavePipeDocWithRequest() {
        PipeDoc expectedDoc = PipeDoc.newBuilder()
                .setDocId("specific-doc-id")
                .build();

        pipeDocMock.mockSavePipeDocWithRequest(expectedDoc, "specific-node", "specific-drive", "specific-key.bin");

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(expectedDoc)
                .setDrive("specific-drive")
                .build();

        SavePipeDocResponse response = stub.savePipeDoc(request);

        assertNotNull(response);
        assertEquals("specific-node", response.getNodeId());
        assertEquals("specific-drive", response.getDrive());
        assertEquals("specific-key.bin", response.getS3Key());
    }

    @Test
    @DisplayName("Should mock SavePipeDoc for connector")
    void testMockSavePipeDocForConnector() {
        pipeDocMock.mockSavePipeDocForConnector("my-connector", "connector-node", "connector-drive");

        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId("connector-doc")
                .build();

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .setConnectorId("my-connector")
                .build();

        SavePipeDocResponse response = stub.savePipeDoc(request);

        assertNotNull(response);
        assertEquals("connector-node", response.getNodeId());
        assertEquals("connector-drive", response.getDrive());
        assertTrue(response.getS3Key().contains("my-connector"));
    }

    @Test
    @DisplayName("Should mock SavePipeDoc UNAVAILABLE error")
    void testMockSavePipeDocUnavailable() {
        pipeDocMock.mockSavePipeDocUnavailable();

        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId("unavailable-doc")
                .build();

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .build();

        io.grpc.StatusRuntimeException exception = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.savePipeDoc(request)
        );

        assertEquals(io.grpc.Status.Code.UNAVAILABLE, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("temporarily unavailable"));
    }

    @Test
    @DisplayName("Should mock SavePipeDoc INTERNAL error")
    void testMockSavePipeDocError() {
        pipeDocMock.mockSavePipeDocError("Storage write failure");

        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId("error-doc")
                .build();

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .build();

        io.grpc.StatusRuntimeException exception = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.savePipeDoc(request)
        );

        assertEquals(io.grpc.Status.Code.INTERNAL, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("Storage write failure"));
    }

    @Test
    @DisplayName("Should mock SavePipeDoc RESOURCE_EXHAUSTED error for storage full")
    void testMockSavePipeDocStorageFull() {
        pipeDocMock.mockSavePipeDocStorageFull("Storage quota exceeded");

        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId("quota-doc")
                .build();

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .build();

        io.grpc.StatusRuntimeException exception = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.savePipeDoc(request)
        );

        assertEquals(io.grpc.Status.Code.RESOURCE_EXHAUSTED, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("Storage quota exceeded"));
    }

    @Test
    @DisplayName("Should mock SavePipeDoc ALREADY_EXISTS error")
    void testMockSavePipeDocAlreadyExists() {
        pipeDocMock.mockSavePipeDocAlreadyExists("duplicate-doc-id");

        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId("duplicate-doc-id")
                .build();

        SavePipeDocRequest request = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .build();

        io.grpc.StatusRuntimeException exception = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.savePipeDoc(request)
        );

        assertEquals(io.grpc.Status.Code.ALREADY_EXISTS, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("duplicate-doc-id"));
    }

    @Test
    @DisplayName("Should support complete save and retrieve workflow")
    void testSaveAndRetrieveWorkflow() {
        // 1. Save a document
        String docId = "workflow-doc";
        String nodeId = "workflow-node";
        String drive = "workflow-drive";
        String s3Key = "workflow-key.bin";

        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId(docId)
                .build();

        pipeDocMock.mockSavePipeDoc(nodeId, drive, s3Key);

        SavePipeDocRequest saveRequest = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .setDrive(drive)
                .build();

        SavePipeDocResponse saveResponse = stub.savePipeDoc(saveRequest);
        assertEquals(nodeId, saveResponse.getNodeId());

        // 2. Register the document for retrieval
        pipeDocMock.registerPipeDoc(docId, "test-account", docToSave, nodeId, drive);

        // 3. Retrieve the document
        assertTrue(pipeDocMock.hasRegisteredDoc(docId, "test-account"));
        PipeDocServiceMock.RegisteredDoc regDoc = pipeDocMock.getRegisteredDoc(docId, "test-account");
        assertEquals(nodeId, regDoc.nodeId);
        assertEquals(drive, regDoc.drive);
    }

    @Test
    @DisplayName("Should support save with blob workflow")
    void testSaveWithBlobWorkflow() {
        String docId = "blob-workflow-doc";
        String nodeId = "blob-workflow-node";
        String drive = "blob-workflow-drive";

        // 1. Register a blob that will be associated with the document
        FileStorageReference blobRef = FileStorageReference.newBuilder()
                .setDriveName(drive)
                .setObjectKey("blobs/" + docId + "/content.bin")
                .build();
        ByteString blobData = ByteString.copyFromUtf8("Document content to be saved");
        pipeDocMock.registerBlob(blobRef, blobData);

        // 2. Create and save the document
        PipeDoc docToSave = PipeDoc.newBuilder()
                .setDocId(docId)
                .build();

        pipeDocMock.mockSavePipeDoc(nodeId, drive, "pipedocs/" + docId + "/doc.bin");

        SavePipeDocRequest saveRequest = SavePipeDocRequest.newBuilder()
                .setPipedoc(docToSave)
                .setDrive(drive)
                .build();

        SavePipeDocResponse saveResponse = stub.savePipeDoc(saveRequest);
        assertEquals(nodeId, saveResponse.getNodeId());

        // 3. Verify the blob can still be retrieved
        GetBlobRequest blobRequest = GetBlobRequest.newBuilder()
                .setStorageRef(blobRef)
                .build();
        GetBlobResponse blobResponse = stub.getBlob(blobRequest);
        assertEquals(blobData, blobResponse.getData());
    }
}
