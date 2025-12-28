package ai.pipestream.wiremock.client;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EngineV1ServiceMock.
 * <p>
 * Includes both mock setup tests and actual gRPC integration tests
 * to verify the mocks work correctly when called via gRPC.
 */
class EngineV1ServiceMockTest {

    private static WireMockServer wireMockServer;
    private static WireMock wireMock;
    private static ManagedChannel channel;
    private static EngineV1ServiceGrpc.EngineV1ServiceBlockingStub stub;
    private EngineV1ServiceMock engineMock;

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
        stub = EngineV1ServiceGrpc.newBlockingStub(channel);
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
        engineMock = new EngineV1ServiceMock(wireMock);
    }

    @Test
    @DisplayName("Should get service name")
    void testGetServiceName() {
        assertEquals("ai.pipestream.engine.v1.EngineV1Service", engineMock.getServiceName());
    }

    @Test
    @DisplayName("Should initialize defaults without error")
    void testInitializeDefaults() {
        EngineV1ServiceMock freshMock = new EngineV1ServiceMock();
        assertDoesNotThrow(() -> freshMock.initializeDefaults(wireMock));
    }

    // ============================================
    // IntakeHandoff Mock Setup Tests
    // ============================================

    @Test
    @DisplayName("Should mock IntakeHandoff accepted without error")
    void testMockIntakeHandoffAccepted() {
        assertDoesNotThrow(() -> engineMock.mockIntakeHandoffAccepted());
    }

    @Test
    @DisplayName("Should mock IntakeHandoff accepted with specific values")
    void testMockIntakeHandoffAcceptedWithValues() {
        assertDoesNotThrow(() ->
            engineMock.mockIntakeHandoffAccepted("stream-123", "entry-parser")
        );
    }

    @Test
    @DisplayName("Should mock IntakeHandoff accepted for datasource")
    void testMockIntakeHandoffAcceptedForDatasource() {
        assertDoesNotThrow(() ->
            engineMock.mockIntakeHandoffAcceptedForDatasource(
                "datasource-001", "stream-456", "entry-chunker")
        );
    }

    @Test
    @DisplayName("Should mock IntakeHandoff rejected")
    void testMockIntakeHandoffRejected() {
        assertDoesNotThrow(() ->
            engineMock.mockIntakeHandoffRejected("Queue full")
        );
    }

    @Test
    @DisplayName("Should mock IntakeHandoff unavailable")
    void testMockIntakeHandoffUnavailable() {
        assertDoesNotThrow(() -> engineMock.mockIntakeHandoffUnavailable());
    }

    @Test
    @DisplayName("Should mock IntakeHandoff queue full")
    void testMockIntakeHandoffQueueFull() {
        assertDoesNotThrow(() -> engineMock.mockIntakeHandoffQueueFull(1000));
    }

    // ============================================
    // IntakeHandoff gRPC Integration Tests
    // ============================================

    @Test
    @DisplayName("Should receive accepted response via gRPC")
    void testIntakeHandoffAcceptedViaGrpc() {
        String streamId = "grpc-stream-123";
        String entryNode = "grpc-entry-parser";
        engineMock.mockIntakeHandoffAccepted(streamId, entryNode);

        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("original-stream-id")
                .setDocument(PipeDoc.newBuilder().setDocId("doc-1").build())
                .build();

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(stream)
                .setDatasourceId("test-datasource")
                .setAccountId("test-account")
                .build();

        IntakeHandoffResponse response = stub.intakeHandoff(request);

        assertNotNull(response);
        assertTrue(response.getAccepted());
        assertEquals(streamId, response.getAssignedStreamId());
        assertEquals(entryNode, response.getEntryNodeId());
        assertEquals("Stream accepted for processing", response.getMessage());
    }

    @Test
    @DisplayName("Should receive rejected response via gRPC")
    void testIntakeHandoffRejectedViaGrpc() {
        String rejectMessage = "Datasource disabled";
        engineMock.mockIntakeHandoffRejected(rejectMessage);

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("stream-1").build())
                .setDatasourceId("disabled-datasource")
                .build();

        IntakeHandoffResponse response = stub.intakeHandoff(request);

        assertNotNull(response);
        assertFalse(response.getAccepted());
        assertEquals(rejectMessage, response.getMessage());
    }

    @Test
    @DisplayName("Should receive UNAVAILABLE error via gRPC")
    void testIntakeHandoffUnavailableViaGrpc() {
        engineMock.mockIntakeHandoffUnavailable();

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("stream-1").build())
                .build();

        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.intakeHandoff(request)
        );

        assertEquals(io.grpc.Status.Code.UNAVAILABLE, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("temporarily unavailable"));
    }

    @Test
    @DisplayName("Should receive queue full rejection via gRPC")
    void testIntakeHandoffQueueFullViaGrpc() {
        long queueDepth = 5000;
        engineMock.mockIntakeHandoffQueueFull(queueDepth);

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("stream-1").build())
                .build();

        IntakeHandoffResponse response = stub.intakeHandoff(request);

        assertNotNull(response);
        assertFalse(response.getAccepted());
        assertTrue(response.getMessage().contains("Queue full"));
        assertEquals(queueDepth, response.getQueueDepth());
    }

    // ============================================
    // ProcessNode Mock Setup Tests
    // ============================================

    @Test
    @DisplayName("Should mock ProcessNode success without error")
    void testMockProcessNodeSuccess() {
        assertDoesNotThrow(() -> engineMock.mockProcessNodeSuccess());
    }

    @Test
    @DisplayName("Should mock ProcessNode success with updated stream")
    void testMockProcessNodeSuccessWithStream() {
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("updated-stream-123")
                .build();

        assertDoesNotThrow(() -> engineMock.mockProcessNodeSuccess(stream));
    }

    @Test
    @DisplayName("Should mock ProcessNode success with metrics")
    void testMockProcessNodeSuccessWithMetrics() {
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("stream-with-metrics")
                .build();

        ProcessingMetrics metrics = ProcessingMetrics.newBuilder()
                .setProcessingTimeMs(100)
                .build();

        assertDoesNotThrow(() ->
            engineMock.mockProcessNodeSuccessWithMetrics(stream, metrics)
        );
    }

    @Test
    @DisplayName("Should mock ProcessNode failure")
    void testMockProcessNodeFailure() {
        assertDoesNotThrow(() ->
            engineMock.mockProcessNodeFailure("Processing failed")
        );
    }

    @Test
    @DisplayName("Should mock ProcessNode unavailable")
    void testMockProcessNodeUnavailable() {
        assertDoesNotThrow(() -> engineMock.mockProcessNodeUnavailable());
    }

    @Test
    @DisplayName("Should mock ProcessNode internal error")
    void testMockProcessNodeInternalError() {
        assertDoesNotThrow(() ->
            engineMock.mockProcessNodeInternalError("Internal error occurred")
        );
    }

    // ============================================
    // ProcessNode gRPC Integration Tests
    // ============================================

    @Test
    @DisplayName("Should receive ProcessNode success via gRPC")
    void testProcessNodeSuccessViaGrpc() {
        engineMock.mockProcessNodeSuccess();

        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("process-stream-1")
                .setCurrentNodeId("node-chunker")
                .setDocument(PipeDoc.newBuilder().setDocId("doc-1").build())
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        ProcessNodeResponse response = stub.processNode(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals("Node processed successfully", response.getMessage());
    }

    @Test
    @DisplayName("Should receive ProcessNode success with updated stream via gRPC")
    void testProcessNodeSuccessWithStreamViaGrpc() {
        PipeStream updatedStream = PipeStream.newBuilder()
                .setStreamId("updated-stream-after-processing")
                .setCurrentNodeId("node-embedder")
                .setDocument(PipeDoc.newBuilder()
                        .setDocId("processed-doc")
                        .build())
                .build();

        engineMock.mockProcessNodeSuccess(updatedStream);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("original-stream").build())
                .build();

        ProcessNodeResponse response = stub.processNode(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertTrue(response.hasUpdatedStream());
        assertEquals("updated-stream-after-processing", response.getUpdatedStream().getStreamId());
        assertEquals("node-embedder", response.getUpdatedStream().getCurrentNodeId());
    }

    @Test
    @DisplayName("Should receive ProcessNode success with metrics via gRPC")
    void testProcessNodeSuccessWithMetricsViaGrpc() {
        PipeStream updatedStream = PipeStream.newBuilder()
                .setStreamId("stream-with-metrics")
                .build();

        ProcessingMetrics metrics = ProcessingMetrics.newBuilder()
                .setProcessingTimeMs(150)
                .setNodeId("test-node")
                .setModuleId("test-module")
                .setCacheHit(true)
                .setHopCount(3)
                .build();

        engineMock.mockProcessNodeSuccessWithMetrics(updatedStream, metrics);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("original").build())
                .build();

        ProcessNodeResponse response = stub.processNode(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertTrue(response.hasMetrics());
        assertEquals(150, response.getMetrics().getProcessingTimeMs());
        assertEquals("test-node", response.getMetrics().getNodeId());
        assertEquals("test-module", response.getMetrics().getModuleId());
        assertTrue(response.getMetrics().getCacheHit());
        assertEquals(3, response.getMetrics().getHopCount());
    }

    @Test
    @DisplayName("Should receive ProcessNode failure via gRPC")
    void testProcessNodeFailureViaGrpc() {
        String errorMessage = "Module tika-parser failed to parse document";
        engineMock.mockProcessNodeFailure(errorMessage);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("failing-stream").build())
                .build();

        ProcessNodeResponse response = stub.processNode(request);

        assertNotNull(response);
        assertFalse(response.getSuccess());
        assertEquals(errorMessage, response.getMessage());
    }

    @Test
    @DisplayName("Should receive ProcessNode UNAVAILABLE error via gRPC")
    void testProcessNodeUnavailableViaGrpc() {
        engineMock.mockProcessNodeUnavailable();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("stream-1").build())
                .build();

        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.processNode(request)
        );

        assertEquals(io.grpc.Status.Code.UNAVAILABLE, exception.getStatus().getCode());
    }

    @Test
    @DisplayName("Should receive ProcessNode INTERNAL error via gRPC")
    void testProcessNodeInternalErrorViaGrpc() {
        String errorMessage = "Database connection lost";
        engineMock.mockProcessNodeInternalError(errorMessage);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("stream-1").build())
                .build();

        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.processNode(request)
        );

        assertEquals(io.grpc.Status.Code.INTERNAL, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains(errorMessage));
    }

    // ============================================
    // ProcessStream Mock Setup Tests (streaming RPC - setup only tests)
    // Note: ProcessStream is a bidirectional streaming RPC which requires
    // async stubs for full integration testing. We verify mock setup works.
    // ============================================

    @Test
    @DisplayName("Should mock ProcessStream success")
    void testMockProcessStreamSuccess() {
        assertDoesNotThrow(() -> engineMock.mockProcessStreamSuccess());
    }

    @Test
    @DisplayName("Should mock ProcessStream success with updated stream")
    void testMockProcessStreamSuccessWithStream() {
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("processed-stream")
                .build();

        assertDoesNotThrow(() -> engineMock.mockProcessStreamSuccess(stream));
    }

    // ============================================
    // GetHealth Tests
    // ============================================

    @Test
    @DisplayName("Should mock GetHealth healthy")
    void testMockGetHealthHealthy() {
        assertDoesNotThrow(() -> engineMock.mockGetHealthHealthy());
    }

    @Test
    @DisplayName("Should mock GetHealth unhealthy")
    void testMockGetHealthUnhealthy() {
        assertDoesNotThrow(() ->
            engineMock.mockGetHealthUnhealthy("Service degraded")
        );
    }

    @Test
    @DisplayName("Should receive GetHealth healthy via gRPC")
    void testGetHealthHealthyViaGrpc() {
        engineMock.mockGetHealthHealthy();

        GetHealthRequest request = GetHealthRequest.newBuilder().build();

        GetHealthResponse response = stub.getHealth(request);

        assertNotNull(response);
        assertEquals(EngineHealth.ENGINE_HEALTH_HEALTHY, response.getHealth());
    }

    @Test
    @DisplayName("Should receive GetHealth unhealthy via gRPC")
    void testGetHealthUnhealthyViaGrpc() {
        engineMock.mockGetHealthUnhealthy("Service overloaded");

        GetHealthRequest request = GetHealthRequest.newBuilder().build();

        GetHealthResponse response = stub.getHealth(request);

        assertNotNull(response);
        assertEquals(EngineHealth.ENGINE_HEALTH_UNHEALTHY, response.getHealth());
    }

    // ============================================
    // Reset Tests
    // ============================================

    @Test
    @DisplayName("Should reset without error")
    void testReset() {
        // Set up some mocks
        engineMock.mockIntakeHandoffAccepted();
        engineMock.mockProcessNodeSuccess();

        // Reset should not throw
        assertDoesNotThrow(() -> engineMock.reset());
    }

    // ============================================
    // End-to-End Workflow Tests
    // ============================================

    @Test
    @DisplayName("Should support complete intake-to-processing workflow")
    void testCompleteWorkflow() {
        // 1. Set up mock for intake handoff
        String streamId = "workflow-stream-123";
        String entryNodeId = "entry-parser";
        engineMock.mockIntakeHandoffAccepted(streamId, entryNodeId);

        // 2. Perform intake handoff
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("workflow-doc")
                .build();

        PipeStream initialStream = PipeStream.newBuilder()
                .setStreamId("initial-stream")
                .setDocument(doc)
                .build();

        IntakeHandoffRequest intakeRequest = IntakeHandoffRequest.newBuilder()
                .setStream(initialStream)
                .setDatasourceId("workflow-datasource")
                .setAccountId("workflow-account")
                .build();

        IntakeHandoffResponse intakeResponse = stub.intakeHandoff(intakeRequest);

        assertTrue(intakeResponse.getAccepted());
        assertEquals(streamId, intakeResponse.getAssignedStreamId());

        // 3. Set up mock for node processing
        PipeStream processedStream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setCurrentNodeId("node-chunker")
                .setDocument(doc)
                .build();

        ProcessingMetrics metrics = ProcessingMetrics.newBuilder()
                .setProcessingTimeMs(250)
                .setNodeId(entryNodeId)
                .setHopCount(1)
                .build();

        engineMock.mockProcessNodeSuccessWithMetrics(processedStream, metrics);

        // 4. Perform node processing
        ProcessNodeRequest processRequest = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder()
                        .setStreamId(streamId)
                        .setCurrentNodeId(entryNodeId)
                        .setDocument(doc)
                        .build())
                .build();

        ProcessNodeResponse processResponse = stub.processNode(processRequest);

        assertTrue(processResponse.getSuccess());
        assertTrue(processResponse.hasMetrics());
        assertEquals(250, processResponse.getMetrics().getProcessingTimeMs());
    }

    @Test
    @DisplayName("Should support failure and retry workflow")
    void testFailureAndRetryWorkflow() {
        // 1. First call fails with UNAVAILABLE
        engineMock.mockProcessNodeUnavailable();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("retry-stream").build())
                .build();

        // First attempt should fail
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.processNode(request)
        );
        assertEquals(io.grpc.Status.Code.UNAVAILABLE, exception.getStatus().getCode());

        // 2. Reset and configure for success (simulating retry)
        engineMock.reset();
        engineMock.mockProcessNodeSuccess();

        // Second attempt should succeed
        ProcessNodeResponse response = stub.processNode(request);
        assertTrue(response.getSuccess());
    }

    @Test
    @DisplayName("Should support multiple concurrent stream configurations")
    void testMultipleStreamConfigurations() {
        // Configure mock for success (will be used for all streams)
        PipeStream updatedStream = PipeStream.newBuilder()
                .setStreamId("multi-stream-result")
                .setCurrentNodeId("final-node")
                .build();

        engineMock.mockProcessNodeSuccess(updatedStream);

        // Process multiple streams
        for (int i = 1; i <= 5; i++) {
            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(PipeStream.newBuilder()
                            .setStreamId("stream-" + i)
                            .setCurrentNodeId("node-" + i)
                            .build())
                    .build();

            ProcessNodeResponse response = stub.processNode(request);

            assertTrue(response.getSuccess());
            assertEquals("multi-stream-result", response.getUpdatedStream().getStreamId());
        }
    }
}
