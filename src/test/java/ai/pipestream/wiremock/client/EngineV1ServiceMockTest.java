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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

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
        assertThat(engineMock.getServiceName())
                .as("service name should match the EngineV1Service proto definition")
                .isEqualTo("ai.pipestream.engine.v1.EngineV1Service");
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

        assertThat(response).as("intake handoff response").isNotNull();
        assertThat(response.getAccepted()).as("handoff should be accepted").isTrue();
        assertThat(response.getAssignedStreamId()).as("assigned stream ID").isEqualTo(streamId);
        assertThat(response.getEntryNodeId()).as("entry node").isEqualTo(entryNode);
        assertThat(response.getMessage()).as("acceptance message").isEqualTo("Stream accepted for processing");
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

        assertThat(response).as("intake handoff response").isNotNull();
        assertThat(response.getAccepted()).as("handoff should be rejected").isFalse();
        assertThat(response.getMessage()).as("rejection message").isEqualTo(rejectMessage);
    }

    @Test
    @DisplayName("Should receive UNAVAILABLE error via gRPC")
    void testIntakeHandoffUnavailableViaGrpc() {
        engineMock.mockIntakeHandoffUnavailable();

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("stream-1").build())
                .build();

        assertThatThrownBy(() -> stub.intakeHandoff(request))
                .as("engine unavailable should throw gRPC UNAVAILABLE")
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(thrown -> {
                    StatusRuntimeException sre = (StatusRuntimeException) thrown;
                    assertThat(sre.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.UNAVAILABLE);
                    assertThat(sre.getMessage()).contains("temporarily unavailable");
                });
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

        assertThat(response).as("queue full response").isNotNull();
        assertThat(response.getAccepted()).as("handoff should be rejected when queue full").isFalse();
        assertThat(response.getMessage()).as("rejection reason").contains("Queue full");
        assertThat(response.getQueueDepth()).as("reported queue depth").isEqualTo(queueDepth);
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
    @DisplayName("Should mock ProcessNode success with completed_at timestamp")
    void testMockProcessNodeSuccessWithTimestamp() {
        assertDoesNotThrow(() -> engineMock.mockProcessNodeSuccessWithTimestamp());
    }

    @Test
    @DisplayName("Should mock ProcessNode success with output document (test mode)")
    void testMockProcessNodeSuccessWithOutputDoc() {
        PipeDoc outputDoc = PipeDoc.newBuilder()
                .setDocId("processed-doc-123")
                .build();

        assertDoesNotThrow(() -> engineMock.mockProcessNodeSuccessWithOutputDoc(outputDoc));
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

        assertThat(response).as("processNode response").isNotNull();
        assertThat(response.getSuccess()).as("processNode should succeed").isTrue();
        assertThat(response.getMessage()).as("success message").isEqualTo("Node processed successfully");
    }

    @Test
    @DisplayName("Should receive ProcessNode success with completed_at via gRPC")
    void testProcessNodeSuccessWithTimestampViaGrpc() {
        engineMock.mockProcessNodeSuccessWithTimestamp();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("original-stream").build())
                .build();

        ProcessNodeResponse response = stub.processNode(request);

        assertThat(response).as("processNode response").isNotNull();
        assertThat(response.getSuccess()).as("processNode should succeed").isTrue();
        assertThat(response.hasCompletedAt()).as("response should include completed_at timestamp").isTrue();
        assertThat(response.getCompletedAt().getSeconds()).as("completed_at should be a valid epoch time").isPositive();
    }

    @Test
    @DisplayName("Should receive ProcessNode success with output document via gRPC (test mode)")
    void testProcessNodeSuccessWithOutputDocViaGrpc() {
        PipeDoc outputDoc = PipeDoc.newBuilder()
                .setDocId("processed-doc-after-module")
                .build();

        engineMock.mockProcessNodeSuccessWithOutputDoc(outputDoc);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("test-stream").build())
                .build();

        ProcessNodeResponse response = stub.processNode(request);

        assertThat(response).as("processNode response").isNotNull();
        assertThat(response.getSuccess()).as("processNode should succeed").isTrue();
        assertThat(response.hasCompletedAt()).as("response should include completed_at").isTrue();
        assertThat(response.hasOutputDoc()).as("test mode response should include output document").isTrue();
        assertThat(response.getOutputDoc().getDocId()).as("output doc ID should match").isEqualTo("processed-doc-after-module");
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

        assertThat(response).as("processNode failure response").isNotNull();
        assertThat(response.getSuccess()).as("processNode should report failure").isFalse();
        assertThat(response.getMessage()).as("error message should describe the failure").isEqualTo(errorMessage);
    }

    @Test
    @DisplayName("Should receive ProcessNode UNAVAILABLE error via gRPC")
    void testProcessNodeUnavailableViaGrpc() {
        engineMock.mockProcessNodeUnavailable();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("stream-1").build())
                .build();

        assertThatThrownBy(() -> stub.processNode(request))
                .as("processNode should throw UNAVAILABLE")
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(thrown -> assertThat(((StatusRuntimeException) thrown).getStatus().getCode())
                        .isEqualTo(io.grpc.Status.Code.UNAVAILABLE));
    }

    @Test
    @DisplayName("Should receive ProcessNode INTERNAL error via gRPC")
    void testProcessNodeInternalErrorViaGrpc() {
        String errorMessage = "Database connection lost";
        engineMock.mockProcessNodeInternalError(errorMessage);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("stream-1").build())
                .build();

        assertThatThrownBy(() -> stub.processNode(request))
                .as("processNode should throw INTERNAL error")
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(thrown -> {
                    StatusRuntimeException sre = (StatusRuntimeException) thrown;
                    assertThat(sre.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.INTERNAL);
                    assertThat(sre.getMessage()).contains(errorMessage);
                });
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

        assertThat(response).as("health response").isNotNull();
        assertThat(response.getHealth()).as("engine should report healthy").isEqualTo(EngineHealth.ENGINE_HEALTH_HEALTHY);
    }

    @Test
    @DisplayName("Should receive GetHealth unhealthy via gRPC")
    void testGetHealthUnhealthyViaGrpc() {
        engineMock.mockGetHealthUnhealthy("Service overloaded");

        GetHealthRequest request = GetHealthRequest.newBuilder().build();

        GetHealthResponse response = stub.getHealth(request);

        assertThat(response).as("health response").isNotNull();
        assertThat(response.getHealth()).as("engine should report unhealthy").isEqualTo(EngineHealth.ENGINE_HEALTH_UNHEALTHY);
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

        assertThat(intakeResponse.getAccepted()).as("intake handoff should be accepted").isTrue();
        assertThat(intakeResponse.getAssignedStreamId()).as("assigned stream ID").isEqualTo(streamId);

        // 3. Set up mock for node processing
        PipeStream processedStream = PipeStream.newBuilder()
                .setStreamId(streamId)
                .setCurrentNodeId("node-chunker")
                .setDocument(doc)
                .build();

        engineMock.mockProcessNodeSuccessWithTimestamp();

        // 4. Perform node processing
        ProcessNodeRequest processRequest = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder()
                        .setStreamId(streamId)
                        .setCurrentNodeId(entryNodeId)
                        .setDocument(doc)
                        .build())
                .build();

        ProcessNodeResponse processResponse = stub.processNode(processRequest);

        assertThat(processResponse.getSuccess()).as("node processing should succeed").isTrue();
        assertThat(processResponse.hasCompletedAt()).as("response should include completed_at").isTrue();
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
        assertThatThrownBy(() -> stub.processNode(request))
                .as("first attempt should fail with UNAVAILABLE")
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(thrown -> assertThat(((StatusRuntimeException) thrown).getStatus().getCode())
                        .isEqualTo(io.grpc.Status.Code.UNAVAILABLE));

        // 2. Reset and configure for success (simulating retry)
        engineMock.reset();
        engineMock.mockProcessNodeSuccess();

        // Second attempt should succeed
        ProcessNodeResponse response = stub.processNode(request);
        assertThat(response.getSuccess()).as("retry attempt should succeed").isTrue();
    }

    @Test
    @DisplayName("Should support multiple concurrent stream configurations")
    void testMultipleStreamConfigurations() {
        engineMock.mockProcessNodeSuccessWithTimestamp();

        // Process multiple streams
        for (int i = 1; i <= 5; i++) {
            ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                    .setStream(PipeStream.newBuilder()
                            .setStreamId("stream-" + i)
                            .setCurrentNodeId("node-" + i)
                            .build())
                    .build();

            ProcessNodeResponse response = stub.processNode(request);

            assertThat(response.getSuccess())
                    .as("stream-%d should succeed", i)
                    .isTrue();
        }
    }
}
