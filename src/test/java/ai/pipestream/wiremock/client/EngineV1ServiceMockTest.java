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

    // ============================================
    // End-to-End Workflow Tests
    // ============================================

}
