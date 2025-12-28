package ai.pipestream.wiremock.client;

import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.v1.ProcessingMetrics;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EngineV1ServiceMock.
 */
class EngineV1ServiceMockTest {

    private static WireMockServer wireMockServer;
    private static WireMock wireMock;
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
    }

    @AfterAll
    static void tearDown() {
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
    // IntakeHandoff Tests
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
    // ProcessNode Tests
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
    // ProcessStream Tests
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
}
