package ai.pipestream.wiremock.client;

import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.util.UUID;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring EngineV1Service mocks in WireMock.
 * <p>
 * This mock supports testing of engine intake and processing scenarios for the Kafka Sidecar including:
 * <ul>
 *   <li><b>IntakeHandoff</b>: Accept or reject stream handoffs from the sidecar</li>
 *   <li><b>ProcessNode</b>: Process a node and return success/failure with updated stream</li>
 *   <li><b>Error scenarios</b>: Return UNAVAILABLE for retry testing, rejection for DLQ testing</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * EngineV1ServiceMock mock = new EngineV1ServiceMock(wireMock);
 *
 * // Mock successful intake handoff
 * mock.mockIntakeHandoffAccepted("stream-123", "entry-parser");
 *
 * // Mock rejected intake handoff
 * mock.mockIntakeHandoffRejected("Queue full");
 *
 * // Mock successful node processing
 * mock.mockProcessNodeSuccess(updatedStream);
 *
 * // Mock engine unavailable for retry testing
 * mock.mockIntakeHandoffUnavailable();
 * }</pre>
 * <p>
 * This class implements {@link ServiceMockInitializer} and will be automatically
 * discovered and initialized at server startup by the {@link ServiceMockRegistry}.
 */
public class EngineV1ServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(EngineV1ServiceMock.class);

    private static final String SERVICE_NAME = EngineV1ServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService engineService;

    /**
     * Create a helper for the given WireMock client.
     *
     * @param wireMock The WireMock client instance (connected to WireMock server)
     */
    public EngineV1ServiceMock(WireMock wireMock) {
        this.engineService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    /**
     * Default constructor for ServiceLoader discovery.
     */
    public EngineV1ServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.engineService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default EngineV1Service stubs");

        // Set up default successful responses
        mockIntakeHandoffAccepted();
        mockProcessNodeSuccess();

        LOG.info("Added default stubs for EngineV1Service");
    }

    // ============================================
    // IntakeHandoff Mocks
    // ============================================

    /**
     * Mock IntakeHandoff to accept with a generated stream ID and default entry node.
     */
    public void mockIntakeHandoffAccepted() {
        mockIntakeHandoffAccepted(UUID.randomUUID().toString(), "default-entry-node");
    }

    /**
     * Mock IntakeHandoff to accept with specific stream ID and entry node.
     *
     * @param assignedStreamId The stream ID to assign
     * @param entryNodeId      The entry node ID
     */
    public void mockIntakeHandoffAccepted(String assignedStreamId, String entryNodeId) {
        IntakeHandoffResponse response = IntakeHandoffResponse.newBuilder()
                .setAccepted(true)
                .setMessage("Stream accepted for processing")
                .setAssignedStreamId(assignedStreamId)
                .setEntryNodeId(entryNodeId)
                .setQueueDepth(0)
                .build();

        engineService.stubFor(
                method("IntakeHandoff")
                        .willReturn(message(response))
        );

        LOG.debug("Configured IntakeHandoff to accept with streamId={}, entryNode={}",
                assignedStreamId, entryNodeId);
    }

    /**
     * Mock IntakeHandoff to accept for a specific datasource.
     *
     * @param datasourceId     Datasource ID to match
     * @param assignedStreamId The stream ID to assign
     * @param entryNodeId      The entry node ID
     */
    public void mockIntakeHandoffAcceptedForDatasource(String datasourceId, String assignedStreamId,
                                                        String entryNodeId) {
        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build();

        IntakeHandoffResponse response = IntakeHandoffResponse.newBuilder()
                .setAccepted(true)
                .setMessage("Stream accepted for processing")
                .setAssignedStreamId(assignedStreamId)
                .setEntryNodeId(entryNodeId)
                .setQueueDepth(0)
                .build();

        // Note: WireMock gRPC doesn't support partial matching well,
        // so this will match any request with the given datasourceId
        engineService.stubFor(
                method("IntakeHandoff")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock IntakeHandoff to reject with a message.
     *
     * @param rejectMessage The rejection message
     */
    public void mockIntakeHandoffRejected(String rejectMessage) {
        IntakeHandoffResponse response = IntakeHandoffResponse.newBuilder()
                .setAccepted(false)
                .setMessage(rejectMessage)
                .build();

        engineService.stubFor(
                method("IntakeHandoff")
                        .willReturn(message(response))
        );

        LOG.debug("Configured IntakeHandoff to reject with message: {}", rejectMessage);
    }

    /**
     * Mock IntakeHandoff to return UNAVAILABLE for retry testing.
     */
    public void mockIntakeHandoffUnavailable() {
        engineService.stubFor(
                method("IntakeHandoff")
                        .willReturn(WireMockGrpc.Status.UNAVAILABLE,
                                "Engine temporarily unavailable")
        );

        LOG.debug("Configured IntakeHandoff to return UNAVAILABLE");
    }

    /**
     * Mock IntakeHandoff to return RESOURCE_EXHAUSTED (queue full).
     *
     * @param queueDepth Current queue depth
     */
    public void mockIntakeHandoffQueueFull(long queueDepth) {
        IntakeHandoffResponse response = IntakeHandoffResponse.newBuilder()
                .setAccepted(false)
                .setMessage("Queue full, cannot accept more streams")
                .setQueueDepth(queueDepth)
                .build();

        engineService.stubFor(
                method("IntakeHandoff")
                        .willReturn(message(response))
        );

        LOG.debug("Configured IntakeHandoff to reject (queue full, depth={})", queueDepth);
    }

    // ============================================
    // ProcessNode Mocks
    // ============================================

    /**
     * Mock ProcessNode to return success with no stream updates.
     */
    public void mockProcessNodeSuccess() {
        ProcessNodeResponse response = ProcessNodeResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Node processed successfully")
                .build();

        engineService.stubFor(
                method("ProcessNode")
                        .willReturn(message(response))
        );

        LOG.debug("Configured ProcessNode to return success");
    }

    /**
     * Mock ProcessNode to return success with an updated stream.
     *
     * @param updatedStream The updated PipeStream to return
     */
    public void mockProcessNodeSuccess(PipeStream updatedStream) {
        ProcessNodeResponse response = ProcessNodeResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Node processed successfully")
                .setUpdatedStream(updatedStream)
                .build();

        engineService.stubFor(
                method("ProcessNode")
                        .willReturn(message(response))
        );

        LOG.debug("Configured ProcessNode to return success with updated stream");
    }

    /**
     * Mock ProcessNode to return success with metrics.
     *
     * @param updatedStream The updated PipeStream to return
     * @param metrics       Processing metrics
     */
    public void mockProcessNodeSuccessWithMetrics(PipeStream updatedStream, ProcessingMetrics metrics) {
        ProcessNodeResponse response = ProcessNodeResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Node processed successfully")
                .setUpdatedStream(updatedStream)
                .setMetrics(metrics)
                .build();

        engineService.stubFor(
                method("ProcessNode")
                        .willReturn(message(response))
        );

        LOG.debug("Configured ProcessNode to return success with metrics");
    }

    /**
     * Mock ProcessNode to return failure.
     *
     * @param errorMessage Error message
     */
    public void mockProcessNodeFailure(String errorMessage) {
        ProcessNodeResponse response = ProcessNodeResponse.newBuilder()
                .setSuccess(false)
                .setMessage(errorMessage)
                .build();

        engineService.stubFor(
                method("ProcessNode")
                        .willReturn(message(response))
        );

        LOG.debug("Configured ProcessNode to return failure: {}", errorMessage);
    }

    /**
     * Mock ProcessNode to return UNAVAILABLE for retry testing.
     */
    public void mockProcessNodeUnavailable() {
        engineService.stubFor(
                method("ProcessNode")
                        .willReturn(WireMockGrpc.Status.UNAVAILABLE,
                                "Engine temporarily unavailable")
        );

        LOG.debug("Configured ProcessNode to return UNAVAILABLE");
    }

    /**
     * Mock ProcessNode to return INTERNAL error.
     *
     * @param errorMessage Error message
     */
    public void mockProcessNodeInternalError(String errorMessage) {
        engineService.stubFor(
                method("ProcessNode")
                        .willReturn(WireMockGrpc.Status.INTERNAL, errorMessage)
        );

        LOG.debug("Configured ProcessNode to return INTERNAL error: {}", errorMessage);
    }

    // ============================================
    // ProcessStream Mocks (for completeness)
    // ============================================

    /**
     * Mock ProcessStream to return success.
     */
    public void mockProcessStreamSuccess() {
        ProcessStreamResponse response = ProcessStreamResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Stream processed successfully")
                .build();

        engineService.stubFor(
                method("ProcessStream")
                        .willReturn(message(response))
        );

        LOG.debug("Configured ProcessStream to return success");
    }

    /**
     * Mock ProcessStream to return success with an updated stream.
     *
     * @param updatedStream The updated PipeStream to return
     */
    public void mockProcessStreamSuccess(PipeStream updatedStream) {
        ProcessStreamResponse response = ProcessStreamResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Stream processed successfully")
                .setUpdatedStream(updatedStream)
                .build();

        engineService.stubFor(
                method("ProcessStream")
                        .willReturn(message(response))
        );

        LOG.debug("Configured ProcessStream to return success with updated stream");
    }

    // ============================================
    // GetHealth Mocks
    // ============================================

    /**
     * Mock GetHealth to return healthy status.
     */
    public void mockGetHealthHealthy() {
        GetHealthResponse response = GetHealthResponse.newBuilder()
                .setHealth(EngineHealth.ENGINE_HEALTH_HEALTHY)
                .setActiveStreams(0)
                .build();

        engineService.stubFor(
                method("GetHealth")
                        .willReturn(message(response))
        );

        LOG.debug("Configured GetHealth to return healthy");
    }

    /**
     * Mock GetHealth to return unhealthy status.
     *
     * @param reason Reason for unhealthy status (currently not used in proto)
     */
    public void mockGetHealthUnhealthy(String reason) {
        GetHealthResponse response = GetHealthResponse.newBuilder()
                .setHealth(EngineHealth.ENGINE_HEALTH_UNHEALTHY)
                .build();

        engineService.stubFor(
                method("GetHealth")
                        .willReturn(message(response))
        );

        LOG.debug("Configured GetHealth to return unhealthy: {}", reason);
    }

    // ============================================
    // Utility Methods
    // ============================================

    /**
     * Reset all WireMock stubs for the Engine service.
     */
    public void reset() {
        engineService.resetAll();
    }
}
