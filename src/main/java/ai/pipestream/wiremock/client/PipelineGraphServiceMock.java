package ai.pipestream.wiremock.client;

import ai.pipestream.config.v1.PipelineGraph;
import ai.pipestream.engine.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.util.List;
import java.util.UUID;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring PipelineGraphService mocks in WireMock.
 */
public class PipelineGraphServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineGraphServiceMock.class);

    private static final String SERVICE_NAME = PipelineGraphServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService graphService;

    public PipelineGraphServiceMock(WireMock wireMock) {
        this.graphService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    public PipelineGraphServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.graphService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default PipelineGraphService stubs");

        // Default successful activation mock
        mockActivateGraphSuccess("default-graph", 1);
    }

    public void mockCreateAndActivateGraphSuccess(PipelineGraph graph) {
        CreateAndActivateGraphResponse response = CreateAndActivateGraphResponse.newBuilder()
                .setGraph(graph)
                .setId(UUID.randomUUID().toString())
                .setIsActive(true)
                .build();

        graphService.stubFor(
                method("CreateAndActivateGraph")
                        .willReturn(message(response))
        );
    }

    public void mockGetActiveGraph(String graphId, String clusterId, PipelineGraph graph) {
        GetActiveGraphResponse response = GetActiveGraphResponse.newBuilder()
                .setFound(true)
                .setGraph(graph)
                .setId(UUID.randomUUID().toString())
                .setIsActive(true)
                .build();

        GetActiveGraphRequest request = GetActiveGraphRequest.newBuilder()
                .setGraphId(graphId)
                .setClusterId(clusterId)
                .build();

        graphService.stubFor(
                method("GetActiveGraph")
                        .withRequestMessage(org.wiremock.grpc.dsl.WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    public void mockActivateGraphSuccess(String graphId, long version) {
        ActivateGraphResponse response = ActivateGraphResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Graph activated: " + graphId + " v" + version)
                .build();

        graphService.stubFor(
                method("ActivateGraph")
                        .willReturn(message(response))
        );
    }

    public void mockActivateGraphFailure(String graphId, long version, String reason) {
        ActivateGraphResponse response = ActivateGraphResponse.newBuilder()
                .setSuccess(false)
                .setMessage(reason)
                .build();

        graphService.stubFor(
                method("ActivateGraph")
                        .willReturn(message(response))
        );
    }

    public void mockListGraphVersions(String graphId, List<GraphVersionInfo> versions) {
        ListGraphVersionsResponse response = ListGraphVersionsResponse.newBuilder()
                .addAllVersions(versions)
                .setTotalCount(versions.size())
                .build();

        graphService.stubFor(
                method("ListGraphVersions")
                        .willReturn(message(response))
        );
    }

    public void reset() {
        graphService.resetAll();
    }
}
