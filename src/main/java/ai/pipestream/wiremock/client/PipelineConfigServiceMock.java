package ai.pipestream.wiremock.client;

import ai.pipestream.config.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.util.List;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring PipelineConfigService mocks in WireMock.
 */
public class PipelineConfigServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigServiceMock.class);

    private static final String SERVICE_NAME = PipelineConfigServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService configService;

    public PipelineConfigServiceMock(WireMock wireMock) {
        this.configService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    public PipelineConfigServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.configService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default PipelineConfigService stubs");

        // Default cluster
        mockGetCluster("default-cluster");
        
        // Success for common creation operations
        mockCreateClusterSuccess("test-cluster");
        mockCreatePipelineGraphSuccess("test-cluster", "test-graph");
    }

    public void mockGetCluster(String clusterName) {
        Cluster cluster = Cluster.newBuilder()
                .setClusterId("id-" + clusterName)
                .setName(clusterName)
                .setMetadata(ClusterMetadata.newBuilder()
                        .setName(clusterName + "-metadata")
                        .build())
                .build();

        GetClusterResponse response = GetClusterResponse.newBuilder()
                .setCluster(cluster)
                .build();

        configService.stubFor(
                method("GetCluster")
                        .willReturn(message(response))
        );
    }

    public void mockCreateClusterSuccess(String clusterName) {
        Cluster cluster = Cluster.newBuilder()
                .setClusterId("id-" + clusterName)
                .setName(clusterName)
                .build();

        CreateClusterResponse response = CreateClusterResponse.newBuilder()
                .setSuccess(true)
                .setCluster(cluster)
                .setMessage("Cluster created successfully")
                .build();

        configService.stubFor(
                method("CreateCluster")
                        .willReturn(message(response))
        );
    }

    public void mockCreatePipelineGraphSuccess(String clusterId, String graphName) {
        PipelineGraph graph = PipelineGraph.newBuilder()
                .setClusterId(clusterId)
                .setName(graphName)
                .build();

        CreatePipelineGraphResponse response = CreatePipelineGraphResponse.newBuilder()
                .setSuccess(true)
                .setPipelineGraph(graph)
                .setMessage("Pipeline graph created successfully")
                .build();

        configService.stubFor(
                method("CreatePipelineGraph")
                        .willReturn(message(response))
        );
    }

    public void mockListClusters(List<Cluster> clusters) {
        ListClustersResponse response = ListClustersResponse.newBuilder()
                .addAllClusters(clusters)
                .build();

        configService.stubFor(
                method("ListClusters")
                        .willReturn(message(response))
        );
    }

    public void reset() {
        configService.resetAll();
    }
}
