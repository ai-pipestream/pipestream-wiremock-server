package ai.pipestream.wiremock.client;

import ai.pipestream.connector.intake.v1.*;
import ai.pipestream.config.v1.*;
import ai.pipestream.engine.v1.*;
import ai.pipestream.validation.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.List;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Showcase tests demonstrating how to use the strengthened mocks.
 * These serve as documentation and verification of the new mock capabilities.
 */
public class MocksShowcaseTest {

    private WireMockServer wireMockServer;
    private ManagedChannel channel;
    
    // Mocks under test
    private ConnectorIntakeServiceMock intakeMock;
    private PipelineGraphServiceMock graphMock;
    private PipelineConfigServiceMock configMock;
    private ValidationServiceMock validationMock;

    // gRPC Stubs
    private ConnectorIntakeServiceGrpc.ConnectorIntakeServiceBlockingStub intakeStub;
    private PipelineGraphServiceGrpc.PipelineGraphServiceBlockingStub graphStub;
    private PipelineConfigServiceGrpc.PipelineConfigServiceBlockingStub configStub;
    private ValidationServiceGrpc.ValidationServiceBlockingStub validationStub;

    @BeforeEach
    void setUp() {
        WireMockConfiguration config = wireMockConfig()
                .dynamicPort()
                .withRootDirectory("build/resources/test/wiremock")
                .extensions(new GrpcExtensionFactory());

        wireMockServer = new WireMockServer(config);
        wireMockServer.start();

        WireMock wireMock = new WireMock(wireMockServer.port());
        
        // Initialize mocks
        intakeMock = new ConnectorIntakeServiceMock(wireMock);
        graphMock = new PipelineGraphServiceMock(wireMock);
        configMock = new PipelineConfigServiceMock(wireMock);
        validationMock = new ValidationServiceMock(wireMock);

        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();

        // Initialize stubs
        intakeStub = ConnectorIntakeServiceGrpc.newBlockingStub(channel);
        graphStub = PipelineGraphServiceGrpc.newBlockingStub(channel);
        configStub = PipelineConfigServiceGrpc.newBlockingStub(channel);
        validationStub = ValidationServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.shutdown();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    @DisplayName("ConnectorIntakeService: Test deterministic doc_id derivation")
    void testIntakeDeterministicDocId() {
        String dsId = "ds-123";
        String sourceId = "file-abc";
        
        // Reset to clear the default catch-all UploadPipeDoc stub
        intakeMock.reset();
        
        // Configure mock for deterministic derivation
        intakeMock.mockUploadPipeDocDeterministic(dsId, sourceId);

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
                .setDatasourceId(dsId)
                .setApiKey("key-123")
                .setSourceDocId(sourceId)
                .setPipeDoc(ai.pipestream.data.v1.PipeDoc.newBuilder().build())
                .build();

        UploadPipeDocResponse response = intakeStub.uploadPipeDoc(request);

        assertTrue(response.getSuccess());
        assertEquals("ds-123:file-abc", response.getDocId());
    }

    @Test
    @DisplayName("PipelineGraphService: Test graph activation")
    void testGraphActivation() {
        String graphId = "my-graph";
        long version = 5;
        
        graphMock.mockActivateGraphSuccess(graphId, version);

        ActivateGraphResponse response = graphStub.activateGraph(ActivateGraphRequest.newBuilder()
                .setGraphId(graphId)
                .setVersion(version)
                .build());

        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("activated"));
    }

    @Test
    @DisplayName("PipelineConfigService: Test listing clusters")
    void testConfigListClusters() {
        Cluster c1 = Cluster.newBuilder().setClusterId("c1").setName("Cluster 1").build();
        Cluster c2 = Cluster.newBuilder().setClusterId("c2").setName("Cluster 2").build();
        
        configMock.mockListClusters(List.of(c1, c2));

        ListClustersResponse response = configStub.listClusters(ListClustersRequest.newBuilder().build());

        assertEquals(2, response.getClustersCount());
        assertEquals("Cluster 1", response.getClusters(0).getName());
    }

    @Test
    @DisplayName("ValidationService: Test validation failure")
    void testValidationFailure() {
        validationMock.mockValidateNodeFailure("Invalid node type", "ERR_NODE_TYPE");

        ValidateNodeResponse response = validationStub.validateNode(ValidateNodeRequest.newBuilder().build());

        assertFalse(response.getIsValid());
        assertEquals(1, response.getErrorsCount());
        assertEquals("ERR_NODE_TYPE", response.getErrors(0).getErrorCode());
    }
}
