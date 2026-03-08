package ai.pipestream.wiremock.server;

import ai.pipestream.config.v1.*;
import ai.pipestream.opensearch.v1.*;
import ai.pipestream.connector.intake.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * High-fidelity integration test to verify that declarative JSON stubs
 * are correctly loaded and mapped for all platform services.
 */
class PlatformServicesMockTest {

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer directGrpcServer;
    private ManagedChannel declarativeChannel;
    private ManagedChannel directChannel;

    @BeforeEach
    void setUp() throws Exception {
        // Point to the build directory where descriptors are copied
        wireMockServer = new WireMockServer(wireMockConfig()
                .dynamicPort()
                .withRootDirectory("build/resources/test/wiremock")
                .extensions(new GrpcExtensionFactory()));
        wireMockServer.start();

        directGrpcServer = new DirectWireMockGrpcServer(wireMockServer, 0);
        directGrpcServer.start();

        declarativeChannel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
        
        directChannel = ManagedChannelBuilder.forAddress("localhost", directGrpcServer.getGrpcPort())
                .usePlaintext()
                .build();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (declarativeChannel != null) declarativeChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        if (directChannel != null) directChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        if (directGrpcServer != null) directGrpcServer.stop();
        if (wireMockServer != null) wireMockServer.stop();
    }

    @Test
    void testPipelineConfig_GetConfig() {
        PipelineConfigServiceGrpc.PipelineConfigServiceBlockingStub stub = PipelineConfigServiceGrpc.newBlockingStub(declarativeChannel);
        GetPipelineConfigRequest request = GetPipelineConfigRequest.newBuilder()
                .setClusterName("default")
                .setGraphName("test")
                .build();
        GetPipelineConfigResponse response = stub.getPipelineConfig(request);

        assertNotNull(response.getGraph());
        assertEquals("test-pipeline", response.getGraph().getGraphId());
        assertEquals(2, response.getGraph().getNodesCount());
    }

    @Test
    void testVectorSet_Get() {
        VectorSetServiceGrpc.VectorSetServiceBlockingStub stub = VectorSetServiceGrpc.newBlockingStub(declarativeChannel);
        GetVectorSetRequest request = GetVectorSetRequest.newBuilder().setId("vs-1").build();
        GetVectorSetResponse response = stub.getVectorSet(request);

        assertNotNull(response.getVectorSet());
        assertEquals("vs-1", response.getVectorSet().getId());
        assertEquals(1024, response.getVectorSet().getVectorDimensions());
    }

    @Test
    void testChunkerConfig_Get() {
        ChunkerConfigServiceGrpc.ChunkerConfigServiceBlockingStub stub = ChunkerConfigServiceGrpc.newBlockingStub(declarativeChannel);
        GetChunkerConfigRequest request = GetChunkerConfigRequest.newBuilder().setId("chunk-1").build();
        GetChunkerConfigResponse response = stub.getChunkerConfig(request);

        assertNotNull(response.getConfig());
        assertEquals("chunk-1", response.getConfig().getId());
        // Chunker uses config_json field for its parameters
        assertTrue(response.getConfig().hasConfigJson());
    }

    @Test
    void testEmbeddingConfig_Get() {
        EmbeddingConfigServiceGrpc.EmbeddingConfigServiceBlockingStub stub = EmbeddingConfigServiceGrpc.newBlockingStub(declarativeChannel);
        GetEmbeddingModelConfigRequest request = GetEmbeddingModelConfigRequest.newBuilder().setId("embed-1").build();
        GetEmbeddingModelConfigResponse response = stub.getEmbeddingModelConfig(request);

        assertNotNull(response.getConfig());
        assertEquals("embed-1", response.getConfig().getId());
        assertEquals(1024, response.getConfig().getDimensions());
    }

    @Test
    void testDataSourceAdmin_Get() {
        DataSourceAdminServiceGrpc.DataSourceAdminServiceBlockingStub stub = DataSourceAdminServiceGrpc.newBlockingStub(declarativeChannel);
        GetDataSourceRequest request = GetDataSourceRequest.newBuilder().setDatasourceId("ds-1").build();
        GetDataSourceResponse response = stub.getDataSource(request);

        assertNotNull(response.getDatasource());
        assertEquals("ds-1", response.getDatasource().getDatasourceId());
        assertTrue(response.getDatasource().getActive());
    }

    @Test
    void testOpenSearchManager_IndexDocument_Success() {
        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceBlockingStub stub = OpenSearchManagerServiceGrpc.newBlockingStub(directChannel);
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndexName("normal-index")
                .setDocumentId("normal-doc")
                .setDocument(OpenSearchDocument.newBuilder().setTitle("Valid").build())
                .build();
        
        IndexDocumentResponse response = stub.indexDocument(request);
        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("WireMock"));
    }

    @Test
    void testOpenSearchManager_IndexDocument_ForcedError() {
        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceBlockingStub stub = OpenSearchManagerServiceGrpc.newBlockingStub(directChannel);
        
        // This request triggers the error matching logic in DirectWireMockGrpcServer.java
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndexName("fail-this-index")
                .setDocumentId("fail-this-doc")
                .setDocument(OpenSearchDocument.newBuilder().setTitle("This will fail").build())
                .build();
        
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            stub.indexDocument(request);
        });

        assertEquals(Status.Code.INTERNAL, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("Forced internal error"));
    }
}
