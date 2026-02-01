package ai.pipestream.wiremock.client;

import ai.pipestream.opensearch.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EmbeddingConfigServiceMock (step 4.5).
 * Verifies that the WireMock stub returns dummy embedding config data so consumers
 * can run integration tests against the WireMock container without a real opensearch-manager.
 */
class EmbeddingConfigServiceMockTest {

    private WireMockServer wireMockServer;
    private EmbeddingConfigServiceMock embeddingConfigMock;
    private ManagedChannel channel;
    private EmbeddingConfigServiceGrpc.EmbeddingConfigServiceBlockingStub embeddingConfigStub;

    @BeforeEach
    void setUp() {
        WireMockConfiguration config = wireMockConfig()
                .dynamicPort()
                .withRootDirectory("build/resources/test/wiremock")
                .extensions(new GrpcExtensionFactory());

        wireMockServer = new WireMockServer(config);
        wireMockServer.start();

        WireMock wireMock = new WireMock(wireMockServer.port());
        embeddingConfigMock = new EmbeddingConfigServiceMock(wireMock);
        embeddingConfigMock.initializeDefaults(wireMock);

        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();

        embeddingConfigStub = EmbeddingConfigServiceGrpc.newBlockingStub(channel);
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
    void listEmbeddingModelConfigs_returnsDummyData() {
        ListEmbeddingModelConfigsRequest request = ListEmbeddingModelConfigsRequest.newBuilder()
                .setPageSize(10)
                .build();

        ListEmbeddingModelConfigsResponse response = embeddingConfigStub.listEmbeddingModelConfigs(request);

        assertNotNull(response);
        assertEquals(2, response.getConfigsCount());

        EmbeddingModelConfig c1 = response.getConfigs(0);
        assertEquals(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_ID, c1.getId());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_NAME, c1.getName());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_MODEL_IDENTIFIER, c1.getModelIdentifier());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_DIMENSIONS, c1.getDimensions());

        EmbeddingModelConfig c2 = response.getConfigs(1);
        assertEquals(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_ID_2, c2.getId());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_NAME_2, c2.getName());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_MODEL_IDENTIFIER_2, c2.getModelIdentifier());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_DIMENSIONS_2, c2.getDimensions());
    }

    @Test
    void getEmbeddingModelConfig_byId_returnsDummyData() {
        GetEmbeddingModelConfigRequest request = GetEmbeddingModelConfigRequest.newBuilder()
                .setId(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_ID)
                .build();

        GetEmbeddingModelConfigResponse response = embeddingConfigStub.getEmbeddingModelConfig(request);

        assertNotNull(response);
        assertTrue(response.hasConfig());
        EmbeddingModelConfig config = response.getConfig();
        assertEquals(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_ID, config.getId());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_NAME, config.getName());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_MODEL_IDENTIFIER, config.getModelIdentifier());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_DIMENSIONS, config.getDimensions());
    }

    @Test
    void getEmbeddingModelConfig_byName_returnsDummyData() {
        GetEmbeddingModelConfigRequest request = GetEmbeddingModelConfigRequest.newBuilder()
                .setId(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_NAME)
                .setByName(true)
                .build();

        GetEmbeddingModelConfigResponse response = embeddingConfigStub.getEmbeddingModelConfig(request);

        assertNotNull(response);
        assertTrue(response.hasConfig());
        EmbeddingModelConfig config = response.getConfig();
        assertEquals(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_ID, config.getId());
        assertEquals(EmbeddingConfigServiceMock.DUMMY_EMBEDDING_CONFIG_NAME, config.getName());
    }
}
