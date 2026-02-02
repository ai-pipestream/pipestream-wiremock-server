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
 * Tests for ChunkerConfigServiceMock (step 4.5).
 * Verifies that the WireMock stub returns dummy chunker config data so consumers
 * can run integration tests against the WireMock container without a real opensearch-manager.
 */
class ChunkerConfigServiceMockTest {

    private WireMockServer wireMockServer;
    private ChunkerConfigServiceMock chunkerConfigMock;
    private ManagedChannel channel;
    private ChunkerConfigServiceGrpc.ChunkerConfigServiceBlockingStub chunkerConfigStub;

    @BeforeEach
    void setUp() {
        WireMockConfiguration config = wireMockConfig()
                .dynamicPort()
                .withRootDirectory("build/resources/test/wiremock")
                .extensions(new GrpcExtensionFactory());

        wireMockServer = new WireMockServer(config);
        wireMockServer.start();

        WireMock wireMock = new WireMock(wireMockServer.port());
        chunkerConfigMock = new ChunkerConfigServiceMock(wireMock);
        chunkerConfigMock.initializeDefaults(wireMock);

        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();

        chunkerConfigStub = ChunkerConfigServiceGrpc.newBlockingStub(channel);
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
    void listChunkerConfigs_returnsDummyData() {
        ListChunkerConfigsRequest request = ListChunkerConfigsRequest.newBuilder()
                .setPageSize(10)
                .build();

        ListChunkerConfigsResponse response = chunkerConfigStub.listChunkerConfigs(request);

        assertNotNull(response);
        assertEquals(2, response.getConfigsCount());

        ChunkerConfig c1 = response.getConfigs(0);
        assertEquals(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_ID, c1.getId());
        assertEquals(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_NAME, c1.getName());
        assertEquals(ChunkerConfigServiceMock.DUMMY_CONFIG_ID, c1.getConfigId());
        assertTrue(c1.hasConfigJson());

        ChunkerConfig c2 = response.getConfigs(1);
        assertEquals(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_ID_2, c2.getId());
        assertEquals(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_NAME_2, c2.getName());
        assertEquals(ChunkerConfigServiceMock.DUMMY_CONFIG_ID_2, c2.getConfigId());
        assertTrue(c2.hasConfigJson());
    }

    @Test
    void getChunkerConfig_byId_returnsDummyData() {
        GetChunkerConfigRequest request = GetChunkerConfigRequest.newBuilder()
                .setId(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_ID)
                .build();

        GetChunkerConfigResponse response = chunkerConfigStub.getChunkerConfig(request);

        assertNotNull(response);
        assertTrue(response.hasConfig());
        ChunkerConfig config = response.getConfig();
        assertEquals(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_ID, config.getId());
        assertEquals(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_NAME, config.getName());
        assertEquals(ChunkerConfigServiceMock.DUMMY_CONFIG_ID, config.getConfigId());
        assertTrue(config.hasConfigJson());
    }

    @Test
    void getChunkerConfig_byName_returnsDummyData() {
        GetChunkerConfigRequest request = GetChunkerConfigRequest.newBuilder()
                .setId(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_NAME)
                .setByName(true)
                .build();

        GetChunkerConfigResponse response = chunkerConfigStub.getChunkerConfig(request);

        assertNotNull(response);
        assertTrue(response.hasConfig());
        ChunkerConfig config = response.getConfig();
        assertEquals(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_ID, config.getId());
        assertEquals(ChunkerConfigServiceMock.DUMMY_CHUNKER_CONFIG_NAME, config.getName());
    }
}
