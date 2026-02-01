package ai.pipestream.wiremock.client;

import ai.pipestream.opensearch.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.time.Instant;
import java.util.UUID;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * WireMock stub helper for EmbeddingConfigService (opensearch-manager).
 * <p>
 * Provides dummy data for integration tests: predefined embedding model configs
 * and index-embedding bindings so consumers (e.g. module-opensearch-sink) can
 * run against the WireMock container without a real opensearch-manager.
 * <p>
 * Implements {@link ServiceMockInitializer}; registered in META-INF/services
 * for automatic discovery at server startup.
 */
public class EmbeddingConfigServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddingConfigServiceMock.class);

    private static final String SERVICE_NAME = EmbeddingConfigServiceGrpc.SERVICE_NAME;

    /** Dummy embedding config ID returned by List and Get. */
    public static final String DUMMY_EMBEDDING_CONFIG_ID = "wiremock-embedding-config-001";
    /** Dummy embedding config name. */
    public static final String DUMMY_EMBEDDING_CONFIG_NAME = "wiremock-minilm-v2";
    /** Dummy model identifier. */
    public static final String DUMMY_MODEL_IDENTIFIER = "sentence-transformers/all-MiniLM-L6-v2";
    /** Dummy dimensions. */
    public static final int DUMMY_DIMENSIONS = 384;

    /** Second dummy config ID. */
    public static final String DUMMY_EMBEDDING_CONFIG_ID_2 = "wiremock-embedding-config-002";
    public static final String DUMMY_EMBEDDING_CONFIG_NAME_2 = "wiremock-embedder-768";
    public static final String DUMMY_MODEL_IDENTIFIER_2 = "sentence-transformers/all-mpnet-base-v2";
    public static final int DUMMY_DIMENSIONS_2 = 768;

    private WireMockGrpcService embeddingConfigService;

    public EmbeddingConfigServiceMock(WireMock wireMock) {
        this.embeddingConfigService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    public EmbeddingConfigServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.embeddingConfigService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default EmbeddingConfigService stubs (dummy data for step 4.5)");

        Timestamp now = Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(0)
                .build();

        // Stub ListEmbeddingModelConfigs: return 2 dummy configs
        EmbeddingModelConfig config1 = EmbeddingModelConfig.newBuilder()
                .setId(DUMMY_EMBEDDING_CONFIG_ID)
                .setName(DUMMY_EMBEDDING_CONFIG_NAME)
                .setModelIdentifier(DUMMY_MODEL_IDENTIFIER)
                .setDimensions(DUMMY_DIMENSIONS)
                .setCreatedAt(now)
                .setUpdatedAt(now)
                .build();
        EmbeddingModelConfig config2 = EmbeddingModelConfig.newBuilder()
                .setId(DUMMY_EMBEDDING_CONFIG_ID_2)
                .setName(DUMMY_EMBEDDING_CONFIG_NAME_2)
                .setModelIdentifier(DUMMY_MODEL_IDENTIFIER_2)
                .setDimensions(DUMMY_DIMENSIONS_2)
                .setCreatedAt(now)
                .setUpdatedAt(now)
                .build();

        ListEmbeddingModelConfigsResponse listResponse = ListEmbeddingModelConfigsResponse.newBuilder()
                .addConfigs(config1)
                .addConfigs(config2)
                .build();

        // Match any ListEmbeddingModelConfigs request (page_size 10 used by test)
        ListEmbeddingModelConfigsRequest listRequest = ListEmbeddingModelConfigsRequest.newBuilder()
                .setPageSize(10)
                .build();
        embeddingConfigService.stubFor(
                method("ListEmbeddingModelConfigs")
                        .withRequestMessage(WireMockGrpc.equalToMessage(listRequest))
                        .willReturn(message(listResponse))
        );

        // Stub GetEmbeddingModelConfig by ID for dummy config 1
        GetEmbeddingModelConfigRequest getById1 = GetEmbeddingModelConfigRequest.newBuilder()
                .setId(DUMMY_EMBEDDING_CONFIG_ID)
                .build();
        GetEmbeddingModelConfigResponse getResp1 = GetEmbeddingModelConfigResponse.newBuilder()
                .setConfig(config1)
                .build();
        embeddingConfigService.stubFor(
                method("GetEmbeddingModelConfig")
                        .withRequestMessage(WireMockGrpc.equalToMessage(getById1))
                        .willReturn(message(getResp1))
        );

        // Stub GetEmbeddingModelConfig by ID for dummy config 2
        GetEmbeddingModelConfigRequest getById2 = GetEmbeddingModelConfigRequest.newBuilder()
                .setId(DUMMY_EMBEDDING_CONFIG_ID_2)
                .build();
        GetEmbeddingModelConfigResponse getResp2 = GetEmbeddingModelConfigResponse.newBuilder()
                .setConfig(config2)
                .build();
        embeddingConfigService.stubFor(
                method("GetEmbeddingModelConfig")
                        .withRequestMessage(WireMockGrpc.equalToMessage(getById2))
                        .willReturn(message(getResp2))
        );

        // Stub GetEmbeddingModelConfig by name
        GetEmbeddingModelConfigRequest getByName = GetEmbeddingModelConfigRequest.newBuilder()
                .setId(DUMMY_EMBEDDING_CONFIG_NAME)
                .setByName(true)
                .build();
        embeddingConfigService.stubFor(
                method("GetEmbeddingModelConfig")
                        .withRequestMessage(WireMockGrpc.equalToMessage(getByName))
                        .willReturn(message(getResp1))
        );

        LOG.info("EmbeddingConfigService stubs ready: ListEmbeddingModelConfigs (2 configs), GetEmbeddingModelConfig (by id and by name)");
    }
}
