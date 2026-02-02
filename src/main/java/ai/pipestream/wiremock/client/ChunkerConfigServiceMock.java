package ai.pipestream.wiremock.client;

import ai.pipestream.opensearch.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.time.Instant;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * WireMock stub helper for ChunkerConfigService (opensearch-manager).
 * <p>
 * Provides dummy data for integration tests: predefined chunker configs so
 * consumers (e.g. module-chunker, module-opensearch-sink) can run against the
 * WireMock container without a real opensearch-manager.
 * <p>
 * Implements {@link ServiceMockInitializer}; registered in META-INF/services
 * for automatic discovery at server startup.
 */
public class ChunkerConfigServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(ChunkerConfigServiceMock.class);

    private static final String SERVICE_NAME = ChunkerConfigServiceGrpc.SERVICE_NAME;

    /** Dummy chunker config ID returned by List and Get. */
    public static final String DUMMY_CHUNKER_CONFIG_ID = "wiremock-chunker-config-001";
    /** Dummy chunker config name. */
    public static final String DUMMY_CHUNKER_CONFIG_NAME = "wiremock-token-512-50";
    /** Dummy config_id (derived format). */
    public static final String DUMMY_CONFIG_ID = "token-body-512-50";

    /** Second dummy config ID. */
    public static final String DUMMY_CHUNKER_CONFIG_ID_2 = "wiremock-chunker-config-002";
    public static final String DUMMY_CHUNKER_CONFIG_NAME_2 = "wiremock-sentence-1000";
    public static final String DUMMY_CONFIG_ID_2 = "sentence-title-1000-100";

    private WireMockGrpcService chunkerConfigService;

    public ChunkerConfigServiceMock(WireMock wireMock) {
        this.chunkerConfigService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    public ChunkerConfigServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.chunkerConfigService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default ChunkerConfigService stubs (dummy data for step 4.5)");

        Timestamp now = Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(0)
                .build();

        Struct configJson1 = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("token").build())
                .putFields("sourceField", Value.newBuilder().setStringValue("body").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(512).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(50).build())
                .build();

        Struct configJson2 = Struct.newBuilder()
                .putFields("algorithm", Value.newBuilder().setStringValue("sentence").build())
                .putFields("sourceField", Value.newBuilder().setStringValue("title").build())
                .putFields("chunkSize", Value.newBuilder().setNumberValue(1000).build())
                .putFields("chunkOverlap", Value.newBuilder().setNumberValue(100).build())
                .build();

        ChunkerConfig config1 = ChunkerConfig.newBuilder()
                .setId(DUMMY_CHUNKER_CONFIG_ID)
                .setName(DUMMY_CHUNKER_CONFIG_NAME)
                .setConfigId(DUMMY_CONFIG_ID)
                .setConfigJson(configJson1)
                .setCreatedAt(now)
                .setUpdatedAt(now)
                .build();

        ChunkerConfig config2 = ChunkerConfig.newBuilder()
                .setId(DUMMY_CHUNKER_CONFIG_ID_2)
                .setName(DUMMY_CHUNKER_CONFIG_NAME_2)
                .setConfigId(DUMMY_CONFIG_ID_2)
                .setConfigJson(configJson2)
                .setCreatedAt(now)
                .setUpdatedAt(now)
                .build();

        ListChunkerConfigsResponse listResponse = ListChunkerConfigsResponse.newBuilder()
                .addConfigs(config1)
                .addConfigs(config2)
                .build();

        ListChunkerConfigsRequest listRequest = ListChunkerConfigsRequest.newBuilder()
                .setPageSize(10)
                .build();
        chunkerConfigService.stubFor(
                method("ListChunkerConfigs")
                        .withRequestMessage(WireMockGrpc.equalToMessage(listRequest))
                        .willReturn(message(listResponse))
        );

        GetChunkerConfigRequest getById1 = GetChunkerConfigRequest.newBuilder()
                .setId(DUMMY_CHUNKER_CONFIG_ID)
                .build();
        GetChunkerConfigResponse getResp1 = GetChunkerConfigResponse.newBuilder()
                .setConfig(config1)
                .build();
        chunkerConfigService.stubFor(
                method("GetChunkerConfig")
                        .withRequestMessage(WireMockGrpc.equalToMessage(getById1))
                        .willReturn(message(getResp1))
        );

        GetChunkerConfigRequest getById2 = GetChunkerConfigRequest.newBuilder()
                .setId(DUMMY_CHUNKER_CONFIG_ID_2)
                .build();
        GetChunkerConfigResponse getResp2 = GetChunkerConfigResponse.newBuilder()
                .setConfig(config2)
                .build();
        chunkerConfigService.stubFor(
                method("GetChunkerConfig")
                        .withRequestMessage(WireMockGrpc.equalToMessage(getById2))
                        .willReturn(message(getResp2))
        );

        GetChunkerConfigRequest getByName = GetChunkerConfigRequest.newBuilder()
                .setId(DUMMY_CHUNKER_CONFIG_NAME)
                .setByName(true)
                .build();
        chunkerConfigService.stubFor(
                method("GetChunkerConfig")
                        .withRequestMessage(WireMockGrpc.equalToMessage(getByName))
                        .willReturn(message(getResp1))
        );

        LOG.info("ChunkerConfigService stubs ready: ListChunkerConfigs (2 configs), GetChunkerConfig (by id and by name)");
    }
}
