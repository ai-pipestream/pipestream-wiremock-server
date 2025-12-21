package ai.pipestream.wiremock.client;

import ai.pipestream.connector.intake.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring datasource-admin mocks in WireMock.
 * <p>
 * This class provides methods to configure stubs for the DataSourceAdminService gRPC service.
 * It uses the server's protobuf classes to build responses, converts them to JSON,
 * and configures WireMock stubs via the gRPC extension.
 * <p>
 * This class implements {@link ServiceMockInitializer} and will be automatically
 * discovered and initialized at server startup by the {@link ServiceMockRegistry}.
 * <p>
 * Configuration can be provided via:
 * <ul>
 *   <li>Environment variables: {@code WIREMOCK_DATASOURCE_*}</li>
 *   <li>Config file: {@code wiremock-mocks.properties}</li>
 *   <li>System properties: {@code wiremock.datasource.*}</li>
 * </ul>
 */
public class DataSourceAdminMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourceAdminMock.class);

    private static final String SERVICE_NAME = DataSourceAdminServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService datasourceService;

    /**
     * Create a helper for the given WireMock client.
     *
     * @param wireMock The WireMock client instance (connected to WireMock server)
     */
    public DataSourceAdminMock(WireMock wireMock) {
        this.datasourceService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    /**
     * Default constructor for ServiceLoader discovery.
     */
    public DataSourceAdminMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.datasourceService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        MockConfig config = new MockConfig();

        // Read configuration for default datasource
        String datasourceId = config.get("wiremock.datasource.ValidateApiKey.default.id", "default-datasource");
        String accountId = config.get("wiremock.datasource.ValidateApiKey.default.accountId", "default-account");
        String connectorId = config.get("wiremock.datasource.ValidateApiKey.default.connectorId", "s3");
        String driveName = config.get("wiremock.datasource.ValidateApiKey.default.driveName", "default-drive");
        String apiKey = config.get("wiremock.datasource.ValidateApiKey.default.apiKey", "valid-api-key");

        LOG.info("Initializing default DataSourceAdminService stubs with datasourceId: {}", datasourceId);

        // Set up default stub for valid API key
        mockValidateApiKey(datasourceId, apiKey, accountId, connectorId, driveName);

        // Set up test datasources for integration tests
        mockValidateApiKey("valid-datasource", "valid-api-key", "valid-account", "s3", "test-drive");
        mockValidateApiKeyInvalid("valid-datasource", "invalid-api-key");
        mockValidateApiKey("inactive-account-datasource", "inactive-account-key", "inactive-account", "s3", "test-drive");
        mockValidateApiKey("missing-account-datasource", "missing-account-key", "nonexistent-account", "s3", "test-drive");

        LOG.info("Added test datasource stubs for integration tests");
    }

    /**
     * Mock a successful ValidateApiKey response.
     *
     * @param datasourceId The datasource ID
     * @param apiKey The API key to match
     * @param accountId The account ID to return
     * @param connectorId The connector type ID
     * @param driveName The drive name for storage
     */
    public void mockValidateApiKey(String datasourceId, String apiKey, String accountId,
                                    String connectorId, String driveName) {
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
                .build();

        DataSourceConfig config = DataSourceConfig.newBuilder()
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .setConnectorId(connectorId)
                .setDriveName(driveName)
                .setMaxFileSize(100_000_000)
                .setRateLimitPerMinute(1000)
                .build();

        ValidateApiKeyResponse response = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConfig(config)
                .build();

        datasourceService.stubFor(
                method("ValidateApiKey")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock an invalid API key response.
     *
     * @param datasourceId The datasource ID
     * @param apiKey The invalid API key to match
     */
    public void mockValidateApiKeyInvalid(String datasourceId, String apiKey) {
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
                .build();

        ValidateApiKeyResponse response = ValidateApiKeyResponse.newBuilder()
                .setValid(false)
                .setMessage("Invalid API key")
                .build();

        datasourceService.stubFor(
                method("ValidateApiKey")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a datasource not found response.
     *
     * @param datasourceId The datasource ID that doesn't exist
     */
    public void mockDatasourceNotFound(String datasourceId) {
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build();

        datasourceService.stubFor(
                method("ValidateApiKey")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "DataSource not found: " + datasourceId)
        );
    }

    /**
     * Reset all WireMock stubs for the datasource service.
     */
    public void reset() {
        datasourceService.resetAll();
    }
}
