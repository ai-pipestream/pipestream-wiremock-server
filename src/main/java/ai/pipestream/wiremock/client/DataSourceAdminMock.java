package ai.pipestream.wiremock.client;

import ai.pipestream.connector.intake.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.time.Instant;
import java.util.List;

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
        String datasourceId = config.get("wiremock.datasource.ValidateApiKey.default.datasourceId", "test-datasource");
        String accountId = config.get("wiremock.datasource.ValidateApiKey.default.accountId", "test-account");
        String connectorId = config.get("wiremock.datasource.ValidateApiKey.default.connectorId", "s3");
        String driveName = config.get("wiremock.datasource.ValidateApiKey.default.driveName", "test-drive");
        String apiKey = config.get("wiremock.datasource.ValidateApiKey.default.apiKey", "test-api-key");
        boolean active = config.getBoolean("wiremock.datasource.ValidateApiKey.default.active", true);

        LOG.info("Initializing default DataSourceAdminService stubs with datasourceId: {}", datasourceId);

        // Create test datasource for defaults
        DataSource testDataSource = buildDataSource(datasourceId, accountId, connectorId, driveName, apiKey, active);

        // Set up default stubs
        mockValidateApiKey(datasourceId, apiKey, accountId, connectorId, driveName);
        mockGetDataSource(datasourceId, testDataSource);
        mockListDataSources(accountId, List.of(testDataSource));

        // Set up test datasources for integration tests
        DataSource validDs = buildDataSource("valid-datasource", "valid-account", "s3", "test-drive", "valid-api-key", true);
        mockValidateApiKey("valid-datasource", "valid-api-key", "valid-account", "s3", "test-drive");
        mockGetDataSource("valid-datasource", validDs);

        // Invalid API key
        mockValidateApiKeyInvalid("valid-datasource", "invalid-api-key");

        // Inactive datasource
        DataSource inactiveDs = buildDataSource("inactive-datasource", "test-account", "s3", "test-drive", "inactive-key", false);
        mockValidateApiKeyInactive("inactive-datasource", "inactive-key");
        mockGetDataSource("inactive-datasource", inactiveDs);

        // Account scenarios for connector-intake testing
        mockValidateApiKey("inactive-account-datasource", "inactive-account-key", "inactive-account", "s3", "test-drive");
        mockValidateApiKey("missing-account-datasource", "missing-account-key", "nonexistent-account", "s3", "test-drive");

        // Not found scenario
        mockDatasourceNotFound("nonexistent");

        LOG.info("Added test datasource stubs: valid-datasource, inactive-datasource, nonexistent (NOT_FOUND)");
    }

    /**
     * Build a DataSource proto message.
     */
    private DataSource buildDataSource(String datasourceId, String accountId, String connectorId,
                                        String driveName, String apiKey, boolean active) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        return DataSource.newBuilder()
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .setConnectorId(connectorId)
                .setName(datasourceId + "-name")
                .setDriveName(driveName)
                .setApiKey(apiKey)
                .setMaxFileSize(100_000_000)
                .setRateLimitPerMinute(1000)
                .setActive(active)
                .setCreatedAt(timestamp)
                .setUpdatedAt(timestamp)
                .build();
    }

    // ============================================
    // ValidateApiKey mocks
    // ============================================

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
     * Mock a successful ValidateApiKey response with a full DataSourceConfig.
     * <p>
     * This method allows callers to specify the complete DataSourceConfig including
     * ConnectorGlobalConfig (PersistenceConfig, HydrationConfig, etc.) for testing
     * the 2-tier configuration model.
     *
     * @param datasourceId The datasource ID
     * @param apiKey The API key to match
     * @param config The full DataSourceConfig to return (must include datasourceId and accountId)
     */
    public void mockValidateApiKeyWithConfig(String datasourceId, String apiKey, DataSourceConfig config) {
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
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
     * Mock a ValidateApiKey response for an inactive datasource.
     *
     * @param datasourceId The datasource ID
     * @param apiKey The API key to match
     */
    public void mockValidateApiKeyInactive(String datasourceId, String apiKey) {
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
                .build();

        ValidateApiKeyResponse response = ValidateApiKeyResponse.newBuilder()
                .setValid(false)
                .setMessage("DataSource is inactive: " + datasourceId)
                .build();

        datasourceService.stubFor(
                method("ValidateApiKey")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a datasource not found response for ValidateApiKey.
     *
     * @param datasourceId The datasource ID that doesn't exist
     */
    public void mockDatasourceNotFound(String datasourceId) {
        // Match any API key for this datasource
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build();

        datasourceService.stubFor(
                method("ValidateApiKey")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "DataSource not found: " + datasourceId)
        );
    }

    // ============================================
    // GetDataSource mocks
    // ============================================

    /**
     * Mock a successful GetDataSource response.
     *
     * @param datasourceId The datasource ID
     * @param datasource The datasource to return
     */
    public void mockGetDataSource(String datasourceId, DataSource datasource) {
        GetDataSourceRequest request = GetDataSourceRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build();

        GetDataSourceResponse response = GetDataSourceResponse.newBuilder()
                .setDatasource(datasource)
                .build();

        datasourceService.stubFor(
                method("GetDataSource")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a GetDataSource NOT_FOUND response.
     *
     * @param datasourceId The datasource ID that doesn't exist
     */
    public void mockGetDataSourceNotFound(String datasourceId) {
        GetDataSourceRequest request = GetDataSourceRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build();

        datasourceService.stubFor(
                method("GetDataSource")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "DataSource not found: " + datasourceId)
        );
    }

    // ============================================
    // CreateDataSource mocks
    // ============================================

    /**
     * Mock a successful CreateDataSource response.
     *
     * @param accountId The account ID
     * @param connectorId The connector type
     * @param createdDatasource The datasource to return (with generated ID and API key)
     */
    public void mockCreateDataSource(String accountId, String connectorId, DataSource createdDatasource) {
        CreateDataSourceRequest request = CreateDataSourceRequest.newBuilder()
                .setAccountId(accountId)
                .setConnectorId(connectorId)
                .build();

        CreateDataSourceResponse response = CreateDataSourceResponse.newBuilder()
                .setSuccess(true)
                .setDatasource(createdDatasource)
                .setMessage("DataSource created successfully")
                .build();

        datasourceService.stubFor(
                method("CreateDataSource")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a CreateDataSource duplicate error response.
     *
     * @param accountId The account ID
     * @param connectorId The connector type
     */
    public void mockCreateDataSourceDuplicate(String accountId, String connectorId) {
        CreateDataSourceRequest request = CreateDataSourceRequest.newBuilder()
                .setAccountId(accountId)
                .setConnectorId(connectorId)
                .build();

        datasourceService.stubFor(
                method("CreateDataSource")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.ALREADY_EXISTS,
                                "DataSource already exists for account " + accountId + " and connector " + connectorId)
        );
    }

    // ============================================
    // ListDataSources mocks
    // ============================================

    /**
     * Mock a ListDataSources response.
     *
     * @param accountId The account ID to filter by
     * @param datasources The list of datasources to return
     */
    public void mockListDataSources(String accountId, List<DataSource> datasources) {
        ListDataSourcesRequest request = ListDataSourcesRequest.newBuilder()
                .setAccountId(accountId)
                .build();

        ListDataSourcesResponse response = ListDataSourcesResponse.newBuilder()
                .addAllDatasources(datasources)
                .setTotalCount(datasources.size())
                .build();

        datasourceService.stubFor(
                method("ListDataSources")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock an empty ListDataSources response.
     *
     * @param accountId The account ID with no datasources
     */
    public void mockListDataSourcesEmpty(String accountId) {
        mockListDataSources(accountId, List.of());
    }

    // ============================================
    // SetDataSourceStatus mocks
    // ============================================

    /**
     * Mock a successful SetDataSourceStatus response.
     *
     * @param datasourceId The datasource ID
     * @param active The new active status
     */
    public void mockSetDataSourceStatus(String datasourceId, boolean active) {
        SetDataSourceStatusRequest request = SetDataSourceStatusRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setActive(active)
                .build();

        SetDataSourceStatusResponse response = SetDataSourceStatusResponse.newBuilder()
                .setSuccess(true)
                .setMessage("DataSource status updated")
                .build();

        datasourceService.stubFor(
                method("SetDataSourceStatus")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    // ============================================
    // RotateApiKey mocks
    // ============================================

    /**
     * Mock a successful RotateApiKey response.
     *
     * @param datasourceId The datasource ID
     * @param newApiKey The new API key to return
     */
    public void mockRotateApiKey(String datasourceId, String newApiKey) {
        RotateApiKeyRequest request = RotateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .build();

        Instant expiry = Instant.now().plusSeconds(3600); // 1 hour grace period
        Timestamp expiryTimestamp = Timestamp.newBuilder()
                .setSeconds(expiry.getEpochSecond())
                .setNanos(expiry.getNano())
                .build();

        RotateApiKeyResponse response = RotateApiKeyResponse.newBuilder()
                .setSuccess(true)
                .setNewApiKey(newApiKey)
                .setOldKeyExpires(expiryTimestamp)
                .setMessage("API key rotated successfully")
                .build();

        datasourceService.stubFor(
                method("RotateApiKey")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Reset all WireMock stubs for the datasource service.
     */
    public void reset() {
        datasourceService.resetAll();
    }
}
