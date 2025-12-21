package ai.pipestream.wiremock.client;

import ai.pipestream.connector.intake.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.time.Instant;
import java.util.List;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DataSourceAdminMock demonstrating how to use it to configure WireMock stubs.
 * <p>
 * These tests validate all mock methods work correctly by:
 * <ol>
 * <li>Starting a WireMock server with gRPC extension</li>
 * <li>Creating DataSourceAdminMock using the server's port</li>
 * <li>Configuring stubs using DataSourceAdminMock</li>
 * <li>Creating a gRPC client and calling the mocked service</li>
 * <li>Verifying the responses match expected values</li>
 * </ol>
 */
public class DataSourceAdminMockTest {

    private WireMockServer wireMockServer;
    private DataSourceAdminMock dataSourceAdminMock;
    private ManagedChannel channel;
    private DataSourceAdminServiceGrpc.DataSourceAdminServiceBlockingStub stub;

    @BeforeEach
    void setUp() {
        // Start WireMock server with gRPC extension
        WireMockConfiguration config = wireMockConfig()
                .dynamicPort()
                .withRootDirectory("build/resources/test/wiremock")
                .extensions(new GrpcExtensionFactory());

        wireMockServer = new WireMockServer(config);
        wireMockServer.start();

        // Create DataSourceAdminMock using the server's port
        WireMock wireMock = new WireMock(wireMockServer.port());
        dataSourceAdminMock = new DataSourceAdminMock(wireMock);

        // Create a gRPC client channel pointing to WireMock
        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();

        // Create the gRPC stub
        stub = DataSourceAdminServiceGrpc.newBlockingStub(channel);
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

    // ============================================
    // ValidateApiKey Tests
    // ============================================

    @Test
    void testValidateApiKey_Success() {
        // Configure a stub for valid API key
        dataSourceAdminMock.mockValidateApiKey(
                "test-datasource",
                "valid-api-key",
                "test-account",
                "s3",
                "test-drive"
        );

        // Call the mocked service
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId("test-datasource")
                .setApiKey("valid-api-key")
                .build();

        ValidateApiKeyResponse response = stub.validateApiKey(request);

        // Verify the response
        assertNotNull(response);
        assertTrue(response.getValid());
        assertTrue(response.hasConfig());
        DataSourceConfig config = response.getConfig();
        assertEquals("test-datasource", config.getDatasourceId());
        assertEquals("test-account", config.getAccountId());
        assertEquals("s3", config.getConnectorId());
        assertEquals("test-drive", config.getDriveName());
        assertEquals(100_000_000, config.getMaxFileSize());
        assertEquals(1000, config.getRateLimitPerMinute());
    }

    @Test
    void testValidateApiKey_Invalid() {
        // Configure a stub for invalid API key
        dataSourceAdminMock.mockValidateApiKeyInvalid("test-datasource", "wrong-api-key");

        // Call the mocked service
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId("test-datasource")
                .setApiKey("wrong-api-key")
                .build();

        ValidateApiKeyResponse response = stub.validateApiKey(request);

        // Verify the response
        assertNotNull(response);
        assertFalse(response.getValid());
        assertEquals("Invalid API key", response.getMessage());
    }

    @Test
    void testValidateApiKey_Inactive() {
        // Configure a stub for inactive datasource
        dataSourceAdminMock.mockValidateApiKeyInactive("inactive-ds", "some-key");

        // Call the mocked service
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId("inactive-ds")
                .setApiKey("some-key")
                .build();

        ValidateApiKeyResponse response = stub.validateApiKey(request);

        // Verify the response
        assertNotNull(response);
        assertFalse(response.getValid());
        assertTrue(response.getMessage().contains("inactive"));
    }

    @Test
    void testValidateApiKey_DatasourceNotFound() {
        // Configure a stub for datasource not found
        dataSourceAdminMock.mockDatasourceNotFound("nonexistent-ds");

        // Call the mocked service - should throw NOT_FOUND
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId("nonexistent-ds")
                .build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> stub.validateApiKey(request));

        assertEquals(io.grpc.Status.Code.NOT_FOUND, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("DataSource not found"));
    }

    // ============================================
    // GetDataSource Tests
    // ============================================

    @Test
    void testGetDataSource_Success() {
        // Build a test datasource
        DataSource datasource = buildTestDataSource("ds-123", "acct-456", "s3", true);

        // Configure the stub
        dataSourceAdminMock.mockGetDataSource("ds-123", datasource);

        // Call the mocked service
        GetDataSourceRequest request = GetDataSourceRequest.newBuilder()
                .setDatasourceId("ds-123")
                .build();

        GetDataSourceResponse response = stub.getDataSource(request);

        // Verify the response
        assertNotNull(response);
        assertTrue(response.hasDatasource());
        DataSource returned = response.getDatasource();
        assertEquals("ds-123", returned.getDatasourceId());
        assertEquals("acct-456", returned.getAccountId());
        assertEquals("s3", returned.getConnectorId());
        assertTrue(returned.getActive());
    }

    @Test
    void testGetDataSource_NotFound() {
        // Configure a stub for not found
        dataSourceAdminMock.mockGetDataSourceNotFound("missing-ds");

        // Call the mocked service - should throw NOT_FOUND
        GetDataSourceRequest request = GetDataSourceRequest.newBuilder()
                .setDatasourceId("missing-ds")
                .build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> stub.getDataSource(request));

        assertEquals(io.grpc.Status.Code.NOT_FOUND, exception.getStatus().getCode());
    }

    // ============================================
    // CreateDataSource Tests
    // ============================================

    @Test
    void testCreateDataSource_Success() {
        // Build the datasource that will be "created"
        DataSource createdDs = buildTestDataSource("new-ds-id", "acct-123", "gdrive", true);

        // Configure the stub
        dataSourceAdminMock.mockCreateDataSource("acct-123", "gdrive", createdDs);

        // Call the mocked service
        CreateDataSourceRequest request = CreateDataSourceRequest.newBuilder()
                .setAccountId("acct-123")
                .setConnectorId("gdrive")
                .build();

        CreateDataSourceResponse response = stub.createDataSource(request);

        // Verify the response
        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals("DataSource created successfully", response.getMessage());
        assertTrue(response.hasDatasource());
        assertEquals("new-ds-id", response.getDatasource().getDatasourceId());
    }

    @Test
    void testCreateDataSource_Duplicate() {
        // Configure a stub for duplicate error
        dataSourceAdminMock.mockCreateDataSourceDuplicate("acct-123", "s3");

        // Call the mocked service - should throw ALREADY_EXISTS
        CreateDataSourceRequest request = CreateDataSourceRequest.newBuilder()
                .setAccountId("acct-123")
                .setConnectorId("s3")
                .build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> stub.createDataSource(request));

        assertEquals(io.grpc.Status.Code.ALREADY_EXISTS, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("already exists"));
    }

    // ============================================
    // ListDataSources Tests
    // ============================================

    @Test
    void testListDataSources_WithResults() {
        // Build test datasources
        DataSource ds1 = buildTestDataSource("ds-1", "acct-123", "s3", true);
        DataSource ds2 = buildTestDataSource("ds-2", "acct-123", "gdrive", true);

        // Configure the stub
        dataSourceAdminMock.mockListDataSources("acct-123", List.of(ds1, ds2));

        // Call the mocked service
        ListDataSourcesRequest request = ListDataSourcesRequest.newBuilder()
                .setAccountId("acct-123")
                .build();

        ListDataSourcesResponse response = stub.listDataSources(request);

        // Verify the response
        assertNotNull(response);
        assertEquals(2, response.getDatasourcesCount());
        assertEquals(2, response.getTotalCount());
        assertEquals("ds-1", response.getDatasources(0).getDatasourceId());
        assertEquals("ds-2", response.getDatasources(1).getDatasourceId());
    }

    @Test
    void testListDataSources_Empty() {
        // Configure a stub for empty list
        dataSourceAdminMock.mockListDataSourcesEmpty("empty-acct");

        // Call the mocked service
        ListDataSourcesRequest request = ListDataSourcesRequest.newBuilder()
                .setAccountId("empty-acct")
                .build();

        ListDataSourcesResponse response = stub.listDataSources(request);

        // Verify the response
        assertNotNull(response);
        assertEquals(0, response.getDatasourcesCount());
        assertEquals(0, response.getTotalCount());
    }

    // ============================================
    // SetDataSourceStatus Tests
    // ============================================

    @Test
    void testSetDataSourceStatus_Activate() {
        // Configure the stub
        dataSourceAdminMock.mockSetDataSourceStatus("ds-123", true);

        // Call the mocked service
        SetDataSourceStatusRequest request = SetDataSourceStatusRequest.newBuilder()
                .setDatasourceId("ds-123")
                .setActive(true)
                .build();

        SetDataSourceStatusResponse response = stub.setDataSourceStatus(request);

        // Verify the response
        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals("DataSource status updated", response.getMessage());
    }

    @Test
    void testSetDataSourceStatus_Deactivate() {
        // Configure the stub
        dataSourceAdminMock.mockSetDataSourceStatus("ds-456", false);

        // Call the mocked service
        SetDataSourceStatusRequest request = SetDataSourceStatusRequest.newBuilder()
                .setDatasourceId("ds-456")
                .setActive(false)
                .build();

        SetDataSourceStatusResponse response = stub.setDataSourceStatus(request);

        // Verify the response
        assertNotNull(response);
        assertTrue(response.getSuccess());
    }

    // ============================================
    // RotateApiKey Tests
    // ============================================

    @Test
    void testRotateApiKey_Success() {
        // Configure the stub
        dataSourceAdminMock.mockRotateApiKey("ds-123", "new-rotated-api-key");

        // Call the mocked service
        RotateApiKeyRequest request = RotateApiKeyRequest.newBuilder()
                .setDatasourceId("ds-123")
                .build();

        RotateApiKeyResponse response = stub.rotateApiKey(request);

        // Verify the response
        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals("new-rotated-api-key", response.getNewApiKey());
        assertEquals("API key rotated successfully", response.getMessage());
        assertTrue(response.hasOldKeyExpires());
        // Old key should expire in the future (grace period)
        assertTrue(response.getOldKeyExpires().getSeconds() > Instant.now().getEpochSecond());
    }

    // ============================================
    // Reset Test
    // ============================================

    @Test
    void testReset() {
        // Configure a stub
        dataSourceAdminMock.mockValidateApiKey("test-ds", "test-key", "acct", "s3", "drive");

        // Verify it works
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId("test-ds")
                .setApiKey("test-key")
                .build();
        ValidateApiKeyResponse response = stub.validateApiKey(request);
        assertNotNull(response);
        assertTrue(response.getValid());

        // Reset all stubs
        dataSourceAdminMock.reset();

        // Now the call should fail (no matching stub)
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> stub.validateApiKey(request));
        // Accept either UNIMPLEMENTED (WireMock 4 default) or NOT_FOUND
        assertTrue(
                exception.getStatus().getCode() == io.grpc.Status.Code.UNIMPLEMENTED ||
                        exception.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND,
                "Expected UNIMPLEMENTED or NOT_FOUND, got: " + exception.getStatus().getCode()
        );
    }

    // ============================================
    // InitializeDefaults Test
    // ============================================

    @Test
    void testInitializeDefaults_SetsUpTestScenarios() {
        // Initialize with defaults
        WireMock wireMock = new WireMock(wireMockServer.port());
        DataSourceAdminMock mock = new DataSourceAdminMock();
        mock.initializeDefaults(wireMock);

        // Test valid-datasource with valid-api-key
        ValidateApiKeyRequest validRequest = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId("valid-datasource")
                .setApiKey("valid-api-key")
                .build();
        ValidateApiKeyResponse validResponse = stub.validateApiKey(validRequest);
        assertTrue(validResponse.getValid());
        assertEquals("valid-account", validResponse.getConfig().getAccountId());

        // Test valid-datasource with invalid-api-key
        ValidateApiKeyRequest invalidRequest = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId("valid-datasource")
                .setApiKey("invalid-api-key")
                .build();
        ValidateApiKeyResponse invalidResponse = stub.validateApiKey(invalidRequest);
        assertFalse(invalidResponse.getValid());

        // Test inactive-datasource
        ValidateApiKeyRequest inactiveRequest = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId("inactive-datasource")
                .setApiKey("inactive-key")
                .build();
        ValidateApiKeyResponse inactiveResponse = stub.validateApiKey(inactiveRequest);
        assertFalse(inactiveResponse.getValid());
        assertTrue(inactiveResponse.getMessage().contains("inactive"));

        // Test nonexistent datasource - should throw NOT_FOUND
        ValidateApiKeyRequest notFoundRequest = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId("nonexistent")
                .build();
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> stub.validateApiKey(notFoundRequest));
        assertEquals(io.grpc.Status.Code.NOT_FOUND, exception.getStatus().getCode());
    }

    // ============================================
    // Helper Methods
    // ============================================

    private DataSource buildTestDataSource(String datasourceId, String accountId,
                                           String connectorId, boolean active) {
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
                .setDriveName("test-drive")
                .setApiKey("test-api-key")
                .setMaxFileSize(100_000_000)
                .setRateLimitPerMinute(1000)
                .setActive(active)
                .setCreatedAt(timestamp)
                .setUpdatedAt(timestamp)
                .build();
    }
}
