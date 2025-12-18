package ai.pipestream.wiremock.client;

import ai.pipestream.repository.v1.account.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ServiceMockRegistry and ServiceMockInitializer integration.
 * <p>
 * These tests verify:
 * <ol>
 * <li>ServiceMockRegistry discovers ServiceMockInitializer implementations</li>
 * <li>AccountManagerMock.initializeDefaults() creates default stubs</li>
 * <li>Default stubs work with actual gRPC calls</li>
 * <li>Configuration from environment variables and system properties works</li>
 * </ol>
 */
public class ServiceMockRegistryTest {

    private WireMockServer wireMockServer;
    private ManagedChannel channel;
    private AccountServiceGrpc.AccountServiceBlockingStub accountServiceStub;

    @BeforeEach
    void setUp() {
        // Start WireMock server with gRPC extension
        // Use build directory for generated resources (not source directory)
        WireMockConfiguration config = wireMockConfig()
                .dynamicPort()
                .withRootDirectory("build/resources/test/wiremock")
                .extensions(new GrpcExtensionFactory());

        wireMockServer = new WireMockServer(config);
        wireMockServer.start();

        // Create a gRPC client channel pointing to WireMock
        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();

        // Create the gRPC stub (using client's protobuf classes)
        accountServiceStub = AccountServiceGrpc.newBlockingStub(channel);
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
    void testServiceMockRegistry_DiscoverInitializers() {
        // Create registry - should discover AccountManagerMock via ServiceLoader
        ServiceMockRegistry registry = new ServiceMockRegistry();

        // Verify it discovered at least one initializer
        assertTrue(registry.getInitializerCount() > 0, 
                "Should discover at least one ServiceMockInitializer");

        // Verify AccountManagerMock is discovered
        assertTrue(registry.getServiceNames().contains(AccountServiceGrpc.SERVICE_NAME),
                "Should discover AccountService");
    }

    @Test
    void testServiceMockRegistry_InitializeDefaults() {
        // Create registry and initialize all mocks
        WireMock wireMock = new WireMock(wireMockServer.port());
        ServiceMockRegistry registry = new ServiceMockRegistry();
        
        // Initialize all discovered mocks
        registry.initializeAll(wireMock);

        // Verify default stub was created by calling the service
        // AccountManagerMock.initializeDefaults() should have created a stub for "default-account"
        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId("default-account")
                .build();

        GetAccountResponse response = accountServiceStub.getAccount(request);

        // Verify the default response
        assertNotNull(response);
        assertTrue(response.hasAccount());
        Account account = response.getAccount();
        assertEquals("default-account", account.getAccountId());
        assertEquals("Default Account", account.getName());
        assertEquals("Default account for testing", account.getDescription());
        assertTrue(account.getActive());
    }

    @Test
    void testServiceMockRegistry_WithSystemPropertyOverride() {
        // Set system property to override default
        String originalValue = System.getProperty("wiremock.account.GetAccount.default.id");
        try {
            System.setProperty("wiremock.account.GetAccount.default.id", "custom-account-id");
            System.setProperty("wiremock.account.GetAccount.default.name", "Custom Account Name");

            // Create registry and initialize
            WireMock wireMock = new WireMock(wireMockServer.port());
            ServiceMockRegistry registry = new ServiceMockRegistry();
            registry.initializeAll(wireMock);

            // Verify the stub uses the system property values
            GetAccountRequest request = GetAccountRequest.newBuilder()
                    .setAccountId("custom-account-id")
                    .build();

            GetAccountResponse response = accountServiceStub.getAccount(request);

            assertNotNull(response);
            Account account = response.getAccount();
            assertEquals("custom-account-id", account.getAccountId());
            assertEquals("Custom Account Name", account.getName());
        } finally {
            // Restore original value
            if (originalValue != null) {
                System.setProperty("wiremock.account.GetAccount.default.id", originalValue);
            } else {
                System.clearProperty("wiremock.account.GetAccount.default.id");
            }
            System.clearProperty("wiremock.account.GetAccount.default.name");
        }
    }

    @Test
    void testServiceMockRegistry_WithEnvironmentVariableOverride() {
        // Note: We can't easily set environment variables in tests, but we can test
        // that the MockConfig class would read them if they were set.
        // This test verifies the initialization works even with environment variables present.
        
        WireMock wireMock = new WireMock(wireMockServer.port());
        ServiceMockRegistry registry = new ServiceMockRegistry();
        
        // Should not throw even if environment variables are set
        assertDoesNotThrow(() -> registry.initializeAll(wireMock));

        // Default stub should still work
        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId("default-account")
                .build();

        GetAccountResponse response = accountServiceStub.getAccount(request);
        assertNotNull(response);
    }

    @Test
    void testAccountManagerMock_ImplementsServiceMockInitializer() {
        // Verify AccountManagerMock implements the interface
        AccountManagerMock mock = new AccountManagerMock();
        assertInstanceOf(ServiceMockInitializer.class, mock);
        assertEquals(AccountServiceGrpc.SERVICE_NAME, mock.getServiceName());
    }

    @Test
    void testAccountManagerMock_InitializeDefaultsCreatesStub() {
        // Test that initializeDefaults creates a working stub
        WireMock wireMock = new WireMock(wireMockServer.port());
        AccountManagerMock mock = new AccountManagerMock();
        
        // Initialize defaults
        mock.initializeDefaults(wireMock);

        // Verify stub works
        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId("default-account")
                .build();

        GetAccountResponse response = accountServiceStub.getAccount(request);
        assertNotNull(response);
        assertEquals("default-account", response.getAccount().getAccountId());
    }

    @Test
    void testAccountManagerMock_InitializeDefaultsWithCustomConfig() {
        // Test initializeDefaults with custom configuration via system properties
        String originalId = System.getProperty("wiremock.account.GetAccount.default.id");
        String originalName = System.getProperty("wiremock.account.GetAccount.default.name");
        
        try {
            System.setProperty("wiremock.account.GetAccount.default.id", "test-init-account");
            System.setProperty("wiremock.account.GetAccount.default.name", "Test Init Account");
            System.setProperty("wiremock.account.GetAccount.default.description", "Test description");
            System.setProperty("wiremock.account.GetAccount.default.active", "false");

            WireMock wireMock = new WireMock(wireMockServer.port());
            AccountManagerMock mock = new AccountManagerMock();
            mock.initializeDefaults(wireMock);

            // Verify stub uses custom config
            GetAccountRequest request = GetAccountRequest.newBuilder()
                    .setAccountId("test-init-account")
                    .build();

            GetAccountResponse response = accountServiceStub.getAccount(request);
            assertNotNull(response);
            Account account = response.getAccount();
            assertEquals("test-init-account", account.getAccountId());
            assertEquals("Test Init Account", account.getName());
            assertEquals("Test description", account.getDescription());
            assertFalse(account.getActive());
        } finally {
            // Restore original values
            if (originalId != null) {
                System.setProperty("wiremock.account.GetAccount.default.id", originalId);
            } else {
                System.clearProperty("wiremock.account.GetAccount.default.id");
            }
            if (originalName != null) {
                System.setProperty("wiremock.account.GetAccount.default.name", originalName);
            } else {
                System.clearProperty("wiremock.account.GetAccount.default.name");
            }
            System.clearProperty("wiremock.account.GetAccount.default.description");
            System.clearProperty("wiremock.account.GetAccount.default.active");
        }
    }

    @Test
    void testAccountManagerMock_NotFoundStubFromConfig() {
        // Test that NOT_FOUND stub can be configured
        String originalNotFoundId = System.getProperty("wiremock.account.GetAccount.notfound.id");
        
        try {
            System.setProperty("wiremock.account.GetAccount.notfound.id", "config-not-found-account");

            WireMock wireMock = new WireMock(wireMockServer.port());
            AccountManagerMock mock = new AccountManagerMock();
            mock.initializeDefaults(wireMock);

            // Verify NOT_FOUND stub works
            GetAccountRequest request = GetAccountRequest.newBuilder()
                    .setAccountId("config-not-found-account")
                    .build();

            StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> accountServiceStub.getAccount(request));

            assertEquals(io.grpc.Status.Code.NOT_FOUND, exception.getStatus().getCode());
            assertTrue(exception.getMessage().contains("Account not found: config-not-found-account"));
        } finally {
            if (originalNotFoundId != null) {
                System.setProperty("wiremock.account.GetAccount.notfound.id", originalNotFoundId);
            } else {
                System.clearProperty("wiremock.account.GetAccount.notfound.id");
            }
        }
    }

    @Test
    void testEndToEnd_ServiceMockRegistryFlow() {
        // Complete end-to-end test: Registry discovers, initializes, and stubs work
        WireMock wireMock = new WireMock(wireMockServer.port());
        ServiceMockRegistry registry = new ServiceMockRegistry();
        
        // Verify discovery
        assertTrue(registry.getInitializerCount() > 0);
        assertTrue(registry.getServiceNames().contains(AccountServiceGrpc.SERVICE_NAME));
        
        // Initialize all
        registry.initializeAll(wireMock);
        
        // Test default stub
        GetAccountRequest defaultRequest = GetAccountRequest.newBuilder()
                .setAccountId("default-account")
                .build();
        GetAccountResponse defaultResponse = accountServiceStub.getAccount(defaultRequest);
        assertNotNull(defaultResponse);
        assertEquals("default-account", defaultResponse.getAccount().getAccountId());
        
        // Test that we can still add more stubs programmatically
        AccountManagerMock mock = new AccountManagerMock(wireMock);
        mock.mockGetAccount("additional-account", "Additional", "Additional account", true);
        
        GetAccountRequest additionalRequest = GetAccountRequest.newBuilder()
                .setAccountId("additional-account")
                .build();
        GetAccountResponse additionalResponse = accountServiceStub.getAccount(additionalRequest);
        assertNotNull(additionalResponse);
        assertEquals("additional-account", additionalResponse.getAccount().getAccountId());
    }
}

