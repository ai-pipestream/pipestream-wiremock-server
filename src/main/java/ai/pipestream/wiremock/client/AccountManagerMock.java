package ai.pipestream.wiremock.client;

import ai.pipestream.repository.v1.account.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring account-manager mocks in WireMock.
 * <p>
 * This class provides methods to configure stubs for the account-manager gRPC service.
 * It uses the server's protobuf classes to build responses, converts them to JSON,
 * and configures WireMock stubs via the gRPC extension.
 * <p>
 * This class implements {@link ServiceMockInitializer} and will be automatically
 * discovered and initialized at server startup by the {@link ServiceMockRegistry}.
 * <p>
 * Configuration can be provided via:
 * <ul>
 *   <li>Environment variables: {@code WIREMOCK_ACCOUNT_GETACCOUNT_DEFAULT_*}</li>
 *   <li>Config file: {@code wiremock-mocks.properties}</li>
 *   <li>System properties: {@code wiremock.account.GetAccount.default.*}</li>
 * </ul>
 * <p>
 * Example configuration:
 * <pre>
 * wiremock.account.GetAccount.default.id=default-account
 * wiremock.account.GetAccount.default.name=Default Account
 * wiremock.account.GetAccount.default.description=Default account for testing
 * wiremock.account.GetAccount.default.active=true
 * </pre>
 * <p>
 * Example usage in Main.java:
 * <pre>{@code
 * WireMockServer server = new WireMockServer(config);
 * server.start();
 * 
 * WireMock wireMock = new WireMock(server.port());
 * AccountManagerMock accountMock = new AccountManagerMock(wireMock);
 * accountMock.mockGetAccount("default-account", "Default", "Default account", true);
 * }</pre>
 */
public class AccountManagerMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(AccountManagerMock.class);
    
    private static final String SERVICE_NAME = AccountServiceGrpc.SERVICE_NAME;
    
    private WireMockGrpcService accountService;

    /**
     * Create a helper for the given WireMock client.
     * <p>
     * This constructor is used when the WireMock server is embedded in the same process.
     * The WireMock client should be created from the WireMockServer instance.
     *
     * @param wireMock The WireMock client instance (connected to WireMock server)
     */
    public AccountManagerMock(WireMock wireMock) {
        // The service name is the full gRPC service name
        this.accountService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }
    
    /**
     * Default constructor for ServiceLoader discovery.
     * <p>
     * The WireMock client will be provided via {@link #initializeDefaults(WireMock)}.
     */
    public AccountManagerMock() {
        // ServiceLoader will call initializeDefaults with the WireMock instance
    }
    
    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }
    
    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.accountService = new WireMockGrpcService(wireMock, SERVICE_NAME);
        
        MockConfig config = new MockConfig();
        
        // Read configuration with fallback to sensible defaults
        String accountId = config.get("wiremock.account.GetAccount.default.id", "default-account");
        String name = config.get("wiremock.account.GetAccount.default.name", "Default Account");
        String description = config.get("wiremock.account.GetAccount.default.description", "Default account for testing");
        boolean active = config.getBoolean("wiremock.account.GetAccount.default.active", true);
        
        LOG.info("Initializing default AccountService stubs with accountId: {}", accountId);
        
        // Set up default stub
        mockGetAccount(accountId, name, description, active);
        
        // Optionally set up NOT_FOUND stub for a known non-existent account
        String notFoundAccountId = config.get("wiremock.account.GetAccount.notfound.id", "nonexistent-account");
        if (config.hasKey("wiremock.account.GetAccount.notfound.id") || 
            !notFoundAccountId.equals("nonexistent-account")) {
            mockAccountNotFound(notFoundAccountId);
        }
    }

    /**
     * Mock a successful GetAccount response.
     * <p>
     * Uses the server's protobuf classes to build the response, converts to JSON,
     * and configures a WireMock stub.
     *
     * @param accountId The account ID
     * @param name The account name
     * @param description The account description
     * @param active Whether the account is active
     */
    public void mockGetAccount(String accountId, String name, String description, boolean active) {
        Account.Builder account = Account.newBuilder()
                .setAccountId(accountId)
                .setName(name)
                .setDescription(description)
                .setActive(active);

        GetAccountResponse response = GetAccountResponse.newBuilder()
                .setAccount(account)
                .build();

        // Use message() directly with the shaded protobuf from WireMock
        accountService.stubFor(
                method("GetAccount")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a GetAccount response that returns NOT_FOUND.
     *
     * @param accountId The account ID that doesn't exist
     */
    public void mockAccountNotFound(String accountId) {
        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build();

        // Use message() for request matching and Status enum for error
        accountService.stubFor(
                method("GetAccount")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "Account not found: " + accountId)
        );
    }

    /**
     * Reset all WireMock stubs for the account service.
     */
    public void reset() {
        accountService.resetAll();
    }
}

