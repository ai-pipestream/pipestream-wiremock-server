package ai.pipestream.wiremock.client;

import com.github.tomakehurst.wiremock.client.WireMock;

/**
 * Interface for service mock initializers that can set up default stubs at server startup.
 * <p>
 * Implementations of this interface will be automatically discovered and initialized
 * by the {@link ServiceMockRegistry} when the WireMock server starts.
 * <p>
 * Example usage:
 * <pre>{@code
 * public class AccountManagerMock implements ServiceMockInitializer {
 *     private final WireMockGrpcService accountService;
 *     
 *     public AccountManagerMock(WireMock wireMock) {
 *         this.accountService = new WireMockGrpcService(wireMock, AccountServiceGrpc.SERVICE_NAME);
 *     }
 *     
 *     @Override
 *     public String getServiceName() {
 *         return AccountServiceGrpc.SERVICE_NAME;
 *     }
 *     
 *     @Override
 *     public void initializeDefaults(WireMock wireMock) {
 *         // Set up default stubs from config, environment, or sensible defaults
 *         mockGetAccount("default-account", "Default Account", "Default account for testing", true);
 *     }
 * }
 * }</pre>
 */
public interface ServiceMockInitializer {
    
    /**
     * Returns the fully-qualified gRPC service name (e.g., "ai.pipestream.repository.v1.account.AccountService").
     * <p>
     * This is used for logging and identification purposes.
     *
     * @return The service name
     */
    String getServiceName();
    
    /**
     * Initializes default stubs for this service.
     * <p>
     * This method is called automatically at server startup. Implementations should:
     * <ul>
     *   <li>Read configuration from files (if present)</li>
     *   <li>Check environment variables for overrides</li>
     *   <li>Set up sensible default stubs if no configuration is found</li>
     * </ul>
     * <p>
     * The WireMock client is provided to allow programmatic stub creation.
     *
     * @param wireMock The WireMock client instance (connected to the WireMock server)
     */
    void initializeDefaults(WireMock wireMock);
}


