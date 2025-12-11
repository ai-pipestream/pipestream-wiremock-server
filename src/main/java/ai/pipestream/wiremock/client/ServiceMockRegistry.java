package ai.pipestream.wiremock.client;

import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Registry that discovers and initializes all service mock initializers at server startup.
 * <p>
 * This class uses Java's {@link ServiceLoader} mechanism to discover all implementations
 * of {@link ServiceMockInitializer} on the classpath. Each discovered initializer is
 * instantiated and its {@link ServiceMockInitializer#initializeDefaults(WireMock)} method
 * is called to set up default stubs.
 * <p>
 * Usage in Main.java:
 * <pre>{@code
 * WireMockServer server = new WireMockServer(config);
 * server.start();
 * 
 * WireMock wireMock = new WireMock(server.port());
 * ServiceMockRegistry registry = new ServiceMockRegistry();
 * registry.initializeAll(wireMock);
 * }</pre>
 * <p>
 * To register a service mock initializer, create a file:
 * <pre>
 * META-INF/services/ai.pipestream.wiremock.client.ServiceMockInitializer
 * </pre>
 * with the fully-qualified class name of your implementation (one per line).
 */
public class ServiceMockRegistry {
    
    private static final Logger LOG = LoggerFactory.getLogger(ServiceMockRegistry.class);
    
    private final List<ServiceMockInitializer> initializers;
    
    /**
     * Creates a new registry and discovers all service mock initializers on the classpath.
     */
    public ServiceMockRegistry() {
        this.initializers = new ArrayList<>();
        discoverInitializers();
    }
    
    /**
     * Discovers all ServiceMockInitializer implementations using ServiceLoader.
     */
    private void discoverInitializers() {
        ServiceLoader<ServiceMockInitializer> loader = ServiceLoader.load(ServiceMockInitializer.class);
        for (ServiceMockInitializer initializer : loader) {
            initializers.add(initializer);
            LOG.debug("Discovered service mock initializer: {}", initializer.getClass().getName());
        }
        
        if (initializers.isEmpty()) {
            LOG.warn("No service mock initializers found. Make sure implementations are registered in META-INF/services/");
        } else {
            LOG.info("Discovered {} service mock initializer(s)", initializers.size());
        }
    }
    
    /**
     * Initializes all discovered service mocks with default stubs.
     * <p>
     * This method calls {@link ServiceMockInitializer#initializeDefaults(WireMock)} on each
     * discovered initializer. If an initializer throws an exception, it is logged but does
     * not prevent other initializers from being initialized.
     *
     * @param wireMock The WireMock client instance (connected to the WireMock server)
     */
    public void initializeAll(WireMock wireMock) {
        if (initializers.isEmpty()) {
            LOG.info("No service mock initializers to initialize");
            return;
        }
        
        LOG.info("Initializing {} service mock(s)...", initializers.size());
        
        for (ServiceMockInitializer initializer : initializers) {
            try {
                String serviceName = initializer.getServiceName();
                LOG.info("Initializing default stubs for service: {}", serviceName);
                initializer.initializeDefaults(wireMock);
                LOG.info("Successfully initialized default stubs for service: {}", serviceName);
            } catch (Exception e) {
                LOG.error("Failed to initialize default stubs for service: {}", 
                        initializer.getServiceName(), e);
                // Continue with other initializers even if one fails
            }
        }
        
        LOG.info("Finished initializing service mocks");
    }
    
    /**
     * Returns the number of discovered initializers.
     *
     * @return The count of initializers
     */
    public int getInitializerCount() {
        return initializers.size();
    }
    
    /**
     * Returns a list of all discovered initializer service names.
     *
     * @return List of service names
     */
    public List<String> getServiceNames() {
        return initializers.stream()
                .map(ServiceMockInitializer::getServiceName)
                .toList();
    }
}

