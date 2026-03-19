package ai.pipestream.wiremock.server;

import ai.pipestream.wiremock.client.ServiceMockRegistry;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.LaunchMode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.io.File;
import java.nio.file.Paths;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

/**
 * Managed bean that handles the lifecycle of WireMock and the Direct gRPC Server.
 */
@ApplicationScoped
public class WiremockManager {

    private static final Logger LOG = Logger.getLogger(WiremockManager.class);

    @ConfigProperty(name = "wiremock.port", defaultValue = "8080")
    int port;

    @ConfigProperty(name = "wiremock.streaming.port", defaultValue = "50052")
    int streamingPort;

    @ConfigProperty(name = "wiremock.max-message-size", defaultValue = "2147483647")
    int maxMessageSize;

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer streamingServer;

    void onStart(@Observes StartupEvent ev) {
        LOG.info("Starting Pipestream WireMock Services...");

        // 1. Start WireMock Server
        LOG.infof("Starting WireMock Server on port %d", port);
        WireMockConfiguration config = wireMockConfig()
                .port(port)
                .bindAddress("0.0.0.0");

        // In dev mode, we point directly to the source resources to support hot-reloading of mappings
        if (LaunchMode.current() == LaunchMode.DEVELOPMENT) {
            String projectRoot = System.getProperty("user.dir");
            File resourceDir = new File(projectRoot, "src/main/resources/wiremock");
            if (resourceDir.exists()) {
                LOG.infof("Dev mode: using filesystem resource directory: %s", resourceDir.getAbsolutePath());
                config.withRootDirectory(resourceDir.getAbsolutePath());
            } else {
                LOG.warnf("Dev mode: resource directory not found at %s, falling back to classpath", resourceDir.getAbsolutePath());
                config.usingFilesUnderClasspath("wiremock");
            }
        } else {
            config.usingFilesUnderClasspath("wiremock");
        }
        
        config.extensions(new GrpcExtensionFactory());

        wireMockServer = new WireMockServer(config);
        wireMockServer.start();
        LOG.info("WireMock Server started.");

        // 2. Initialize Service Mocks
        WireMock wireMock = new WireMock(wireMockServer.port());
        ServiceMockRegistry registry = new ServiceMockRegistry();
        if (registry.getInitializerCount() > 0) {
            LOG.infof("Initializing %d service mock(s)...", registry.getInitializerCount());
            registry.initializeAll(wireMock);
            LOG.infof("Service mocks initialized. Services: %s", String.join(", ", registry.getServiceNames()));
        } else {
            LOG.info("No service mock initializers found.");
        }

        // 3. Start Direct gRPC Streaming Server
        LOG.infof("Starting Direct Streaming gRPC Server on port %d", streamingPort);
        streamingServer = new DirectWireMockGrpcServer(wireMockServer, streamingPort, maxMessageSize);
        try {
            streamingServer.start();
            LOG.info("Direct Streaming gRPC Server started.");
        } catch (java.io.IOException e) {
            LOG.error("Failed to start Direct Streaming gRPC Server", e);
            throw new RuntimeException(e);
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOG.info("Stopping Pipestream WireMock Services...");
        if (streamingServer != null) {
            try {
                streamingServer.stop();
            } catch (InterruptedException e) {
                LOG.error("Interrupted while stopping Direct Streaming gRPC Server", e);
                Thread.currentThread().interrupt();
            }
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
        LOG.info("Services stopped.");
    }
}
