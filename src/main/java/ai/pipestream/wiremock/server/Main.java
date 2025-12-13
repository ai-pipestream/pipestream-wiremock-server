package ai.pipestream.wiremock.server;

import ai.pipestream.wiremock.client.ServiceMockRegistry;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.wiremock.grpc.GrpcExtensionFactory;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The entry point for the Pipestream WireMock Server.
 * <p>
 * This application starts two separate servers:
 * <ol>
 * <li><b>WireMock Server (Port 8080):</b> Handles HTTP requests and standard
 * gRPC mocks via the {@code wiremock-grpc-extension}.</li>
 * <li><b>Direct gRPC Server (Port 50052):</b> A custom Netty-based gRPC server
 * that handles complex streaming scenarios
 * (like {@code PlatformRegistrationService}) which are currently difficult to
 * mock with WireMock alone.</li>
 * </ol>
 * <p>
 * At startup, the server automatically discovers and initializes all service mock
 * initializers (implementations of {@link ai.pipestream.wiremock.client.ServiceMockInitializer})
 * via the {@link ServiceMockRegistry}. This sets up default stubs for all registered services.
 * <p>
 * Configuration can be provided via:
 * <ul>
 *   <li>Environment variables: {@code WIREMOCK_*}</li>
 *   <li>Config file: {@code wiremock-mocks.properties} in classpath or current directory</li>
 *   <li>System properties: {@code wiremock.*}</li>
 * </ul>
 */
public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    /**
     * Starts the servers.
     *
     * @param args Command line arguments. The first argument is optional and
     *             specifies the WireMock port (default 8080).
     */
    public static void main(String[] args) {
        int port = 8080;
        int streamingPort = 50052; // Separate port for the custom streaming server

        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                LOGGER.log(Level.WARNING, "Invalid port number in arguments, using default {0}", port);
            }
        }

        // #region agent log
        if (isDebugEnabled()) {
            writeDebugLog("A", "Main.java:startup", "run-pre", "Starting wiremock-main", java.util.Map.of(
                    "port", port,
                    "streamingPort", streamingPort,
                    "javaVersion", System.getProperty("java.version"),
                    "classpath", System.getProperty("java.class.path", "")
            ));
            writeDebugLog("A", "Main.java:files", "run-pre", "Listing /deployments", listDeployments());
        }
        // #endregion

        LOGGER.info("Starting WireMock Server with gRPC extension on port " + port);

            WireMockConfiguration config = wireMockConfig()
                    .port(port)
                    .bindAddress("0.0.0.0") // Bind to all interfaces for container deployment
                    // Essential: Allow extension to find descriptor files in the classpath or
                    // filesystem
                    .usingFilesUnderClasspath(".") // Force classpath loading from root
                    .extensions(new GrpcExtensionFactory());

        WireMockServer server = new WireMockServer(config);
        server.start();
        LOGGER.info("WireMock Server started.");
        
        // Debug: List stub mappings
        server.getStubMappings().forEach(stub -> 
            LOGGER.info("Loaded Stub: " + stub.getRequest().getUrlPath()));

        // Automatically discover and initialize all service mock initializers
        WireMock wireMock = new WireMock(server.port());
        ServiceMockRegistry registry = new ServiceMockRegistry();
        
        if (registry.getInitializerCount() > 0) {
            LOGGER.info("Initializing " + registry.getInitializerCount() + " service mock(s)...");
            registry.initializeAll(wireMock);
            LOGGER.info("Service mocks initialized. Services: " + String.join(", ", registry.getServiceNames()));
        } else {
            LOGGER.info("No service mock initializers found. Server running with no default stubs.");
        }

        // Start the DirectWireMockGrpcServer for streaming capabilities
        LOGGER.info("Starting Direct Streaming gRPC Server on port " + streamingPort);
        DirectWireMockGrpcServer streamingServer = new DirectWireMockGrpcServer(server, streamingPort);
        try {
            streamingServer.start();
            LOGGER.info("Direct Streaming gRPC Server started.");
        } catch (java.io.IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to start Direct Streaming gRPC Server: " + e.getMessage(), e);
            server.stop();
            System.exit(1);
        }

        // Keep the process alive
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Stopping Servers...");
            try {
                streamingServer.stop();
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "Interrupted while stopping Direct Streaming gRPC Server", e);
                Thread.currentThread().interrupt();
            }
            server.stop();
            LOGGER.info("Servers stopped.");
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, "Main thread interrupted; shutting down", e);
            Thread.currentThread().interrupt();
        }
    }

    // #region agent log
    private static boolean isDebugEnabled() {
        return Boolean.parseBoolean(System.getProperty("wiremock.debug", "false"));
    }

    @SuppressWarnings("SameParameterValue")
    private static void writeDebugLog(String hypothesisId, String location, String runId, String message, java.util.Map<String, Object> data) {
        try (java.io.FileWriter fw = new java.io.FileWriter("/home/krickert/IdeaProjects/.cursor/debug.log", true)) {
            long ts = System.currentTimeMillis();
            String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(java.util.Map.of(
                    "id", "log_" + ts,
                    "timestamp", ts,
                    "location", location,
                    "message", message,
                    "data", data,
                    "sessionId", "debug-session",
                    "runId", runId,
                    "hypothesisId", hypothesisId
            ));
            fw.write(json + "\n");
        } catch (Exception ignored) {
            // Debug logging is optional; fail silently if Jackson is not available
        }
    }

    private static java.util.Map<String, Object> listDeployments() {
        java.io.File dir = new java.io.File("/deployments");
        String[] files = dir.list();
        return java.util.Map.of(
                "exists", dir.exists(),
                "files", files == null ? java.util.List.of() : java.util.List.of(files)
        );
    }
    // #endregion
}
