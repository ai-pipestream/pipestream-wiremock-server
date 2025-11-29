package ai.pipestream.wiremock.server;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.wiremock.grpc.GrpcExtensionFactory;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

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
 */
public class Main {

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
                System.err.println("Invalid port number, using default " + port);
            }
        }

        System.out.println("Starting WireMock Server with gRPC extension on port " + port);

        WireMockConfiguration config = wireMockConfig()
                .port(port)
                // Essential: Allow extension to find descriptor files in the classpath or
                // filesystem
                .usingFilesUnderClasspath("META-INF")
                .extensions(new GrpcExtensionFactory());

        WireMockServer server = new WireMockServer(config);
        server.start();
        System.out.println("WireMock Server started.");

        // Start the DirectWireMockGrpcServer for streaming capabilities
        System.out.println("Starting Direct Streaming gRPC Server on port " + streamingPort);
        DirectWireMockGrpcServer streamingServer = new DirectWireMockGrpcServer(server, streamingPort);
        try {
            streamingServer.start();
            System.out.println("Direct Streaming gRPC Server started.");
        } catch (java.io.IOException e) {
            System.err.println("Failed to start Direct Streaming gRPC Server: " + e.getMessage());
            e.printStackTrace();
            server.stop();
            System.exit(1);
        }

        // Keep the process alive
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping Servers...");
            try {
                streamingServer.stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            server.stop();
            System.out.println("Servers stopped.");
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
