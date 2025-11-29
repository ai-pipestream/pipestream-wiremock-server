package ai.pipestream.wiremock.server;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.wiremock.grpc.GrpcExtensionFactory;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public class Main {
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
                // Essential: Allow extension to find descriptor files in the classpath or filesystem
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
