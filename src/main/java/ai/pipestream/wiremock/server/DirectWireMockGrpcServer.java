package ai.pipestream.wiremock.server;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import ai.pipestream.platform.registration.PlatformRegistrationGrpc;
import ai.pipestream.platform.registration.RegistrationEvent;
import ai.pipestream.platform.registration.EventType;
import ai.pipestream.platform.registration.ServiceRegistrationRequest;
import ai.pipestream.platform.registration.ModuleRegistrationRequest;
import ai.pipestream.platform.registration.ServiceListResponse;
import ai.pipestream.platform.registration.ModuleListResponse;
import ai.pipestream.platform.registration.ServiceDetails;
import ai.pipestream.platform.registration.ModuleDetails;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A direct gRPC server implementation that runs alongside WireMock to handle
 * streaming responses.
 * <p>
 * <b>Why this exists:</b> The standard WireMock gRPC extension is excellent for
 * unary (request-response) calls,
 * but it has limited support for complex server-side streaming scenarios where
 * we need to simulate
 * time-delayed events (like a long-running registration process).
 * <p>
 * This server binds to a separate port (default 50052) and implements the
 * {@code PlatformRegistrationService}
 * manually to provide realistic, multi-phase streaming responses for testing.
 */
public class DirectWireMockGrpcServer {

    private static final Logger LOG = LoggerFactory.getLogger(DirectWireMockGrpcServer.class);

    private final WireMockServer wireMockServer;
    private final Server grpcServer;
    private final int grpcPort;

    /**
     * Construct a DirectWireMockGrpcServer.
     *
     * @param wireMockServer The existing WireMock server instance.
     * @param grpcPort       The port to bind the streaming gRPC server to.
     */
    public DirectWireMockGrpcServer(WireMockServer wireMockServer, int grpcPort) {
        this.wireMockServer = wireMockServer;
        this.grpcPort = grpcPort;

        // Create gRPC server
        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new PlatformRegistrationServiceImpl())
                .build();
    }

    public void start() throws IOException {
        grpcServer.start();
        LOG.info("DirectWireMockGrpcServer: gRPC server started on port " + grpcServer.getPort());
    }

    public void stop() throws InterruptedException {
        LOG.info("DirectWireMockGrpcServer: Stopping gRPC server...");
        grpcServer.shutdown();

        if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
            grpcServer.shutdownNow();
            LOG.warn("DirectWireMockGrpcServer: gRPC server did not terminate gracefully.");
        }
        LOG.info("DirectWireMockGrpcServer: Server stopped.");
    }

    public int getGrpcPort() {
        return grpcServer.getPort();
    }

    /**
     * Implementation of the Platform Registration Service that uses WireMock
     * for request matching but handles streaming responses directly.
     * <p>
     * This service simulates the "Registration Lifecycle" by emitting a sequence of
     * events
     * (STARTED -> VALIDATED -> REGISTERED -> HEALTHY -> COMPLETED) with artificial
     * delays
     * to test the client's ability to handle streams.
     */
    private class PlatformRegistrationServiceImpl extends PlatformRegistrationGrpc.PlatformRegistrationImplBase {

        @Override
        public void registerService(ServiceRegistrationRequest request,
                StreamObserver<RegistrationEvent> responseObserver) {
            LOG.info("DirectWireMockGrpcServer: registerService called for: " + request.getServiceName());
            try {
                // Simulate the 6-phase service registration process
                LOG.info("DirectWireMockGrpcServer: Emitting STARTED event.");
                responseObserver.onNext(RegistrationEvent.newBuilder()
                        .setEventType(EventType.STARTED)
                        .setMessage("Starting service registration")
                        .build());

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting VALIDATED event.");
                responseObserver.onNext(RegistrationEvent.newBuilder()
                        .setEventType(EventType.VALIDATED)
                        .setMessage("Service registration request validated")
                        .build());

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting CONSUL_REGISTERED event.");
                responseObserver.onNext(RegistrationEvent.newBuilder()
                        .setEventType(EventType.CONSUL_REGISTERED)
                        .setMessage("Service registered with Consul")
                        .build());

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting HEALTH_CHECK_CONFIGURED event.");
                responseObserver.onNext(RegistrationEvent.newBuilder()
                        .setEventType(EventType.HEALTH_CHECK_CONFIGURED)
                        .setMessage("Health check configured")
                        .build());

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting CONSUL_HEALTHY event.");
                responseObserver.onNext(RegistrationEvent.newBuilder()
                        .setEventType(EventType.CONSUL_HEALTHY)
                        .setMessage("Service reported healthy by Consul")
                        .build());

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting COMPLETED event.");
                responseObserver.onNext(RegistrationEvent.newBuilder()
                        .setEventType(EventType.COMPLETED)
                        .setMessage("Service registration completed successfully")
                        .build());

                responseObserver.onCompleted();
                LOG.info("DirectWireMockGrpcServer: Streaming completed for registerService.");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("DirectWireMockGrpcServer: Streaming interrupted for registerService.", e);
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription("Interrupted").asRuntimeException());
            }
        }

        @Override
        public void registerModule(ModuleRegistrationRequest request,
                StreamObserver<RegistrationEvent> responseObserver) {
            LOG.info("DirectWireMockGrpcServer: registerModule called for: " + request.getModuleName());
            try {
                EventType[] phases = {
                        EventType.STARTED, EventType.VALIDATED, EventType.CONSUL_REGISTERED,
                        EventType.HEALTH_CHECK_CONFIGURED, EventType.CONSUL_HEALTHY,
                        EventType.METADATA_RETRIEVED, EventType.SCHEMA_VALIDATED,
                        EventType.DATABASE_SAVED, EventType.APICURIO_REGISTERED, EventType.COMPLETED
                };

                String[] messages = {
                        "Starting module registration",
                        "Module registration request validated",
                        "Module registered with Consul",
                        "Health check configured",
                        "Module reported healthy by Consul",
                        "Module metadata retrieved",
                        "Schema validated or synthesized",
                        "Module registration saved to database",
                        "Schema registered in Apicurio",
                        "Module registration completed successfully"
                };

                for (int i = 0; i < phases.length; i++) {
                    LOG.info("DirectWireMockGrpcServer: Emitting " + phases[i] + " event.");
                    responseObserver.onNext(RegistrationEvent.newBuilder()
                            .setEventType(phases[i])
                            .setMessage(messages[i])
                            .build());

                    Thread.sleep(20);
                }

                responseObserver.onCompleted();
                LOG.info("DirectWireMockGrpcServer: Streaming completed for registerModule.");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("DirectWireMockGrpcServer: Streaming interrupted for registerModule.", e);
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription("Interrupted").asRuntimeException());
            }
        }

        @Override
        public void listServices(Empty request, StreamObserver<ServiceListResponse> responseObserver) {
            LOG.info("DirectWireMockGrpcServer: listServices called.");
            ServiceListResponse response = ServiceListResponse.newBuilder()
                    .addServices(ServiceDetails.newBuilder()
                            .setServiceName("repository-service")
                            .setServiceId("repo-1")
                            .setHost("localhost")
                            .setPort(8080)
                            .setVersion("1.0.0")
                            .setIsHealthy(true)
                            .build())
                    .addServices(ServiceDetails.newBuilder()
                            .setServiceName("account-manager")
                            .setServiceId("account-1")
                            .setHost("localhost")
                            .setPort(38105)
                            .setVersion("1.0.0")
                            .setIsHealthy(true)
                            .build())
                    .setAsOf(Timestamp.getDefaultInstance())
                    .setTotalCount(2)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            LOG.info("DirectWireMockGrpcServer: listServices completed.");
        }

        @Override
        public void listModules(Empty request, StreamObserver<ModuleListResponse> responseObserver) {
            LOG.info("DirectWireMockGrpcServer: listModules called.");
            ModuleListResponse response = ModuleListResponse.newBuilder()
                    .addModules(ModuleDetails.newBuilder()
                            .setModuleName("parser")
                            .setServiceId("parser-1")
                            .setHost("localhost")
                            .setPort(8081)
                            .setVersion("1.0.0")
                            .setInputFormat("text/plain")
                            .setOutputFormat("application/json")
                            .addDocumentTypes("text")
                            .setIsHealthy(true)
                            .build())
                    .addModules(ModuleDetails.newBuilder()
                            .setModuleName("chunker")
                            .setServiceId("chunker-1")
                            .setHost("localhost")
                            .setPort(8082)
                            .setVersion("1.0.0")
                            .setInputFormat("application/json")
                            .setOutputFormat("application/json")
                            .addDocumentTypes("text")
                            .setIsHealthy(true)
                            .build())
                    .setAsOf(Timestamp.getDefaultInstance())
                    .setTotalCount(2)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            LOG.info("DirectWireMockGrpcServer: listModules completed.");
        }
    }
}