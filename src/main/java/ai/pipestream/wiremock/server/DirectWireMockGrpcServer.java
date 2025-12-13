package ai.pipestream.wiremock.server;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import ai.pipestream.platform.registration.v1.PlatformRegistrationServiceGrpc;
import ai.pipestream.platform.registration.v1.RegistrationEvent;
import ai.pipestream.platform.registration.v1.EventType;
import ai.pipestream.platform.registration.v1.RegisterRequest;
import ai.pipestream.platform.registration.v1.RegisterResponse;
import ai.pipestream.platform.registration.v1.ServiceType;
import ai.pipestream.platform.registration.v1.ListServicesRequest;
import ai.pipestream.platform.registration.v1.ListServicesResponse;
import ai.pipestream.platform.registration.v1.ListModulesRequest;
import ai.pipestream.platform.registration.v1.ListModulesResponse;
import ai.pipestream.platform.registration.v1.GetServiceResponse;
import ai.pipestream.platform.registration.v1.GetModuleResponse;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.pipestream.repository.v1.filesystem.upload.NodeUploadServiceGrpc;
import ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocRequest;
import ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocResponse;

import java.io.IOException;
import java.time.Instant;
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

    private final Server grpcServer;

    /**
     * Construct a DirectWireMockGrpcServer.
     *
     * @param wireMockServer        The existing WireMock server instance (unused currently but kept for potential future linkage).
     * @param grpcPort              The port to bind the streaming gRPC server to.
     * @param maxInboundMessageSize Max inbound message size in bytes.
     */
    public DirectWireMockGrpcServer(WireMockServer wireMockServer, int grpcPort, int maxInboundMessageSize) {
        // wireMockServer unused in current implementation

        // Create gRPC server
        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .maxInboundMessageSize(maxInboundMessageSize)
                .addService(new PlatformRegistrationServiceImpl())
                .addService(new NodeUploadServiceImpl()) // Add the performance stub
                .build();
    }

    // Overload for backward compatibility (defaults to 500MB)
    public DirectWireMockGrpcServer(WireMockServer wireMockServer, int grpcPort) {
        this(wireMockServer, grpcPort, 500 * 1024 * 1024);
    }

    public void start() throws IOException {
        grpcServer.start();
        LOG.info("DirectWireMockGrpcServer: gRPC server started on port {}", grpcServer.getPort());
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
     * High-performance "Black Hole" implementation of NodeUploadService.
     * <p>
     * Used for benchmarking throughput without the overhead of WireMock's JSON conversion.
     * It accepts large payloads (e.g., 250MB) and returns success immediately.
     */
    private static class NodeUploadServiceImpl extends NodeUploadServiceGrpc.NodeUploadServiceImplBase {
        @Override
        public void uploadPipeDoc(UploadPipeDocRequest request, StreamObserver<UploadPipeDocResponse> responseObserver) {
            // "Black hole" - simply acknowledge receipt
            // The request payload has already been deserialized by gRPC (Netty), which is unavoidable,
            // but we avoid the WireMock JSON serialization step.
            
            // Optional: Log size for debug (might impact perf if logging full object)
            int size = request.getSerializedSize();
            if (LOG.isDebugEnabled()) {
                LOG.debug("DirectWireMock: Received uploadPipeDoc, size=" + size + " bytes");
            }

            UploadPipeDocResponse response = UploadPipeDocResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId("perf-mock-doc-" + System.currentTimeMillis())
                    .setMessage("Direct mock upload successful (size=" + size + ")")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
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
    private static class PlatformRegistrationServiceImpl extends PlatformRegistrationServiceGrpc.PlatformRegistrationServiceImplBase {

        private Timestamp currentTimestamp() {
            Instant now = Instant.now();
            return Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();
        }

        private RegisterResponse createResponse(EventType eventType, String message) {
            return RegisterResponse.newBuilder()
                    .setEvent(RegistrationEvent.newBuilder()
                            .setEventType(eventType)
                            .setTimestamp(currentTimestamp())
                            .setMessage(message)
                            .build())
                    .build();
        }

        @Override
        public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
            String name = request.getName();
            ServiceType serviceType = request.getType();
            
            if (serviceType == ServiceType.SERVICE_TYPE_SERVICE) {
                LOG.info("DirectWireMockGrpcServer: register called for SERVICE: " + name);
                registerService(responseObserver);
            } else if (serviceType == ServiceType.SERVICE_TYPE_MODULE) {
                LOG.info("DirectWireMockGrpcServer: register called for MODULE: " + name);
                registerModule(responseObserver);
            } else {
                LOG.warn("DirectWireMockGrpcServer: Unknown service type: " + serviceType);
                responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("Unknown service type: " + serviceType)
                        .asRuntimeException());
            }
        }

        private void registerService(StreamObserver<RegisterResponse> responseObserver) {
            try {
                // Simulate the 6-phase service registration process
                LOG.info("DirectWireMockGrpcServer: Emitting STARTED event.");
                responseObserver.onNext(createResponse(
                        EventType.EVENT_TYPE_STARTED,
                        "Starting service registration"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting VALIDATED event.");
                responseObserver.onNext(createResponse(
                        EventType.EVENT_TYPE_VALIDATED,
                        "Service registration request validated"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting CONSUL_REGISTERED event.");
                responseObserver.onNext(createResponse(
                        EventType.EVENT_TYPE_CONSUL_REGISTERED,
                        "Service registered with Consul"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting HEALTH_CHECK_CONFIGURED event.");
                responseObserver.onNext(createResponse(
                        EventType.EVENT_TYPE_HEALTH_CHECK_CONFIGURED,
                        "Health check configured"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting CONSUL_HEALTHY event.");
                responseObserver.onNext(createResponse(
                        EventType.EVENT_TYPE_CONSUL_HEALTHY,
                        "Service reported healthy by Consul"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting COMPLETED event.");
                responseObserver.onNext(createResponse(
                        EventType.EVENT_TYPE_COMPLETED,
                        "Service registration completed successfully"));

                responseObserver.onCompleted();
                LOG.info("DirectWireMockGrpcServer: Streaming completed for service registration.");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("DirectWireMockGrpcServer: Streaming interrupted for service registration.", e);
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription("Interrupted").asRuntimeException());
            }
        }

        private void registerModule(StreamObserver<RegisterResponse> responseObserver) {
            try {
                EventType[] phases = {
                        EventType.EVENT_TYPE_STARTED, EventType.EVENT_TYPE_VALIDATED, EventType.EVENT_TYPE_CONSUL_REGISTERED,
                        EventType.EVENT_TYPE_HEALTH_CHECK_CONFIGURED, EventType.EVENT_TYPE_CONSUL_HEALTHY,
                        EventType.EVENT_TYPE_METADATA_RETRIEVED, EventType.EVENT_TYPE_SCHEMA_VALIDATED,
                        EventType.EVENT_TYPE_DATABASE_SAVED, EventType.EVENT_TYPE_APICURIO_REGISTERED, EventType.EVENT_TYPE_COMPLETED
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
                    responseObserver.onNext(createResponse(phases[i], messages[i]));
                    Thread.sleep(20);
                }

                responseObserver.onCompleted();
                LOG.info("DirectWireMockGrpcServer: Streaming completed for module registration.");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("DirectWireMockGrpcServer: Streaming interrupted for module registration.", e);
                responseObserver.onError(io.grpc.Status.INTERNAL.withDescription("Interrupted").asRuntimeException());
            }
        }

        @Override
        public void listServices(ListServicesRequest request, StreamObserver<ListServicesResponse> responseObserver) {
            LOG.info("DirectWireMockGrpcServer: listServices called.");
            ListServicesResponse response = ListServicesResponse.newBuilder()
                    .addServices(GetServiceResponse.newBuilder()
                            .setServiceName("repository-service")
                            .setServiceId("repo-1")
                            .setHost("localhost")
                            .setPort(8080)
                            .setVersion("1.0.0")
                            .setIsHealthy(true)
                            .build())
                    .addServices(GetServiceResponse.newBuilder()
                            .setServiceName("account-manager")
                            .setServiceId("account-1")
                            .setHost("localhost")
                            .setPort(38105)
                            .setVersion("1.0.0")
                            .setIsHealthy(true)
                            .build())
                    .setAsOf(currentTimestamp())
                    .setTotalCount(2)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            LOG.info("DirectWireMockGrpcServer: listServices completed.");
        }

        @Override
        public void listModules(ListModulesRequest request, StreamObserver<ListModulesResponse> responseObserver) {
            LOG.info("DirectWireMockGrpcServer: listModules called.");
            ListModulesResponse response = ListModulesResponse.newBuilder()
                    .addModules(GetModuleResponse.newBuilder()
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
                    .addModules(GetModuleResponse.newBuilder()
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
                    .setAsOf(currentTimestamp())
                    .setTotalCount(2)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            LOG.info("DirectWireMockGrpcServer: listModules completed.");
        }
    }
}