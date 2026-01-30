package ai.pipestream.wiremock.server;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.StreamObserver;
import ai.pipestream.platform.registration.v1.PlatformRegistrationServiceGrpc;
import ai.pipestream.platform.registration.v1.RegistrationEvent;
import ai.pipestream.platform.registration.v1.PlatformEventType;
import ai.pipestream.platform.registration.v1.RegisterRequest;
import ai.pipestream.platform.registration.v1.RegisterResponse;
import ai.pipestream.platform.registration.v1.ServiceType;
import ai.pipestream.platform.registration.v1.HttpEndpoint;
import ai.pipestream.platform.registration.v1.ListServicesRequest;
import ai.pipestream.platform.registration.v1.ListServicesResponse;
import ai.pipestream.platform.registration.v1.ListPlatformModulesRequest;
import ai.pipestream.platform.registration.v1.ListPlatformModulesResponse;
import ai.pipestream.platform.registration.v1.GetServiceResponse;
import ai.pipestream.platform.registration.v1.GetModuleResponse;
import ai.pipestream.platform.registration.v1.UnregisterRequest;
import ai.pipestream.platform.registration.v1.UnregisterResponse;
import com.google.protobuf.Timestamp;
import org.jboss.logging.Logger;

import ai.pipestream.repository.filesystem.upload.v1.NodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse;

import ai.pipestream.repository.account.v1.Account;
import ai.pipestream.repository.account.v1.AccountServiceGrpc;
import ai.pipestream.repository.account.v1.GetAccountRequest;
import ai.pipestream.repository.account.v1.GetAccountResponse;
import ai.pipestream.repository.account.v1.StreamAllAccountsRequest;
import ai.pipestream.repository.account.v1.StreamAllAccountsResponse;
import ai.pipestream.wiremock.client.MockConfig;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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

    private static final Logger LOG = Logger.getLogger(DirectWireMockGrpcServer.class);

    private final Server grpcServer;

    public static final int DEFAULT_MAX_MESSAGE_SIZE = Integer.MAX_VALUE;

    // Context keys for test metadata routing
    public static final Context.Key<String> TEST_SCENARIO_KEY = Context.key("test-scenario");
    public static final Context.Key<String> TEST_DOC_ID_KEY = Context.key("test-doc-id");
    public static final Context.Key<Integer> TEST_DELAY_MS_KEY = Context.key("test-delay-ms");

    // Metadata keys that clients send as headers
    private static final Metadata.Key<String> SCENARIO_HEADER =
            Metadata.Key.of("x-test-scenario", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DOC_ID_HEADER =
            Metadata.Key.of("x-test-doc-id", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DELAY_MS_HEADER =
            Metadata.Key.of("x-test-delay-ms", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Interceptor that extracts test routing metadata from gRPC headers
     * and attaches them to the gRPC Context for use in service implementations.
     */
    private static class TestMetadataInterceptor implements ServerInterceptor {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {

            Context ctx = Context.current();

            String scenario = headers.get(SCENARIO_HEADER);
            if (scenario != null) {
                ctx = ctx.withValue(TEST_SCENARIO_KEY, scenario);
                LOG.debugf("Test scenario header: %s", scenario);
            }

            String docId = headers.get(DOC_ID_HEADER);
            if (docId != null) {
                ctx = ctx.withValue(TEST_DOC_ID_KEY, docId);
            }

            String delayStr = headers.get(DELAY_MS_HEADER);
            if (delayStr != null) {
                try {
                    ctx = ctx.withValue(TEST_DELAY_MS_KEY, Integer.parseInt(delayStr));
                } catch (NumberFormatException ignored) {
                    // Ignore invalid delay values
                }
            }

            return Contexts.interceptCall(ctx, call, headers, next);
        }
    }

    /**
     * Construct a DirectWireMockGrpcServer.
     *
     * @param wireMockServer        The existing WireMock server instance.
     * @param grpcPort              The port to bind the streaming gRPC server to.
     * @param maxInboundMessageSize Max inbound message size in bytes.
     */
    public DirectWireMockGrpcServer(WireMockServer wireMockServer, int grpcPort, int maxInboundMessageSize) {
        if (wireMockServer == null) {
            throw new IllegalArgumentException("wireMockServer must not be null");
        }
        LOG.infof("Initializing DirectWireMockGrpcServer on port %d with maxInboundMessageSize: %d", grpcPort, maxInboundMessageSize);
        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .maxInboundMessageSize(maxInboundMessageSize)
                .intercept(new TestMetadataInterceptor())
                .addService(new PlatformRegistrationServiceImpl())
                .addService(new NodeUploadServiceImpl())
                .addService(new AccountServiceStreamingImpl())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build();
    }

    // Overload for backward compatibility (defaults to 2GB)
    public DirectWireMockGrpcServer(WireMockServer wireMockServer, int grpcPort) {
        this(wireMockServer, grpcPort, DEFAULT_MAX_MESSAGE_SIZE);
    }

    public void start() throws IOException {
        grpcServer.start();
        LOG.infof("DirectWireMockGrpcServer: gRPC server started on port %d", grpcServer.getPort());
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
     * High-performance implementation of NodeUploadService with test routing support.
     * <p>
     * Supports metadata-based routing via gRPC headers:
     * <ul>
     *   <li>{@code x-test-scenario}: Controls behavior (success, failure, timeout, etc.)</li>
     *   <li>{@code x-test-doc-id}: Returns a specific document ID</li>
     *   <li>{@code x-test-delay-ms}: Adds artificial delay in milliseconds</li>
     * </ul>
     * <p>
     * Supported scenarios:
     * <ul>
     *   <li>{@code success} (default): Returns success with generated doc ID</li>
     *   <li>{@code failure}: Returns failure with error message</li>
     *   <li>{@code not-found}: Returns GRPC NOT_FOUND status</li>
     *   <li>{@code unavailable}: Returns GRPC UNAVAILABLE status</li>
     *   <li>{@code echo-size}: Returns success with size info in message</li>
     * </ul>
     */
    private static class NodeUploadServiceImpl extends NodeUploadServiceGrpc.NodeUploadServiceImplBase {
        @Override
        public void uploadFilesystemPipeDoc(UploadFilesystemPipeDocRequest request, StreamObserver<UploadFilesystemPipeDocResponse> responseObserver) {
            int size = request.getSerializedSize();
            String scenario = TEST_SCENARIO_KEY.get();
            String customDocId = TEST_DOC_ID_KEY.get();
            Integer delayMs = TEST_DELAY_MS_KEY.get();

            if (LOG.isDebugEnabled()) {
                LOG.debugf("DirectWireMock: uploadFilesystemPipeDoc size=%d bytes, scenario=%s, docId=%s, delay=%s",
                        size, scenario, customDocId, delayMs);
            }

            // Apply artificial delay if specified
            if (delayMs != null && delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    responseObserver.onError(io.grpc.Status.CANCELLED
                            .withDescription("Interrupted during delay")
                            .asRuntimeException());
                    return;
                }
            }

            // Route based on scenario
            if (scenario == null || scenario.isEmpty() || "success".equals(scenario)) {
                // Default success scenario
                String docId = customDocId != null ? customDocId : "mock-doc-" + System.currentTimeMillis();
                UploadFilesystemPipeDocResponse response = UploadFilesystemPipeDocResponse.newBuilder()
                        .setSuccess(true)
                        .setDocumentId(docId)
                        .setMessage("Direct mock upload successful (size=" + size + " bytes)")
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } else if ("failure".equals(scenario)) {
                // Simulated failure
                UploadFilesystemPipeDocResponse response = UploadFilesystemPipeDocResponse.newBuilder()
                        .setSuccess(false)
                        .setDocumentId("")
                        .setMessage("Simulated upload failure for testing")
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } else if ("not-found".equals(scenario)) {
                // gRPC NOT_FOUND status
                responseObserver.onError(io.grpc.Status.NOT_FOUND
                        .withDescription("Simulated NOT_FOUND for testing")
                        .asRuntimeException());

            } else if ("unavailable".equals(scenario)) {
                // gRPC UNAVAILABLE status (simulates service down)
                responseObserver.onError(io.grpc.Status.UNAVAILABLE
                        .withDescription("Simulated service unavailable for testing")
                        .asRuntimeException());

            } else if ("echo-size".equals(scenario)) {
                // Echo request details
                String docId = customDocId != null ? customDocId : "echo-" + System.currentTimeMillis();
                String docType = request.hasDocument() ? request.getDocument().getClass().getSimpleName() : "none";
                UploadFilesystemPipeDocResponse response = UploadFilesystemPipeDocResponse.newBuilder()
                        .setSuccess(true)
                        .setDocumentId(docId)
                        .setMessage(String.format("Echo: size=%d bytes, docType=%s", size, docType))
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } else {
                // Unknown scenario - treat as success but log warning
                LOG.warnf("Unknown test scenario: '%s', treating as success", scenario);
                String docId = customDocId != null ? customDocId : "unknown-scenario-" + System.currentTimeMillis();
                UploadFilesystemPipeDocResponse response = UploadFilesystemPipeDocResponse.newBuilder()
                        .setSuccess(true)
                        .setDocumentId(docId)
                        .setMessage("Unknown scenario '" + scenario + "', defaulted to success")
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
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

        private RegisterResponse createResponse(PlatformEventType eventType, String message) {
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
            
            if (serviceType == ServiceType.SERVICE_TYPE_SERVICE || serviceType == ServiceType.SERVICE_TYPE_CONNECTOR) {
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
                        PlatformEventType.PLATFORM_EVENT_TYPE_STARTED,
                        "Starting service registration"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting VALIDATED event.");
                responseObserver.onNext(createResponse(
                        PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED,
                        "Service registration request validated"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting CONSUL_REGISTERED event.");
                responseObserver.onNext(createResponse(
                        PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED,
                        "Service registered with Consul"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting HEALTH_CHECK_CONFIGURED event.");
                responseObserver.onNext(createResponse(
                        PlatformEventType.PLATFORM_EVENT_TYPE_HEALTH_CHECK_CONFIGURED,
                        "Health check configured"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting CONSUL_HEALTHY event.");
                responseObserver.onNext(createResponse(
                        PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY,
                        "Service reported healthy by Consul"));

                Thread.sleep(50);

                LOG.info("DirectWireMockGrpcServer: Emitting COMPLETED event.");
                responseObserver.onNext(createResponse(
                        PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED,
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
                PlatformEventType[] phases = {
                        PlatformEventType.PLATFORM_EVENT_TYPE_STARTED, PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED, PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED,
                        PlatformEventType.PLATFORM_EVENT_TYPE_HEALTH_CHECK_CONFIGURED, PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY,
                        PlatformEventType.PLATFORM_EVENT_TYPE_METADATA_RETRIEVED, PlatformEventType.PLATFORM_EVENT_TYPE_SCHEMA_VALIDATED,
                        PlatformEventType.PLATFORM_EVENT_TYPE_DATABASE_SAVED, PlatformEventType.PLATFORM_EVENT_TYPE_APICURIO_REGISTERED, PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED
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
                            .setServiceName("repository")
                            .setServiceId("repo-1")
                            .setHost("localhost")
                            .setPort(8080)
                            .setVersion("1.0.0")
                            .setIsHealthy(true)
                            .addHttpEndpoints(HttpEndpoint.newBuilder()
                                    .setScheme("http")
                                    .setHost("localhost")
                                    .setPort(8080)
                                    .setBasePath("/repository")
                                    .setHealthPath("/q/health")
                                    .setTlsEnabled(false)
                                    .build())
                            .setHttpSchemaArtifactId("repository-service-http-schema")
                            .setHttpSchemaVersion("1.0.0")
                            .build())
                    .addServices(GetServiceResponse.newBuilder()
                            .setServiceName("account-manager")
                            .setServiceId("account-1")
                            .setHost("localhost")
                            .setPort(38105)
                            .setVersion("1.0.0")
                            .setIsHealthy(true)
                            .addHttpEndpoints(HttpEndpoint.newBuilder()
                                    .setScheme("http")
                                    .setHost("localhost")
                                    .setPort(38105)
                                    .setBasePath("/account-manager")
                                    .setHealthPath("/q/health")
                                    .setTlsEnabled(false)
                                    .build())
                            .setHttpSchemaArtifactId("account-manager-http-schema")
                            .setHttpSchemaVersion("1.0.0")
                            .build())
                    .setAsOf(currentTimestamp())
                    .setTotalCount(2)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            LOG.info("DirectWireMockGrpcServer: listServices completed.");
        }

        @Override
        public void listPlatformModules(ListPlatformModulesRequest request, StreamObserver<ListPlatformModulesResponse> responseObserver) {
            LOG.info("DirectWireMockGrpcServer: listPlatformModules called.");
            ListPlatformModulesResponse response = ListPlatformModulesResponse.newBuilder()
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
            LOG.info("DirectWireMockGrpcServer: listPlatformModules completed.");
        }

        @Override
        public void unregister(UnregisterRequest request, StreamObserver<UnregisterResponse> responseObserver) {
            String name = request.getName();
            String host = request.getHost();
            int port = request.getPort();

            LOG.info("DirectWireMockGrpcServer: unregister called for " + name + " at " + host + ":" + port);

            UnregisterResponse response = UnregisterResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Service unregistered successfully")
                    .setTimestamp(currentTimestamp())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            LOG.info("DirectWireMockGrpcServer: unregister completed for " + name);
        }
    }

    /**
     * Streaming implementation of AccountService for bulk account listing.
     * <p>
     * WireMock's gRPC extension does not support server-side streaming well, so
     * this direct server handles the StreamAllAccounts RPC.
     */
    private static class AccountServiceStreamingImpl extends AccountServiceGrpc.AccountServiceImplBase {
        private static final String STREAM_IDS_KEY = "wiremock.account.StreamAllAccounts.ids";
        private static final String STREAM_NAMES_KEY = "wiremock.account.StreamAllAccounts.names";
        private static final String STREAM_DESCRIPTIONS_KEY = "wiremock.account.StreamAllAccounts.descriptions";
        private static final String STREAM_ACTIVES_KEY = "wiremock.account.StreamAllAccounts.actives";

        private final List<Account> streamAccounts;

        private AccountServiceStreamingImpl() {
            this.streamAccounts = buildStreamAccounts(new MockConfig());
        }

        @Override
        public void streamAllAccounts(StreamAllAccountsRequest request, StreamObserver<StreamAllAccountsResponse> responseObserver) {
            request.getQuery();
            String query = request.getQuery().trim().toLowerCase(Locale.ROOT);
            boolean includeInactive = request.getIncludeInactive();

            LOG.infof("DirectWireMockGrpcServer: streamAllAccounts called query='%s' includeInactive=%s", query, includeInactive);

            for (Account account : streamAccounts) {
                if (!includeInactive && !account.getActive()) {
                    continue;
                }
                if (!query.isEmpty() && !matchesQuery(account, query)) {
                    continue;
                }
                responseObserver.onNext(StreamAllAccountsResponse.newBuilder()
                        .setAccount(account)
                        .build());
            }

            responseObserver.onCompleted();
        }

        @Override
        public void getAccount(GetAccountRequest request, StreamObserver<GetAccountResponse> responseObserver) {
            String accountId = request.getAccountId();
            Account account = findAccount(accountId);
            if (account == null) {
                responseObserver.onError(io.grpc.Status.NOT_FOUND
                        .withDescription("Account not found: " + accountId)
                        .asRuntimeException());
                return;
            }
            responseObserver.onNext(GetAccountResponse.newBuilder()
                    .setAccount(account)
                    .build());
            responseObserver.onCompleted();
        }

        private boolean matchesQuery(Account account, String query) {
            String accountId = account.getAccountId().toLowerCase(Locale.ROOT);
            String name = account.getName().toLowerCase(Locale.ROOT);
            return accountId.contains(query) || name.contains(query);
        }

        private Account findAccount(String accountId) {
            if (accountId == null || accountId.isBlank()) {
                return null;
            }
            for (Account account : streamAccounts) {
                if (accountId.equals(account.getAccountId())) {
                    return account;
                }
            }
            return null;
        }

        private List<Account> buildStreamAccounts(MockConfig config) {
            String idsValue = getConfig(config, STREAM_IDS_KEY, "");
            if (idsValue == null || idsValue.isBlank()) {
                return defaultStreamAccounts(config);
            }

            List<String> ids = parseCsv(idsValue);
            List<String> names = parseCsv(getConfig(config, STREAM_NAMES_KEY, ""));
            List<String> descriptions = parseCsv(getConfig(config, STREAM_DESCRIPTIONS_KEY, ""));
            List<String> actives = parseCsv(getConfig(config, STREAM_ACTIVES_KEY, ""));

            List<Account> accounts = new ArrayList<>();
            Timestamp timestamp = currentTimestamp();

            for (int i = 0; i < ids.size(); i++) {
                String id = ids.get(i).trim();
                if (id.isEmpty()) {
                    continue;
                }
                String name = i < names.size() ? names.get(i).trim() : id;
                String description = i < descriptions.size() ? descriptions.get(i).trim() : "";
                boolean active = i >= actives.size() || Boolean.parseBoolean(actives.get(i).trim());

                accounts.add(Account.newBuilder()
                        .setAccountId(id)
                        .setName(name)
                        .setDescription(description)
                        .setActive(active)
                        .setCreatedAt(timestamp)
                        .setUpdatedAt(timestamp)
                        .build());
            }

            if (accounts.isEmpty()) {
                return defaultStreamAccounts(config);
            }
            return accounts;
        }

        private List<Account> defaultStreamAccounts(MockConfig config) {
            List<Account> accounts = new ArrayList<>();
            Timestamp timestamp = currentTimestamp();

            String defaultId = getConfig(config, "wiremock.account.GetAccount.default.id", "default-account");
            String defaultName = getConfig(config, "wiremock.account.GetAccount.default.name", "Default Account");
            String defaultDescription = getConfig(config, "wiremock.account.GetAccount.default.description", "Default account for testing");
            boolean defaultActive = Boolean.parseBoolean(getConfig(config, "wiremock.account.GetAccount.default.active", "true"));

            accounts.add(Account.newBuilder()
                    .setAccountId(defaultId)
                    .setName(defaultName)
                    .setDescription(defaultDescription)
                    .setActive(defaultActive)
                    .setCreatedAt(timestamp)
                    .setUpdatedAt(timestamp)
                    .build());

            accounts.add(Account.newBuilder()
                    .setAccountId("valid-account")
                    .setName("Valid Account")
                    .setDescription("Valid account for testing")
                    .setActive(true)
                    .setCreatedAt(timestamp)
                    .setUpdatedAt(timestamp)
                    .build());

            accounts.add(Account.newBuilder()
                    .setAccountId("inactive-account")
                    .setName("Inactive Account")
                    .setDescription("Inactive account for testing")
                    .setActive(false)
                    .setCreatedAt(timestamp)
                    .setUpdatedAt(timestamp)
                    .build());

            return accounts;
        }

        private List<String> parseCsv(String value) {
            if (value == null || value.isBlank()) {
                return List.of();
            }
            String[] parts = value.split(",");
            List<String> results = new ArrayList<>(parts.length);
            for (String part : parts) {
                results.add(part.trim());
            }
            return results;
        }

        private String getConfig(MockConfig config, String key, String defaultValue) {
            if (config.hasKey(key)) {
                return config.get(key, defaultValue);
            }
            String lowerKey = key.toLowerCase(Locale.ROOT);
            if (config.hasKey(lowerKey)) {
                return config.get(lowerKey, defaultValue);
            }
            return config.get(key, defaultValue);
        }

        private Timestamp currentTimestamp() {
            Instant now = Instant.now();
            return Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();
        }
    }
}
