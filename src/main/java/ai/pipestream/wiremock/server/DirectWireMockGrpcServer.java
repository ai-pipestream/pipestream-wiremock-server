package ai.pipestream.wiremock.server;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.*;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.StreamObserver;
import io.grpc.health.v1.*;
import ai.pipestream.platform.registration.v1.*;
import com.google.protobuf.Timestamp;
import org.jboss.logging.Logger;

import ai.pipestream.repository.filesystem.upload.v1.*;
import ai.pipestream.repository.account.v1.*;
import ai.pipestream.opensearch.v1.*;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A direct gRPC server implementation that runs alongside WireMock to handle
 * streaming responses and smart mocks for complex services like OpenSearch Manager.
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
    private static final Metadata.Key<String> FORCE_ERROR_HEADER =
            Metadata.Key.of("x-force-error", Metadata.ASCII_STRING_MARSHALLER);

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
                    int delayMs = Integer.parseInt(delayStr);
                    ctx = ctx.withValue(TEST_DELAY_MS_KEY, delayMs);
                    // Honor the delay universally for every Direct gRPC service before
                    // the call is dispatched. Individual service implementations no
                    // longer need to repeat this Thread.sleep.
                    if (delayMs > 0) {
                        try {
                            LOG.debugf("Honoring x-test-delay-ms=%dms on Direct gRPC server", delayMs);
                            Thread.sleep(delayMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } catch (NumberFormatException ignored) {
                    // Ignore invalid delay values
                }
            }
            
            String forceError = headers.get(FORCE_ERROR_HEADER);
            if ("true".equals(forceError)) {
                ctx = ctx.withValue(TEST_SCENARIO_KEY, "force-error");
            }

            return Contexts.interceptCall(ctx, call, headers, next);
        }
    }

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
                .addService(new OpenSearchManagerServiceImpl())
                .addService(new HealthImpl())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build();
    }

    public DirectWireMockGrpcServer(WireMockServer wireMockServer, int grpcPort) {
        this(wireMockServer, grpcPort, DEFAULT_MAX_MESSAGE_SIZE);
    }

    public void start() throws IOException {
        grpcServer.start();
        LOG.infof("DirectWireMockGrpcServer started on port %d", grpcServer.getPort());
    }

    public void stop() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.shutdown();
            if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                grpcServer.shutdownNow();
            }
        }
    }

    public int getGrpcPort() {
        return grpcServer.getPort();
    }

    private static class NodeUploadServiceImpl extends NodeUploadServiceGrpc.NodeUploadServiceImplBase {
        @Override
        public void uploadFilesystemPipeDoc(UploadFilesystemPipeDocRequest request, StreamObserver<UploadFilesystemPipeDocResponse> responseObserver) {
            int size = request.getSerializedSize();
            String scenario = TEST_SCENARIO_KEY.get();
            String customDocId = TEST_DOC_ID_KEY.get();
            // Note: x-test-delay-ms is honored centrally by TestMetadataInterceptor.

            if ("failure".equals(scenario)) {
                responseObserver.onNext(UploadFilesystemPipeDocResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Simulated failure")
                        .build());
            } else if ("not-found".equals(scenario)) {
                responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            } else {
                String docId = customDocId != null ? customDocId : "mock-doc-" + System.currentTimeMillis();
                responseObserver.onNext(UploadFilesystemPipeDocResponse.newBuilder()
                        .setSuccess(true)
                        .setDocumentId(docId)
                        .setMessage("Direct mock upload successful (size=" + size + " bytes)")
                        .build());
            }
            responseObserver.onCompleted();
        }
    }

    private static class HealthImpl extends HealthGrpc.HealthImplBase {
        @Override
        public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
            responseObserver.onNext(HealthCheckResponse.newBuilder()
                    .setStatus(HealthCheckResponse.ServingStatus.SERVING)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void watch(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
            responseObserver.onNext(HealthCheckResponse.newBuilder()
                    .setStatus(HealthCheckResponse.ServingStatus.SERVING)
                    .build());
            // Keep the stream open
        }
    }

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
            ServiceType serviceType = request.getType();
            if (serviceType == ServiceType.SERVICE_TYPE_MODULE) {
                PlatformEventType[] modulePhases = {
                    PlatformEventType.PLATFORM_EVENT_TYPE_STARTED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_HEALTH_CHECK_CONFIGURED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY,
                    PlatformEventType.PLATFORM_EVENT_TYPE_METADATA_RETRIEVED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_SCHEMA_VALIDATED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_DATABASE_SAVED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_APICURIO_REGISTERED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED
                };
                for (PlatformEventType phase : modulePhases) {
                    responseObserver.onNext(createResponse(phase, phase.name()));
                }
            } else {
                PlatformEventType[] servicePhases = {
                    PlatformEventType.PLATFORM_EVENT_TYPE_STARTED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_HEALTH_CHECK_CONFIGURED,
                    PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY,
                    PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED
                };
                for (PlatformEventType phase : servicePhases) {
                    responseObserver.onNext(createResponse(phase, phase.name()));
                }
            }
            responseObserver.onCompleted();
        }

        @Override
        public void listServices(ListServicesRequest request, StreamObserver<ListServicesResponse> responseObserver) {
            ListServicesResponse response = buildListServicesResponse();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void watchServices(WatchServicesRequest request, StreamObserver<WatchServicesResponse> responseObserver) {
            ListServicesResponse services = buildListServicesResponse();
            WatchServicesResponse response = WatchServicesResponse.newBuilder()
                    .addAllServices(services.getServicesList())
                    .setAsOf(currentTimestamp())
                    .setTotalCount(services.getTotalCount())
                    .build();
            
            responseObserver.onNext(response);
            // We keep it open but don't send updates for now in this mock
        }

        private ListServicesResponse buildListServicesResponse() {
            int mockPort = 50052; // WireMock streaming port
            return ListServicesResponse.newBuilder()
                    .addServices(GetServiceResponse.newBuilder()
                            .setServiceName("repository-service")
                            .setServiceId("repo-1")
                            .setHost("localhost")
                            .setPort(mockPort)
                            .setIsHealthy(true)
                            .build())
                    .addServices(GetServiceResponse.newBuilder()
                            .setServiceName("account-service")
                            .setServiceId("account-1")
                            .setHost("localhost")
                            .setPort(mockPort)
                            .setIsHealthy(true)
                            .build())
                    .addServices(GetServiceResponse.newBuilder()
                            .setServiceName("opensearch-manager")
                            .setServiceId("os-1")
                            .setHost("localhost")
                            .setPort(mockPort)
                            .setIsHealthy(true)
                            .build())
                    .addServices(GetServiceResponse.newBuilder()
                            .setServiceName("platform-registration-service")
                            .setServiceId("reg-1")
                            .setHost("localhost")
                            .setPort(mockPort)
                            .setIsHealthy(true)
                            .build())
                    .setAsOf(currentTimestamp())
                    .setTotalCount(4)
                    .build();
        }

        @Override
        public void listPlatformModules(ListPlatformModulesRequest request, StreamObserver<ListPlatformModulesResponse> responseObserver) {
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
        }

        @Override
        public void unregister(UnregisterRequest request, StreamObserver<UnregisterResponse> responseObserver) {
            responseObserver.onNext(UnregisterResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Service unregistered successfully")
                    .setTimestamp(currentTimestamp())
                    .build());
            responseObserver.onCompleted();
        }
    }

    private static class OpenSearchManagerServiceImpl extends OpenSearchManagerServiceGrpc.OpenSearchManagerServiceImplBase {

        @Override
        public void indexDocument(IndexDocumentRequest request, StreamObserver<IndexDocumentResponse> responseObserver) {
            LOG.infof("DirectWireMockGrpcServer: indexDocument index=%s id=%s", request.getIndexName(), request.getDocumentId());
            
            // Handle forced failure scenarios
            String scenario = TEST_SCENARIO_KEY.get();
            if ("force-error".equals(scenario) || "fail-this-doc".equals(request.getDocumentId()) || request.getIndexName().contains("fail-this-index")) {
                responseObserver.onNext(IndexDocumentResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Forced internal error via mock trigger")
                        .build());
                responseObserver.onCompleted();
                return;
            }

            // High-fidelity contract validation
            if (!request.hasDocument()) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Request missing document").asRuntimeException());
                return;
            }

            if (request.getDocument().getOriginalDocId().isBlank() && request.getDocumentId().isBlank()) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Request missing document ID").asRuntimeException());
                return;
            }

            // Validate structured data
            for (SemanticVectorSet set : request.getDocument().getSemanticSetsList()) {
                if (set.getEmbeddingsCount() > 0) {
                    for (OpenSearchEmbedding emb : set.getEmbeddingsList()) {
                        if (emb.getVectorCount() == 0) {
                            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Embedding missing vector floats").asRuntimeException());
                            return;
                        }
                    }
                }
            }

            responseObserver.onNext(IndexDocumentResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId(request.getDocumentId().isBlank() ? request.getDocument().getOriginalDocId() : request.getDocumentId())
                    .setMessage("Indexed via WireMock High-Fidelity Mock")
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<StreamIndexDocumentsRequest> streamIndexDocuments(StreamObserver<StreamIndexDocumentsResponse> responseObserver) {
            return new StreamObserver<StreamIndexDocumentsRequest>() {
                @Override
                public void onNext(StreamIndexDocumentsRequest request) {
                    LOG.debugf("DirectWireMockGrpcServer: streamIndexDocuments index=%s id=%s", request.getIndexName(), request.getDocumentId());
                    
                    String scenario = TEST_SCENARIO_KEY.get();
                    if ("force-error".equals(scenario) || "fail-this-doc".equals(request.getDocumentId()) || request.getIndexName().contains("fail-this-index")) {
                        responseObserver.onNext(StreamIndexDocumentsResponse.newBuilder()
                                .setRequestId(request.getRequestId())
                                .setSuccess(false)
                                .setMessage("Forced internal error via mock trigger")
                                .build());
                        return;
                    }

                    if (!request.hasDocument()) {
                        responseObserver.onNext(StreamIndexDocumentsResponse.newBuilder()
                                .setRequestId(request.getRequestId())
                                .setSuccess(false)
                                .setMessage("Request missing document")
                                .build());
                        return;
                    }

                    if (request.getDocument().getOriginalDocId().isBlank() && request.getDocumentId().isBlank()) {
                        responseObserver.onNext(StreamIndexDocumentsResponse.newBuilder()
                                .setRequestId(request.getRequestId())
                                .setSuccess(false)
                                .setMessage("Request missing document ID")
                                .build());
                        return;
                    }

                    for (SemanticVectorSet set : request.getDocument().getSemanticSetsList()) {
                        if (set.getEmbeddingsCount() > 0) {
                            for (OpenSearchEmbedding emb : set.getEmbeddingsList()) {
                                if (emb.getVectorCount() == 0) {
                                    responseObserver.onNext(StreamIndexDocumentsResponse.newBuilder()
                                            .setRequestId(request.getRequestId())
                                            .setSuccess(false)
                                            .setMessage("Embedding missing vector floats")
                                            .build());
                                    return;
                                }
                            }
                        }
                    }

                    responseObserver.onNext(StreamIndexDocumentsResponse.newBuilder()
                            .setRequestId(request.getRequestId())
                            .setSuccess(true)
                            .setDocumentId(request.getDocumentId().isBlank() ? request.getDocument().getOriginalDocId() : request.getDocumentId())
                            .setMessage("Streamed via WireMock High-Fidelity Mock")
                            .build());
                }

                @Override
                public void onError(Throwable t) {
                    LOG.error("Error in streamIndexDocuments from client", t);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void indexAnyDocument(IndexAnyDocumentRequest request, StreamObserver<IndexAnyDocumentResponse> responseObserver) {
            responseObserver.onNext(IndexAnyDocumentResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("AnyDocument received by WireMock High-Fidelity Mock")
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void createIndex(CreateIndexRequest request, StreamObserver<CreateIndexResponse> responseObserver) {
            responseObserver.onNext(CreateIndexResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void indexExists(IndexExistsRequest request, StreamObserver<IndexExistsResponse> responseObserver) {
            responseObserver.onNext(IndexExistsResponse.newBuilder().setExists(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void searchFilesystemMeta(SearchFilesystemMetaRequest request, StreamObserver<SearchFilesystemMetaResponse> responseObserver) {
            responseObserver.onNext(SearchFilesystemMetaResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void ensureNestedEmbeddingsFieldExists(ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest request,
                StreamObserver<ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse> responseObserver) {
            responseObserver.onNext(ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse.newBuilder()
                    .setSchemaExisted(false)
                    .build());
            responseObserver.onCompleted();
        }
    }

    private static class AccountServiceStreamingImpl extends AccountServiceGrpc.AccountServiceImplBase {

        private static final String NOT_FOUND_ID = System.getenv().getOrDefault(
                "WIREMOCK_ACCOUNT_GETACCOUNT_NOTFOUND_ID", "nonexistent");

        private static final Map<String, Account> ACCOUNT_FIXTURES;

        static {
            Timestamp ts = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();
            ACCOUNT_FIXTURES = Map.of(
                    "default-account", Account.newBuilder().setAccountId("default-account").setName("Default Account").setActive(true).setCreatedAt(ts).setUpdatedAt(ts).build(),
                    "valid-account", Account.newBuilder().setAccountId("valid-account").setName("Valid Account").setActive(true).setCreatedAt(ts).setUpdatedAt(ts).build(),
                    "inactive-account", Account.newBuilder().setAccountId("inactive-account").setName("Inactive Account").setActive(false).setCreatedAt(ts).setUpdatedAt(ts).build()
            );
        }

        @Override
        public void getAccount(GetAccountRequest request, StreamObserver<GetAccountResponse> responseObserver) {
            String accountId = request.getAccountId();

            if (NOT_FOUND_ID.equals(accountId)) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Account not found: " + accountId)
                        .asRuntimeException());
                return;
            }

            Account fixture = ACCOUNT_FIXTURES.get(accountId);
            if (fixture != null) {
                responseObserver.onNext(GetAccountResponse.newBuilder().setAccount(fixture).build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Account not found: " + accountId)
                        .asRuntimeException());
            }
        }

        @Override
        public void streamAllAccounts(StreamAllAccountsRequest request, StreamObserver<StreamAllAccountsResponse> responseObserver) {
            boolean includeInactive = request.getIncludeInactive();
            Timestamp ts = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

            Account[] mockAccounts = {
                Account.newBuilder().setAccountId("default-account").setName("Default Account").setActive(true).setCreatedAt(ts).setUpdatedAt(ts).build(),
                Account.newBuilder().setAccountId("valid-account").setName("Valid Account").setActive(true).setCreatedAt(ts).setUpdatedAt(ts).build(),
                Account.newBuilder().setAccountId("inactive-account").setName("Inactive Account").setActive(false).setCreatedAt(ts).setUpdatedAt(ts).build()
            };

            for (Account account : mockAccounts) {
                if (!includeInactive && !account.getActive()) {
                    continue;
                }
                responseObserver.onNext(StreamAllAccountsResponse.newBuilder().setAccount(account).build());
            }
            responseObserver.onCompleted();
        }
    }
}
