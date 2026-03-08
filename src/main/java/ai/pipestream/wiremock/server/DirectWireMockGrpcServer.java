package ai.pipestream.wiremock.server;

import ai.pipestream.opensearch.v1.*;
import ai.pipestream.platform.registration.v1.*;
import ai.pipestream.repository.account.v1.*;
import ai.pipestream.repository.filesystem.upload.v1.*;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest;
import ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse;
import ai.pipestream.schemamanager.v1.VectorFieldDefinition;
import ai.pipestream.wiremock.client.MockConfig;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.grpc.*;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.StreamObserver;
import org.apache.hc.core5.http.HttpHost;
import org.jboss.logging.Logger;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.*;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
                .addService(new AccountServiceImpl())
                .addService(new OpenSearchManagerServiceImpl())
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
            String customDocId = TEST_DOC_ID_KEY.get();
            String docId = customDocId != null ? customDocId : "mock-doc-" + System.currentTimeMillis();
            responseObserver.onNext(UploadFilesystemPipeDocResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId(docId)
                    .setMessage("Direct mock upload successful")
                    .build());
            responseObserver.onCompleted();
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

        private ai.pipestream.platform.registration.v1.RegisterResponse createResponse(PlatformEventType eventType, String message) {
            return ai.pipestream.platform.registration.v1.RegisterResponse.newBuilder()
                    .setEvent(RegistrationEvent.newBuilder()
                            .setEventType(eventType)
                            .setTimestamp(currentTimestamp())
                            .setMessage(message)
                            .build())
                    .build();
        }

        @Override
        public void register(RegisterRequest request, StreamObserver<ai.pipestream.platform.registration.v1.RegisterResponse> responseObserver) {
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
        public void listServices(ListServicesRequest request, StreamObserver<ai.pipestream.platform.registration.v1.ListServicesResponse> responseObserver) {
            ai.pipestream.platform.registration.v1.ListServicesResponse response = ai.pipestream.platform.registration.v1.ListServicesResponse.newBuilder()
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
                            .setHttpSchemaArtifactId("repository-http-schema")
                            .setHttpSchemaVersion("1.0.0")
                            .build())
                    .addServices(GetServiceResponse.newBuilder()
                            .setServiceName("account-manager")
                            .setServiceId("account-1")
                            .setHost("localhost")
                            .setPort(18105)
                            .setVersion("1.0.0")
                            .setIsHealthy(true)
                            .addHttpEndpoints(HttpEndpoint.newBuilder()
                                    .setScheme("http")
                                    .setHost("localhost")
                                    .setPort(18105)
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
        }

        @Override
        public void listPlatformModules(ListPlatformModulesRequest request, StreamObserver<ai.pipestream.platform.registration.v1.ListPlatformModulesResponse> responseObserver) {
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
        public void unregister(UnregisterRequest request, StreamObserver<ai.pipestream.platform.registration.v1.UnregisterResponse> responseObserver) {
            responseObserver.onNext(ai.pipestream.platform.registration.v1.UnregisterResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Success")
                    .setTimestamp(currentTimestamp())
                    .build());
            responseObserver.onCompleted();
        }
    }

    private static class OpenSearchManagerServiceImpl extends OpenSearchManagerServiceGrpc.OpenSearchManagerServiceImplBase {
        @Override
        public void indexDocument(IndexDocumentRequest request, StreamObserver<IndexDocumentResponse> responseObserver) {
            responseObserver.onNext(IndexDocumentResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId(request.getDocumentId())
                    .setMessage("Indexed via WireMock")
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void indexAnyDocument(IndexAnyDocumentRequest request, StreamObserver<IndexAnyDocumentResponse> responseObserver) {
            responseObserver.onNext(IndexAnyDocumentResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("AnyDocument received by WireMock")
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void createIndex(ai.pipestream.opensearch.v1.CreateIndexRequest request, StreamObserver<CreateIndexResponse> responseObserver) {
            responseObserver.onNext(CreateIndexResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void indexExists(ai.pipestream.opensearch.v1.IndexExistsRequest request, StreamObserver<IndexExistsResponse> responseObserver) {
            responseObserver.onNext(IndexExistsResponse.newBuilder().setExists(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void searchFilesystemMeta(ai.pipestream.opensearch.v1.SearchFilesystemMetaRequest request, StreamObserver<SearchFilesystemMetaResponse> responseObserver) {
            responseObserver.onNext(SearchFilesystemMetaResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void ensureNestedEmbeddingsFieldExists(EnsureNestedEmbeddingsFieldExistsRequest request,
                StreamObserver<EnsureNestedEmbeddingsFieldExistsResponse> responseObserver) {
            responseObserver.onNext(EnsureNestedEmbeddingsFieldExistsResponse.newBuilder().setSchemaExisted(false).build());
            responseObserver.onCompleted();
        }
    }

    private static class AccountServiceImpl extends AccountServiceGrpc.AccountServiceImplBase {
        @Override
        public void getAccount(GetAccountRequest request, StreamObserver<ai.pipestream.repository.account.v1.GetAccountResponse> responseObserver) {
            responseObserver.onNext(ai.pipestream.repository.account.v1.GetAccountResponse.newBuilder()
                    .setAccount(Account.newBuilder().setAccountId(request.getAccountId()).setName("Mock Account").setActive(true).build())
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void streamAllAccounts(StreamAllAccountsRequest request, StreamObserver<ai.pipestream.repository.account.v1.StreamAllAccountsResponse> responseObserver) {
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
