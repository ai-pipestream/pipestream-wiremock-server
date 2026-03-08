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
    private final WireMockServer wireMockServer;

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
        this.wireMockServer = wireMockServer;
        LOG.infof("Initializing DirectWireMockGrpcServer on port %d with maxInboundMessageSize: %d", grpcPort, maxInboundMessageSize);
        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .maxInboundMessageSize(maxInboundMessageSize)
                .intercept(new TestMetadataInterceptor())
                .addService(new PlatformRegistrationServiceImpl())
                .addService(new NodeUploadServiceImpl())
                .addService(new AccountServiceStreamingImpl(wireMockServer))
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
        public void uploadFilesystemPipeDoc(ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest request, StreamObserver<ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse> responseObserver) {
            String customDocId = TEST_DOC_ID_KEY.get();
            String docId = customDocId != null ? customDocId : "mock-doc-" + System.currentTimeMillis();
            responseObserver.onNext(ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse.newBuilder()
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
        public void register(ai.pipestream.platform.registration.v1.RegisterRequest request, StreamObserver<ai.pipestream.platform.registration.v1.RegisterResponse> responseObserver) {
            ServiceType serviceType = request.getType();
            LOG.infof("DirectWireMockGrpcServer: register called for: %s (%s)", request.getName(), serviceType);
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
        public void listServices(ai.pipestream.platform.registration.v1.ListServicesRequest request, StreamObserver<ai.pipestream.platform.registration.v1.ListServicesResponse> responseObserver) {
            LOG.info("DirectWireMockGrpcServer: listServices called.");
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
        public void listPlatformModules(ai.pipestream.platform.registration.v1.ListPlatformModulesRequest request, StreamObserver<ai.pipestream.platform.registration.v1.ListPlatformModulesResponse> responseObserver) {
            LOG.info("DirectWireMockGrpcServer: listPlatformModules called.");
            ai.pipestream.platform.registration.v1.ListPlatformModulesResponse response = ai.pipestream.platform.registration.v1.ListPlatformModulesResponse.newBuilder()
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
        public void unregister(ai.pipestream.platform.registration.v1.UnregisterRequest request, StreamObserver<ai.pipestream.platform.registration.v1.UnregisterResponse> responseObserver) {
            responseObserver.onNext(ai.pipestream.platform.registration.v1.UnregisterResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Success")
                    .setTimestamp(currentTimestamp())
                    .build());
            responseObserver.onCompleted();
        }
    }

    private static class OpenSearchManagerServiceImpl extends OpenSearchManagerServiceGrpc.OpenSearchManagerServiceImplBase {
        private static final String NESTED_FIELD = "embeddings";

        @Override
        public void indexDocument(ai.pipestream.opensearch.v1.IndexDocumentRequest request, StreamObserver<ai.pipestream.opensearch.v1.IndexDocumentResponse> responseObserver) {
            LOG.infof("DirectWireMockGrpcServer: indexDocument index=%s id=%s", request.getIndexName(), request.getDocumentId());
            try {
                String hosts = System.getenv("OPENSEARCH_HOSTS");
                if (hosts != null && !hosts.isBlank()) {
                    String jsonDoc = JsonFormat.printer().print(request.getDocument());
                    indexToOpenSearch(request.getIndexName(), request.getDocumentId(), jsonDoc, request.getRouting(), hosts);
                    LOG.infof("DirectWireMockGrpcServer: successfully proxied document %s to OpenSearch", request.getDocumentId());
                }
                
                responseObserver.onNext(ai.pipestream.opensearch.v1.IndexDocumentResponse.newBuilder()
                        .setSuccess(true)
                        .setDocumentId(request.getDocumentId())
                        .setMessage("Indexed via WireMock Smart Proxy")
                        .build());
            } catch (Exception e) {
                LOG.errorf(e, "Failed to index document in WireMock Proxy");
                responseObserver.onNext(ai.pipestream.opensearch.v1.IndexDocumentResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage(e.getMessage())
                        .build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void indexAnyDocument(ai.pipestream.opensearch.v1.IndexAnyDocumentRequest request, StreamObserver<ai.pipestream.opensearch.v1.IndexAnyDocumentResponse> responseObserver) {
            LOG.infof("DirectWireMockGrpcServer: indexAnyDocument index=%s", request.getIndexName());
            responseObserver.onNext(ai.pipestream.opensearch.v1.IndexAnyDocumentResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("AnyDocument received by WireMock Smart Proxy")
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void createIndex(ai.pipestream.opensearch.v1.CreateIndexRequest request, StreamObserver<ai.pipestream.opensearch.v1.CreateIndexResponse> responseObserver) {
            responseObserver.onNext(ai.pipestream.opensearch.v1.CreateIndexResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void indexExists(ai.pipestream.opensearch.v1.IndexExistsRequest request, StreamObserver<ai.pipestream.opensearch.v1.IndexExistsResponse> responseObserver) {
            responseObserver.onNext(ai.pipestream.opensearch.v1.IndexExistsResponse.newBuilder().setExists(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void searchFilesystemMeta(ai.pipestream.opensearch.v1.SearchFilesystemMetaRequest request, StreamObserver<ai.pipestream.opensearch.v1.SearchFilesystemMetaResponse> responseObserver) {
            responseObserver.onNext(ai.pipestream.opensearch.v1.SearchFilesystemMetaResponse.newBuilder().setTotalCount(0).build());
            responseObserver.onCompleted();
        }

        @Override
        public void ensureNestedEmbeddingsFieldExists(ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest request,
                StreamObserver<ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse> responseObserver) {
            LOG.infof("DirectWireMockGrpcServer: ensureNestedEmbeddingsFieldExists index=%s field=%s",
                    request.getIndexName(), request.getNestedFieldName());
            try {
                createIndexIfNeeded(request);
                responseObserver.onNext(ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsResponse.newBuilder()
                        .setSchemaExisted(false)
                        .build());
            } catch (Exception e) {
                LOG.errorf(e, "Failed to ensure nested embeddings field");
                responseObserver.onError(Status.INTERNAL.withCause(e).asRuntimeException());
                return;
            }
            responseObserver.onCompleted();
        }

        private void indexToOpenSearch(String index, String id, String json, String routing, String hosts) throws IOException {
            HttpHost[] httpHosts = parseHttpHosts(hosts);
            var transport = ApacheHttpClient5TransportBuilder.builder(httpHosts)
                    .setMapper(new JacksonJsonpMapper())
                    .build();
            OpenSearchClient client = new OpenSearchClient(transport);
            client.index(i -> {
                i.index(index).document(json);
                if (id != null && !id.isBlank()) i.id(id);
                if (routing != null && !routing.isBlank()) i.routing(routing);
                return i;
            });
        }

        private void createIndexIfNeeded(EnsureNestedEmbeddingsFieldExistsRequest request) throws IOException {
            String hosts = System.getenv("OPENSEARCH_HOSTS");
            if (hosts == null || hosts.isBlank()) return;
            
            String indexName = request.getIndexName();
            String fieldName = request.getNestedFieldName().isBlank() ? NESTED_FIELD : request.getNestedFieldName();
            int dimension = request.hasVectorFieldDefinition() ? request.getVectorFieldDefinition().getDimension() : 384;

            HttpHost[] httpHosts = parseHttpHosts(hosts);
            var transport = ApacheHttpClient5TransportBuilder.builder(httpHosts).setMapper(new JacksonJsonpMapper()).build();
            OpenSearchClient client = new OpenSearchClient(transport);

            if (!client.indices().exists(e -> e.index(indexName)).value()) {
                KnnVectorProperty knnVector = KnnVectorProperty.of(k -> k.dimension(dimension));
                TypeMapping mapping = new TypeMapping.Builder()
                        .properties(fieldName, Property.of(p -> p.nested(NestedProperty.of(n -> n.properties(Map.of("vector", Property.of(v -> v.knnVector(knnVector))))))))
                        .build();
                client.indices().create(c -> c.index(indexName).settings(s -> s.knn(true)).mappings(mapping));
            }
        }

        private HttpHost[] parseHttpHosts(String hosts) {
            String[] parts = hosts.split(",");
            HttpHost[] result = new HttpHost[parts.length];
            for (int i = 0; i < parts.length; i++) {
                URI uri = URI.create(parts[i].trim().startsWith("http") ? parts[i].trim() : "http://" + parts[i].trim());
                result[i] = new HttpHost(uri.getScheme() != null ? uri.getScheme() : "http", uri.getHost(), uri.getPort() > 0 ? uri.getPort() : 9200);
            }
            return result;
        }
    }

    private static class AccountServiceStreamingImpl extends AccountServiceGrpc.AccountServiceImplBase {
        private final WireMockServer wireMockServer;

        public AccountServiceStreamingImpl(WireMockServer wireMockServer) {
            this.wireMockServer = wireMockServer;
        }

        @Override
        public void getAccount(ai.pipestream.repository.account.v1.GetAccountRequest request, StreamObserver<ai.pipestream.repository.account.v1.GetAccountResponse> responseObserver) {
            responseObserver.onNext(ai.pipestream.repository.account.v1.GetAccountResponse.newBuilder()
                    .setAccount(Account.newBuilder().setAccountId(request.getAccountId()).setName("Mock Account").setActive(true).build())
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void streamAllAccounts(ai.pipestream.repository.account.v1.StreamAllAccountsRequest request, StreamObserver<ai.pipestream.repository.account.v1.StreamAllAccountsResponse> responseObserver) {
            String scenario = TEST_SCENARIO_KEY.get();
            MockConfig config = new MockConfig();
            
            boolean includeInactive = request.getIncludeInactive();
            List<Account> all = getMockAccounts(scenario, config);

            for (Account account : all) {
                if (!includeInactive && !account.getActive()) {
                    continue;
                }
                responseObserver.onNext(StreamAllAccountsResponse.newBuilder().setAccount(account).build());
            }
            responseObserver.onCompleted();
        }

        private List<Account> getMockAccounts(String scenario, MockConfig config) {
            List<Account> accounts = new ArrayList<>();
            Timestamp ts = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

            accounts.add(Account.newBuilder().setAccountId("default-account").setName("Default Account").setActive(true).setCreatedAt(ts).setUpdatedAt(ts).build());
            accounts.add(Account.newBuilder().setAccountId("valid-account").setName("Valid Account").setActive(true).setCreatedAt(ts).setUpdatedAt(ts).build());
            accounts.add(Account.newBuilder().setAccountId("inactive-account").setName("Inactive Account").setActive(false).setCreatedAt(ts).setUpdatedAt(ts).build());
            
            return accounts;
        }
    }
}
