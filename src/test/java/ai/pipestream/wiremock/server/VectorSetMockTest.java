package ai.pipestream.wiremock.server;

import ai.pipestream.opensearch.v1.ChunkerConfigServiceGrpc;
import ai.pipestream.opensearch.v1.CreateChunkerConfigRequest;
import ai.pipestream.opensearch.v1.CreateEmbeddingModelConfigRequest;
import ai.pipestream.opensearch.v1.CreateVectorSetRequest;
import ai.pipestream.opensearch.v1.CreateVectorSetResponse;
import ai.pipestream.opensearch.v1.EmbeddingConfigServiceGrpc;
import ai.pipestream.opensearch.v1.OpenSearchDocument;
import ai.pipestream.opensearch.v1.OpenSearchEmbedding;
import ai.pipestream.opensearch.v1.OpenSearchManagerServiceGrpc;
import ai.pipestream.opensearch.v1.SemanticVectorSet;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsRequest;
import ai.pipestream.opensearch.v1.StreamIndexDocumentsResponse;
import ai.pipestream.opensearch.v1.VectorSetServiceGrpc;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the in-memory {@code ChunkerConfigService /
 * EmbeddingConfigService / VectorSetService} mocks added to
 * {@link DirectWireMockGrpcServer}, plus the registration-aware
 * validation in
 * {@code OpenSearchManagerService.streamIndexDocuments}.
 *
 * <p>These tests validate the contract the deployed image enforces:
 * a streaming index request whose document carries a populated
 * {@code (source_field_name, chunk_config_id, embedding_id)} tuple
 * MUST have a corresponding {@code VectorSet} previously created AND
 * bound to the target index — otherwise the mock rejects with the
 * "VectorSet 'X' not found" / "not bound" error message format.
 *
 * <p>Sink-side test resources can therefore wire CreateVectorSet +
 * BindVectorSetToIndex into their {@code @BeforeEach} hooks against
 * this mock and rely on the same behavior the production
 * opensearch-manager would expose.
 */
class VectorSetMockTest {

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer directGrpcServer;
    private ManagedChannel channel;
    private OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub osManagerAsyncStub;
    private VectorSetServiceGrpc.VectorSetServiceBlockingStub vsBlocking;
    private ChunkerConfigServiceGrpc.ChunkerConfigServiceBlockingStub chunkerBlocking;
    private EmbeddingConfigServiceGrpc.EmbeddingConfigServiceBlockingStub embedderBlocking;

    @BeforeEach
    void setUp() throws Exception {
        MockServiceState.get().clear();
        wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
        wireMockServer.start();

        directGrpcServer = new DirectWireMockGrpcServer(wireMockServer, 0);
        directGrpcServer.start();

        channel = ManagedChannelBuilder.forAddress("localhost", directGrpcServer.getGrpcPort())
                .usePlaintext()
                .build();

        osManagerAsyncStub = OpenSearchManagerServiceGrpc.newStub(channel);
        vsBlocking = VectorSetServiceGrpc.newBlockingStub(channel);
        chunkerBlocking = ChunkerConfigServiceGrpc.newBlockingStub(channel);
        embedderBlocking = EmbeddingConfigServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (directGrpcServer != null) {
            directGrpcServer.stop();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
        MockServiceState.get().clear();
    }

    @Test
    void createChunkerConfig_returnsStoredRecord() {
        var resp = chunkerBlocking.createChunkerConfig(CreateChunkerConfigRequest.newBuilder()
                .setId("chunker-v1")
                .setName("chunker-v1")
                .build());

        assertThat(resp.getConfig().getId())
                .as("CreateChunkerConfig must echo back the supplied id")
                .isEqualTo("chunker-v1");
        assertThat(MockServiceState.get().getChunkerConfig("chunker-v1"))
                .as("CreateChunkerConfig must persist into the shared mock state")
                .isNotNull();
    }

    @Test
    void createEmbeddingModelConfig_returnsStoredRecord() {
        var resp = embedderBlocking.createEmbeddingModelConfig(CreateEmbeddingModelConfigRequest.newBuilder()
                .setId("embed-v1")
                .setName("embed-v1")
                .setModelIdentifier("test-model")
                .build());

        assertThat(resp.getConfig().getId())
                .as("CreateEmbeddingModelConfig must echo back the supplied id")
                .isEqualTo("embed-v1");
        assertThat(MockServiceState.get().getEmbeddingConfig("embed-v1"))
                .as("CreateEmbeddingModelConfig must persist into the shared mock state")
                .isNotNull();
    }

    @Test
    void createVectorSet_registersByLookupKey() {
        chunkerBlocking.createChunkerConfig(CreateChunkerConfigRequest.newBuilder()
                .setId("chunker-v1").setName("chunker-v1").build());
        embedderBlocking.createEmbeddingModelConfig(CreateEmbeddingModelConfigRequest.newBuilder()
                .setId("embed-v1").setName("embed-v1").setModelIdentifier("m").build());

        CreateVectorSetResponse resp = vsBlocking.createVectorSet(CreateVectorSetRequest.newBuilder()
                .setName("vs-body")
                .setChunkerConfigId("chunker-v1")
                .setEmbeddingModelConfigId("embed-v1")
                .setFieldName("vector")
                .setSourceField("body")
                .setIndexName("idx-test")
                .build());

        assertThat(resp.getVectorSet().getName())
                .as("CreateVectorSet must echo the supplied name")
                .isEqualTo("vs-body");
        assertThat(MockServiceState.get().getVectorSetByLookup("body", "chunker-v1", "embed-v1"))
                .as("CreateVectorSet must register under the (sourceField, chunker, embedder) lookup key the streaming validator uses")
                .isNotNull();
    }

    @Test
    void streamIndexDocuments_unregisteredSemanticTuple_returnsFailureNamingTheTuple() throws Exception {
        // Doc carries (body, chunker-v1, embed-v1) but nothing was registered.
        StreamIndexDocumentsResponse resp = streamOne(StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-unreg")
                .setIndexName("idx-test")
                .setDocument(OpenSearchDocument.newBuilder()
                        .setOriginalDocId("doc-1")
                        .addSemanticSets(SemanticVectorSet.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("chunker-v1")
                                .setEmbeddingId("embed-v1")
                                .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                        .addVector(0.1f).addVector(0.2f).addVector(0.3f).build())
                                .build())
                        .build())
                .build());

        assertThat(resp.getSuccess())
                .as("unregistered semantic tuple must be rejected by the streaming endpoint")
                .isFalse();
        assertThat(resp.getMessage())
                .as("rejection message must name the missing VectorSet lookup key in the deployed-image format")
                .contains("body_chunker_v1_embed_v1")
                .contains("not found")
                .contains("CreateVectorSet")
                .contains("BindVectorSetToIndex");
    }

    @Test
    void streamIndexDocuments_registeredAndBound_succeeds() throws Exception {
        // The proto on this branch does not split recipe and binding —
        // CreateVectorSetRequest.index_name is required and the create call
        // IS the bind. Tests that exercise "registered but unbound" cannot
        // be expressed in this proto version; once the protos repo lands
        // the recipe/binding split that test will return.
        chunkerBlocking.createChunkerConfig(CreateChunkerConfigRequest.newBuilder()
                .setId("chunker-v1").setName("chunker-v1").build());
        embedderBlocking.createEmbeddingModelConfig(CreateEmbeddingModelConfigRequest.newBuilder()
                .setId("embed-v1").setName("embed-v1").setModelIdentifier("m").build());
        vsBlocking.createVectorSet(CreateVectorSetRequest.newBuilder()
                .setName("vs-body")
                .setChunkerConfigId("chunker-v1")
                .setEmbeddingModelConfigId("embed-v1")
                .setFieldName("vector")
                .setSourceField("body")
                .setIndexName("idx-test")
                .build());

        StreamIndexDocumentsResponse resp = streamOne(StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-ok")
                .setIndexName("idx-test")
                .setDocument(OpenSearchDocument.newBuilder()
                        .setOriginalDocId("doc-1")
                        .addSemanticSets(SemanticVectorSet.newBuilder()
                                .setSourceFieldName("body")
                                .setChunkConfigId("chunker-v1")
                                .setEmbeddingId("embed-v1")
                                .addEmbeddings(OpenSearchEmbedding.newBuilder()
                                        .addVector(0.1f).addVector(0.2f).addVector(0.3f).build())
                                .build())
                        .build())
                .build());

        assertThat(resp.getSuccess())
                .as("registered and bound VectorSet must allow the streaming index doc to succeed")
                .isTrue();
        assertThat(resp.getRequestId())
                .as("streaming response must echo the inbound request_id for client correlation")
                .isEqualTo("req-ok");
        assertThat(resp.getMessage())
                .as("success message must come from the high-fidelity mock path")
                .contains("Streamed via WireMock");
    }

    @Test
    void streamIndexDocuments_docWithoutSemanticTuple_passesThrough() throws Exception {
        // Pre-existing tests that send title-only docs (no chunk/embedder tuple)
        // must keep passing — validation only triggers when the tuple is populated.
        StreamIndexDocumentsResponse resp = streamOne(StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("req-no-semantic")
                .setIndexName("idx-test")
                .setDocument(OpenSearchDocument.newBuilder()
                        .setOriginalDocId("doc-1")
                        .setTitle("title only — no semantic content")
                        .build())
                .build());

        assertThat(resp.getSuccess())
                .as("docs without a populated semantic tuple bypass VS validation")
                .isTrue();
    }

    /**
     * Bidi-streaming helper: sends a single request, awaits a single
     * response, closes the stream. Mirrors the per-call short-lived bidi
     * pattern the sink uses in production.
     */
    private StreamIndexDocumentsResponse streamOne(StreamIndexDocumentsRequest request) throws Exception {
        CompletableFuture<StreamIndexDocumentsResponse> future = new CompletableFuture<>();
        StreamObserver<StreamIndexDocumentsRequest> reqObs = osManagerAsyncStub.streamIndexDocuments(
                new StreamObserver<StreamIndexDocumentsResponse>() {
                    @Override public void onNext(StreamIndexDocumentsResponse value) { future.complete(value); }
                    @Override public void onError(Throwable t) { future.completeExceptionally(t); }
                    @Override public void onCompleted() {
                        if (!future.isDone()) {
                            future.completeExceptionally(new IllegalStateException("stream closed without response"));
                        }
                    }
                });
        reqObs.onNext(request);
        reqObs.onCompleted();
        return future.get(5, TimeUnit.SECONDS);
    }
}
