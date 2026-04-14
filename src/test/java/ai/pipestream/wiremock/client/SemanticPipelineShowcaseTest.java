package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.test.support.semantic.SemanticPipelineInvariants;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * End-to-end contract-lock showcase for the three semantic pipeline step mocks.
 *
 * <p>This test exercises the full chunker → embedder → semantic-graph path
 * by sending three consecutive {@code ProcessData} gRPC calls against a single
 * WireMock server, switching only the {@code x-module-name} gRPC metadata
 * header between calls. Each response is validated against its corresponding
 * stage invariant from {@code pipestream-test-support}.
 *
 * <h2>What this test proves</h2>
 * <ul>
 *   <li>{@link ChunkerStepMock}, {@link EmbedderStepMock}, and
 *       {@link SemanticGraphStepMock} can coexist in the same WireMock server
 *       without stub collisions (each is scoped by a distinct
 *       {@code x-module-name} value).</li>
 *   <li>Each mock's canned response passes its corresponding
 *       {@code SemanticPipelineInvariants.assertPost*} method, which is the
 *       contract lock used by the three real module refactors in Phase 3.</li>
 *   <li>The three stage fixtures built by {@link
 *       ai.pipestream.wiremock.client.semantic.SemanticFixtureBuilder} form a
 *       coherent shape chain (stage 0 → stage 1 → stage 2 → stage 3).</li>
 * </ul>
 *
 * <h2>What this test does NOT prove</h2>
 * <ul>
 *   <li>That the real {@code module-chunker}, {@code module-embedder}, and
 *       {@code module-semantic-graph} produce those fixtures. Each module's
 *       own unit tests (R1/R2/R3) prove that.</li>
 *   <li>That the real pipeline works end-to-end. That's the
 *       {@code module-testing-sidecar} R5 verification step.</li>
 * </ul>
 */
public class SemanticPipelineShowcaseTest {

    private static WireMockServer wireMockServer;
    private static ManagedChannel channel;

    @BeforeAll
    static void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new GrpcExtensionFactory())
                .withRootDirectory("build/resources/test/wiremock"));
        wireMockServer.start();

        WireMock wireMock = new WireMock(wireMockServer.port());
        new ChunkerStepMock().initializeDefaults(wireMock);
        new EmbedderStepMock().initializeDefaults(wireMock);
        new SemanticGraphStepMock().initializeDefaults(wireMock);

        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
    }

    @AfterAll
    static void tearDown() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void semanticPipelineRoundTripPassesAllThreeInvariants() {
        // Stage 1: chunker
        PipeDoc stage1 = callProcessData("chunker");

        assertThatCode(() -> SemanticPipelineInvariants.assertPostChunker(stage1))
                .as("chunker step mock response must satisfy assertPostChunker")
                .doesNotThrowAnyException();

        // Stage 2: embedder (the mock returns its canned fixture regardless of
        // the input, so passing stage1 or an empty request produces the same
        // output. The round-trip's purpose is mock wiring + fixture coherence,
        // not a real pipeline.)
        PipeDoc stage2 = callProcessData("embedder");

        assertThatCode(() -> SemanticPipelineInvariants.assertPostEmbedder(stage2))
                .as("embedder step mock response must satisfy assertPostEmbedder")
                .doesNotThrowAnyException();

        // Stage 3: semantic-graph
        PipeDoc stage3 = callProcessData("semantic-graph");

        assertThatCode(() -> SemanticPipelineInvariants.assertPostSemanticGraph(stage3))
                .as("semantic-graph step mock response must satisfy assertPostSemanticGraph")
                .doesNotThrowAnyException();
    }

    @Test
    void semanticPipelineShapeChainIsCoherent() {
        // Verify the three stages' SPRs form a coherent chain: the SPR at
        // source_field="body", chunk_config_id="sentence_v1" should exist at
        // every stage, with the embedding_config_id transitioning from empty
        // (stage 1) to populated (stage 2+).
        PipeDoc stage1 = callProcessData("chunker");
        PipeDoc stage2 = callProcessData("embedder");
        PipeDoc stage3 = callProcessData("semantic-graph");

        // Stage 1: one SPR, embedding_config_id empty
        assertThat(stage1.getSearchMetadata().getSemanticResultsCount())
                .as("stage 1 fixture should have one SPR")
                .isEqualTo(1);
        assertThat(stage1.getSearchMetadata().getSemanticResults(0).getEmbeddingConfigId())
                .as("stage 1 SPR embedding_config_id must be empty (placeholder)")
                .isEmpty();

        // Stage 2: one SPR, embedding_config_id populated
        assertThat(stage2.getSearchMetadata().getSemanticResultsCount())
                .as("stage 2 fixture should have one SPR")
                .isEqualTo(1);
        assertThat(stage2.getSearchMetadata().getSemanticResults(0).getEmbeddingConfigId())
                .as("stage 2 SPR embedding_config_id must be populated")
                .isEqualTo("minilm");

        // Stage 3: three SPRs (stage-2 preserved + centroid + boundary)
        assertThat(stage3.getSearchMetadata().getSemanticResultsCount())
                .as("stage 3 fixture should have three SPRs (preserved stage-2 + centroid + boundary)")
                .isEqualTo(3);
    }

    private PipeDoc callProcessData(String moduleName) {
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-module-name", Metadata.ASCII_STRING_MARSHALLER), moduleName);
        PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub stub =
                PipeStepProcessorServiceGrpc.newBlockingStub(channel)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));

        ProcessDataResponse response = stub.processData(
                ProcessDataRequest.newBuilder().build());

        assertThat(response.getOutcome())
                .as("%s step mock should return PROCESSING_OUTCOME_SUCCESS", moduleName)
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);
        assertThat(response.hasOutputDoc())
                .as("%s step mock should include output_doc", moduleName)
                .isTrue();

        return response.getOutputDoc();
    }
}
