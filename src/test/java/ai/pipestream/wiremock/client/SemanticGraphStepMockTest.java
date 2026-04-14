package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.wiremock.client.semantic.SemanticPipelineInvariants;
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
 * Tests for {@link SemanticGraphStepMock}. Verifies the mock responds correctly
 * when a gRPC {@code ProcessData} request arrives with
 * {@code x-module-name: semantic-graph} metadata and that the returned
 * {@link PipeDoc} passes
 * {@link SemanticPipelineInvariants#assertPostSemanticGraph(PipeDoc)}.
 */
class SemanticGraphStepMockTest {

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
    void successScenarioReturnsStage3PipeDocPassingAssertPostSemanticGraph() {
        PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub stub =
                stubWithModuleHeader("semantic-graph");

        ProcessDataResponse response = stub.processData(
                ProcessDataRequest.newBuilder().build());

        assertThat(response.getOutcome())
                .as("semantic-graph step mock success scenario should return PROCESSING_OUTCOME_SUCCESS")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);

        assertThat(response.hasOutputDoc())
                .as("semantic-graph step mock success scenario should include output_doc")
                .isTrue();

        PipeDoc outputDoc = response.getOutputDoc();

        assertThatCode(() -> SemanticPipelineInvariants.assertPostSemanticGraph(outputDoc))
                .as("semantic-graph step mock success response must satisfy SemanticPipelineInvariants.assertPostSemanticGraph")
                .doesNotThrowAnyException();
    }

    private PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub stubWithModuleHeader(
            String moduleName) {
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-module-name", Metadata.ASCII_STRING_MARSHALLER), moduleName);
        return PipeStepProcessorServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
    }
}
