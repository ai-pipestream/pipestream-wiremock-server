package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import ai.pipestream.wiremock.client.semantic.SemanticFixtureBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

/**
 * Header-scoped mock for the semantic pipeline's embedder step.
 *
 * <p>Registers a high-priority stub on
 * {@link PipeStepProcessorServiceGrpc#SERVICE_NAME}'s {@code ProcessData} method
 * that fires only when the gRPC request carries {@code x-module-name: embedder}.
 * Returns a canned stage-2 {@link ai.pipestream.data.v1.PipeDoc} built by
 * {@link SemanticFixtureBuilder#buildStage2PipeDoc()} — the exact shape required
 * by {@code SemanticPipelineInvariants.assertPostEmbedder}.
 *
 * <p>This class only handles the SUCCESS scenario in its current form. Failure
 * and partial scenarios land in follow-up commits once Phase 3 module agents
 * actually need them.
 */
public class EmbedderStepMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(EmbedderStepMock.class);

    private static final String SERVICE_NAME = PipeStepProcessorServiceGrpc.SERVICE_NAME;
    private static final String MODULE_NAME = "embedder";
    private static final String MODULE_NAME_HEADER = "x-module-name";
    private static final int HEADER_STUB_PRIORITY = 1;

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        registerSuccessScenario(wireMock);
        LOG.info("EmbedderStepMock registered success scenario for x-module-name={}", MODULE_NAME);
    }

    private void registerSuccessScenario(WireMock wireMock) {
        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                .setOutputDoc(SemanticFixtureBuilder.buildStage2PipeDoc())
                .build();

        String grpcPath = "/" + SERVICE_NAME + "/ProcessData";
        try {
            String responseJson = JsonFormat.printer().print(response);
            wireMock.register(
                    post(urlPathEqualTo(grpcPath))
                            .withHeader(MODULE_NAME_HEADER, equalTo(MODULE_NAME))
                            .atPriority(HEADER_STUB_PRIORITY)
                            .willReturn(okJson(responseJson)));
        } catch (Exception e) {
            LOG.error("Failed to register EmbedderStepMock success scenario", e);
            throw new IllegalStateException("EmbedderStepMock init failed", e);
        }
    }
}
