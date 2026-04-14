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
 * Header-scoped mock for the semantic pipeline's semantic-graph step.
 *
 * <p>Registers a high-priority stub on
 * {@link PipeStepProcessorServiceGrpc#SERVICE_NAME}'s {@code ProcessData} method
 * that fires only when the gRPC request carries {@code x-module-name: semantic-graph}.
 * Returns a canned stage-3 {@link ai.pipestream.data.v1.PipeDoc} built by
 * {@link SemanticFixtureBuilder#buildStage3PipeDoc()} — the exact shape required
 * by {@code SemanticPipelineInvariants.assertPostSemanticGraph}.
 *
 * <p>This class only handles the SUCCESS scenario in its current form. Failure
 * and partial scenarios land in follow-up commits once Phase 3 module agents
 * actually need them.
 */
public class SemanticGraphStepMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(SemanticGraphStepMock.class);

    private static final String SERVICE_NAME = PipeStepProcessorServiceGrpc.SERVICE_NAME;
    private static final String MODULE_NAME = "semantic-graph";
    private static final String MODULE_NAME_HEADER = "x-module-name";
    private static final int HEADER_STUB_PRIORITY = 1;

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        registerSuccessScenario(wireMock);
        LOG.info("SemanticGraphStepMock registered success scenario for x-module-name={}", MODULE_NAME);
    }

    private void registerSuccessScenario(WireMock wireMock) {
        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                .setOutputDoc(SemanticFixtureBuilder.buildStage3PipeDoc())
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
            LOG.error("Failed to register SemanticGraphStepMock success scenario", e);
            throw new IllegalStateException("SemanticGraphStepMock init failed", e);
        }
    }
}
