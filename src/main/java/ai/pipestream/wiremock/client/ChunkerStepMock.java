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
 * Header-scoped mock for the semantic pipeline's chunker step.
 *
 * <p>Registers a high-priority stub on
 * {@link PipeStepProcessorServiceGrpc#SERVICE_NAME}'s {@code ProcessData} method
 * that fires only when the gRPC request carries {@code x-module-name: chunker}.
 * Returns a canned stage-1 {@link ai.pipestream.data.v1.PipeDoc} built by
 * {@link SemanticFixtureBuilder#buildStage1PipeDoc()} — the exact shape required
 * by {@code SemanticPipelineInvariants.assertPostChunker}.
 *
 * <p>Coexists with the existing {@link PipeStepProcessorMock}, which handles
 * {@code GetServiceRegistration} and the default (non-header-scoped)
 * {@code ProcessData} catchall. Multiple {@link ServiceMockInitializer}
 * implementations can target the same gRPC service — the registry invokes each
 * in sequence at startup.
 *
 * <p>This class only handles the SUCCESS scenario in its current form. Failure
 * and partial scenarios (fail-precondition, fail-invalid-arg, fail-internal,
 * partial, slow) will be added in follow-up commits once the success path is
 * validated end-to-end.
 */
public class ChunkerStepMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(ChunkerStepMock.class);

    private static final String SERVICE_NAME = PipeStepProcessorServiceGrpc.SERVICE_NAME;
    private static final String MODULE_NAME = "chunker";
    private static final String MODULE_NAME_HEADER = "x-module-name";
    private static final int HEADER_STUB_PRIORITY = 1;

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        registerSuccessScenario(wireMock);
        LOG.info("ChunkerStepMock registered success scenario for x-module-name={}", MODULE_NAME);
    }

    private void registerSuccessScenario(WireMock wireMock) {
        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setOutcome(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS)
                .setOutputDoc(SemanticFixtureBuilder.buildStage1PipeDoc())
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
            LOG.error("Failed to register ChunkerStepMock success scenario", e);
            throw new IllegalStateException("ChunkerStepMock init failed", e);
        }
    }
}
