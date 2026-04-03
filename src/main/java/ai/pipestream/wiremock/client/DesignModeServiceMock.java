package ai.pipestream.wiremock.client;

import ai.pipestream.design.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring DesignModeService mocks in WireMock.
 */
public class DesignModeServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(DesignModeServiceMock.class);

    private static final String SERVICE_NAME = DesignModeServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService designService;

    public DesignModeServiceMock(WireMock wireMock) {
        this.designService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    public DesignModeServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.designService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default DesignModeService stubs");

        // Default successful responses
        mockValidateDesignGraphSuccess();
        mockSimulatePipelineSuccess();
    }

    public void mockValidateDesignGraphSuccess() {
        ValidateDesignGraphResponse response = ValidateDesignGraphResponse.newBuilder()
                .setIsValid(true)
                .setDeploymentReadiness(DeploymentReadiness.newBuilder()
                        .setReadyForDeployment(true)
                        .build())
                .build();

        designService.stubFor(
                method("ValidateDesignGraph")
                        .willReturn(message(response))
        );
    }

    public void mockSimulatePipelineSuccess() {
        SimulatePipelineResponse response = SimulatePipelineResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Simulation completed")
                .setMetrics(SimulationMetrics.newBuilder()
                        .setOverallSuccessRate(1.0)
                        .setNodesProcessed(1)
                        .build())
                .build();

        designService.stubFor(
                method("SimulatePipeline")
                        .willReturn(message(response))
        );
    }

    public void reset() {
        designService.resetAll();
    }
}
