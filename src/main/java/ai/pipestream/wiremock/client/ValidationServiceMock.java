package ai.pipestream.wiremock.client;

import ai.pipestream.validation.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring ValidationService mocks in WireMock.
 */
public class ValidationServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(ValidationServiceMock.class);

    private static final String SERVICE_NAME = ValidationServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService validationService;

    public ValidationServiceMock(WireMock wireMock) {
        this.validationService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    public ValidationServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.validationService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default ValidationService stubs");

        // Default successful responses
        mockValidateNodeSuccess();
        mockValidateGraphSuccess();
        mockValidateModuleSuccess();
    }

    public void mockValidateNodeSuccess() {
        ValidateNodeResponse response = ValidateNodeResponse.newBuilder()
                .setIsValid(true)
                .build();

        validationService.stubFor(
                method("ValidateNode")
                        .willReturn(message(response))
        );
    }

    public void mockValidateGraphSuccess() {
        ValidateGraphResponse response = ValidateGraphResponse.newBuilder()
                .setIsValid(true)
                .build();

        validationService.stubFor(
                method("ValidateGraph")
                        .willReturn(message(response))
        );
    }

    public void mockValidateModuleSuccess() {
        ValidateModuleResponse response = ValidateModuleResponse.newBuilder()
                .setIsValid(true)
                .build();

        validationService.stubFor(
                method("ValidateModule")
                        .willReturn(message(response))
        );
    }

    public void mockValidateNodeFailure(String message, String errorCode) {
        ValidateNodeResponse response = ValidateNodeResponse.newBuilder()
                .setIsValid(false)
                .addErrors(ValidationError.newBuilder()
                        .setMessage(message)
                        .setErrorCode(errorCode)
                        .build())
                .build();

        validationService.stubFor(
                method("ValidateNode")
                        .willReturn(message(response))
        );
    }

    public void reset() {
        validationService.resetAll();
    }
}
