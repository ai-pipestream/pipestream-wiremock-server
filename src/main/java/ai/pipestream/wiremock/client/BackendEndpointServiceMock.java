package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.util.ArrayList;
import java.util.List;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring BackendEndpointService mocks in WireMock.
 * <p>
 * This service allows live-swapping external backend endpoints for modules
 * like DJL Serving or Docling.
 */
public class BackendEndpointServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(BackendEndpointServiceMock.class);

    private static final String SERVICE_NAME = BackendEndpointServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService backendService;

    public BackendEndpointServiceMock(WireMock wireMock) {
        this.backendService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    public BackendEndpointServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.backendService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default BackendEndpointService stubs");

        // Mock a default backend for DJL Serving
        mockGetBackendEndpoints(List.of(
                BackendEndpointInfo.newBuilder()
                        .setBackendId("primary-djl")
                        .setEndpointUrl("http://djl-serving:8080")
                        .setHealthy(true)
                        .setDescription("Primary DJL Serving instance")
                        .build()
        ));

        // Mock success for any update by default
        mockUpdateBackendEndpointSuccess("http://new-backend:8080", "http://old-backend:8080");
    }

    /**
     * Mock a successful UpdateBackendEndpoint response.
     *
     * @param newUrl The new endpoint URL
     * @param oldUrl The previous endpoint URL
     */
    public void mockUpdateBackendEndpointSuccess(String newUrl, String oldUrl) {
        UpdateBackendEndpointResponse response = UpdateBackendEndpointResponse.newBuilder()
                .setSuccess(true)
                .setActiveEndpointUrl(newUrl)
                .setPreviousEndpointUrl(oldUrl)
                .build();

        backendService.stubFor(
                method("UpdateBackendEndpoint")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a failed UpdateBackendEndpoint response (rollback).
     *
     * @param failedUrl The URL that failed the health probe
     * @param activeUrl The URL that remains active (original)
     * @param errorMessage The error message from the failed probe
     */
    public void mockUpdateBackendEndpointFailure(String failedUrl, String activeUrl, String errorMessage) {
        UpdateBackendEndpointResponse response = UpdateBackendEndpointResponse.newBuilder()
                .setSuccess(false)
                .setActiveEndpointUrl(activeUrl)
                .setPreviousEndpointUrl(activeUrl)
                .setErrorMessage(errorMessage)
                .build();

        backendService.stubFor(
                method("UpdateBackendEndpoint")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetBackendEndpoints response.
     *
     * @param endpoints List of backend endpoints to return
     */
    public void mockGetBackendEndpoints(List<BackendEndpointInfo> endpoints) {
        GetBackendEndpointsResponse response = GetBackendEndpointsResponse.newBuilder()
                .addAllEndpoints(endpoints)
                .build();

        backendService.stubFor(
                method("GetBackendEndpoints")
                        .willReturn(message(response))
        );
    }

    /**
     * Reset all WireMock stubs for the backend service.
     */
    public void reset() {
        backendService.resetAll();
    }
}
