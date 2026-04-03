package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for BackendEndpointServiceMock.
 */
class BackendEndpointServiceMockTest {

    private static WireMockServer wireMockServer;
    private static BackendEndpointServiceMock backendMock;
    private static ManagedChannel channel;
    private static BackendEndpointServiceGrpc.BackendEndpointServiceBlockingStub stub;

    @BeforeAll
    static void setUp() {
        // Start WireMock server with gRPC extension
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new GrpcExtensionFactory())
                .withRootDirectory("build/resources/test/wiremock"));
        wireMockServer.start();

        WireMock wireMock = new WireMock(wireMockServer.port());
        backendMock = new BackendEndpointServiceMock(wireMock);

        // Create gRPC channel
        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
        stub = BackendEndpointServiceGrpc.newBlockingStub(channel);
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

    @BeforeEach
    void resetMocks() {
        backendMock.reset();
    }

    @Test
    @DisplayName("Should mock GetBackendEndpoints")
    void testMockGetBackendEndpoints() {
        // Prepare mock data
        BackendEndpointInfo info = BackendEndpointInfo.newBuilder()
                .setBackendId("test-backend")
                .setEndpointUrl("http://localhost:9090")
                .setHealthy(true)
                .setDescription("Test Backend")
                .build();
        
        backendMock.mockGetBackendEndpoints(List.of(info));

        // Call the service
        GetBackendEndpointsResponse response = stub.getBackendEndpoints(GetBackendEndpointsRequest.newBuilder().build());

        // Verify
        assertNotNull(response);
        assertEquals(1, response.getEndpointsCount());
        assertEquals("test-backend", response.getEndpoints(0).getBackendId());
        assertEquals("http://localhost:9090", response.getEndpoints(0).getEndpointUrl());
        assertTrue(response.getEndpoints(0).getHealthy());
    }

    @Test
    @DisplayName("Should mock UpdateBackendEndpoint success")
    void testMockUpdateBackendEndpointSuccess() {
        String newUrl = "http://new-host:8080";
        String oldUrl = "http://old-host:8080";
        
        backendMock.mockUpdateBackendEndpointSuccess(newUrl, oldUrl);

        // Call the service
        UpdateBackendEndpointRequest request = UpdateBackendEndpointRequest.newBuilder()
                .setEndpointUrl(newUrl)
                .setBackendId("primary")
                .build();
        UpdateBackendEndpointResponse response = stub.updateBackendEndpoint(request);

        // Verify
        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals(newUrl, response.getActiveEndpointUrl());
        assertEquals(oldUrl, response.getPreviousEndpointUrl());
    }

    @Test
    @DisplayName("Should mock UpdateBackendEndpoint failure (rollback)")
    void testMockUpdateBackendEndpointFailure() {
        String failedUrl = "http://broken-host:8080";
        String activeUrl = "http://current-host:8080";
        String error = "Connection refused";
        
        backendMock.mockUpdateBackendEndpointFailure(failedUrl, activeUrl, error);

        // Call the service
        UpdateBackendEndpointRequest request = UpdateBackendEndpointRequest.newBuilder()
                .setEndpointUrl(failedUrl)
                .build();
        UpdateBackendEndpointResponse response = stub.updateBackendEndpoint(request);

        // Verify
        assertNotNull(response);
        assertFalse(response.getSuccess());
        assertEquals(activeUrl, response.getActiveEndpointUrl());
        assertEquals(error, response.getErrorMessage());
    }
}
