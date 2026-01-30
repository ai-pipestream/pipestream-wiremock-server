package ai.pipestream.wiremock.client;

import ai.pipestream.platform.registration.v1.GetConnectorSchemaRequest;
import ai.pipestream.platform.registration.v1.GetServiceRequest;
import ai.pipestream.platform.registration.v1.PlatformRegistrationServiceGrpc;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PlatformRegistrationServiceMockTest {

    private WireMockServer wireMockServer;
    private PlatformRegistrationServiceMock registrationMock;
    private ManagedChannel channel;
    private PlatformRegistrationServiceGrpc.PlatformRegistrationServiceBlockingStub stub;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(wireMockConfig()
                .dynamicPort()
                .withRootDirectory("build/resources/test/wiremock")
                .extensions(new GrpcExtensionFactory()));
        wireMockServer.start();

        WireMock wireMock = new WireMock(wireMockServer.port());
        registrationMock = new PlatformRegistrationServiceMock();
        registrationMock.initializeDefaults(wireMock);

        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
        stub = PlatformRegistrationServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.shutdown();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void testGetConnectorSchema_Defaults() {
        var response = stub.getConnectorSchema(GetConnectorSchemaRequest.newBuilder()
                .setConnectorType("s3")
                .build());

        assertEquals("s3", response.getConnectorType());
        assertEquals("1.0.0", response.getSchemaVersion());
        assertEquals("connector-schema-s3", response.getArtifactId());
        assertFalse(response.getSchemaJson().isBlank());
        assertNotNull(response.getUpdatedAt());
    }

    @Test
    void testGetService_IncludesHttpEndpoints() {
        registrationMock.registerService("repository", "repo-1", "localhost", 8080,
                ai.pipestream.platform.registration.v1.ServiceType.SERVICE_TYPE_SERVICE);

        var response = stub.getService(GetServiceRequest.newBuilder()
                .setServiceName("repository")
                .build());

        assertEquals("repository", response.getServiceName());
        assertEquals(1, response.getHttpEndpointsCount());
        assertEquals("http", response.getHttpEndpoints(0).getScheme());
        assertEquals(8080, response.getHttpEndpoints(0).getPort());
        assertEquals("repository-service-http-schema", response.getHttpSchemaArtifactId());
        assertEquals("1.0.0", response.getHttpSchemaVersion());
    }
}
