package ai.pipestream.wiremock.server;

import ai.pipestream.repository.filesystem.upload.v1.NodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepoServiceMockTest {

    private static final Logger LOG = Logger.getLogger(RepoServiceMockTest.class);

    private WireMockServer wireMockServer;
    private ManagedChannel channel;
    private NodeUploadServiceGrpc.NodeUploadServiceBlockingStub stub;

    @BeforeEach
    void setUp() {
        LOG.infof("Service Name: %s", NodeUploadServiceGrpc.SERVICE_NAME);
        LOG.infof("Full Method Name: %s", NodeUploadServiceGrpc.getUploadFilesystemPipeDocMethod().getFullMethodName());

        // Start WireMock with gRPC extension
        // Force file loading from build dir (finds mappings/ and grpc/ in build/resources/test/wiremock)
        wireMockServer = new WireMockServer(wireMockConfig()
                .dynamicPort()
                .notifier(new com.github.tomakehurst.wiremock.common.ConsoleNotifier(true))
                .withRootDirectory("build/resources/test/wiremock")
                .extensions(new GrpcExtensionFactory()));
        wireMockServer.start();

        // Debug: List stub mappings
        wireMockServer.getStubMappings().forEach(s ->
            LOG.infof("Loaded Stub: %s", s.getRequest().getUrlPath()));

        // Connect gRPC client
        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
        stub = NodeUploadServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void testUploadPipeDoc() {
        UploadFilesystemPipeDocRequest request = UploadFilesystemPipeDocRequest.newBuilder().build();
        UploadFilesystemPipeDocResponse response = stub.uploadFilesystemPipeDoc(request);

        assertTrue(response.getSuccess());
        assertEquals("mock-doc-123", response.getDocumentId());
        assertEquals("Successfully uploaded to mock repository", response.getMessage());
    }
}
