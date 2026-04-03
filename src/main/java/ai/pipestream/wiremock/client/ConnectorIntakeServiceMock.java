package ai.pipestream.wiremock.client;

import ai.pipestream.connector.intake.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring ConnectorIntakeService mocks in WireMock.
 */
public class ConnectorIntakeServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorIntakeServiceMock.class);

    private static final String SERVICE_NAME = ConnectorIntakeServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService intakeService;

    public ConnectorIntakeServiceMock(WireMock wireMock) {
        this.intakeService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    public ConnectorIntakeServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.intakeService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default ConnectorIntakeService stubs");

        // Default successful responses for common operations
        mockUploadPipeDocSuccess("test-doc-id");
        mockUploadBlobSuccess("test-blob-doc-id");
        mockStartCrawlSessionSuccess("test-session-id", "test-crawl-id");
        mockEndCrawlSessionSuccess(0, 0);
        mockHeartbeatSuccess(true);
        mockDeletePipeDocSuccess();
    }

    public void mockUploadPipeDocSuccess(String docId) {
        UploadPipeDocResponse response = UploadPipeDocResponse.newBuilder()
                .setSuccess(true)
                .setDocId(docId)
                .setMessage("Document uploaded successfully")
                .build();

        intakeService.stubFor(
                method("UploadPipeDoc")
                        .willReturn(message(response))
        );
    }

    public void mockUploadBlobSuccess(String docId) {
        UploadBlobResponse response = UploadBlobResponse.newBuilder()
                .setSuccess(true)
                .setDocId(docId)
                .setMessage("Blob uploaded successfully")
                .build();

        intakeService.stubFor(
                method("UploadBlob")
                        .willReturn(message(response))
        );
    }

    public void mockStartCrawlSessionSuccess(String sessionId, String crawlId) {
        StartCrawlSessionResponse response = StartCrawlSessionResponse.newBuilder()
                .setSuccess(true)
                .setSessionId(sessionId)
                .setCrawlId(crawlId)
                .setMessage("Crawl session started")
                .build();

        intakeService.stubFor(
                method("StartCrawlSession")
                        .willReturn(message(response))
        );
    }

    public void mockEndCrawlSessionSuccess(int orphansFound, int orphansDeleted) {
        EndCrawlSessionResponse response = EndCrawlSessionResponse.newBuilder()
                .setSuccess(true)
                .setOrphansFound(orphansFound)
                .setOrphansDeleted(orphansDeleted)
                .setMessage("Crawl session ended")
                .build();

        intakeService.stubFor(
                method("EndCrawlSession")
                        .willReturn(message(response))
        );
    }

    public void mockHeartbeatSuccess(boolean sessionValid) {
        HeartbeatResponse response = HeartbeatResponse.newBuilder()
                .setSessionValid(sessionValid)
                .build();

        intakeService.stubFor(
                method("Heartbeat")
                        .willReturn(message(response))
        );
    }

    public void mockDeletePipeDocSuccess() {
        DeletePipeDocResponse response = DeletePipeDocResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Document deletion requested")
                .build();

        intakeService.stubFor(
                method("DeletePipeDoc")
                        .willReturn(message(response))
        );
    }

    public void reset() {
        intakeService.resetAll();
    }
}
