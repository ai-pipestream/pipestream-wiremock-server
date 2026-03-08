package ai.pipestream.wiremock.server;

import ai.pipestream.opensearch.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DirectWireMockGrpcServer to verify the OpenSearch Manager mock.
 */
class OpenSearchManagerMockTest {

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer directGrpcServer;
    private ManagedChannel channel;
    private OpenSearchManagerServiceGrpc.OpenSearchManagerServiceBlockingStub blockingStub;

    @BeforeEach
    void setUp() throws Exception {
        wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
        wireMockServer.start();

        directGrpcServer = new DirectWireMockGrpcServer(wireMockServer, 0);
        directGrpcServer.start();

        channel = ManagedChannelBuilder.forAddress("localhost", directGrpcServer.getGrpcPort())
                .usePlaintext()
                .build();
        blockingStub = OpenSearchManagerServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (directGrpcServer != null) {
            directGrpcServer.stop();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void testIndexDocument() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndexName("test-index")
                .setDocumentId("doc-1")
                .setDocument(OpenSearchDocument.newBuilder()
                        .setOriginalDocId("doc-1")
                        .setTitle("Test Title")
                        .build())
                .build();

        IndexDocumentResponse response = blockingStub.indexDocument(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals("doc-1", response.getDocumentId());
        assertTrue(response.getMessage().contains("WireMock"));
    }

    @Test
    void testIndexAnyDocument() {
        IndexAnyDocumentRequest request = IndexAnyDocumentRequest.newBuilder()
                .setIndexName("test-index")
                .build();

        IndexAnyDocumentResponse response = blockingStub.indexAnyDocument(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("AnyDocument"));
    }

    @Test
    void testLifecycleMethods() {
        // createIndex
        CreateIndexResponse createResp = blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName("test").build());
        assertTrue(createResp.getSuccess());

        // indexExists
        IndexExistsResponse existsResp = blockingStub.indexExists(IndexExistsRequest.newBuilder().setIndexName("test").build());
        assertTrue(existsResp.getExists());

        // searchFilesystemMeta
        SearchFilesystemMetaResponse searchResp = blockingStub.searchFilesystemMeta(SearchFilesystemMetaRequest.newBuilder().build());
        assertEquals(0, searchResp.getTotalCount());
    }
}
