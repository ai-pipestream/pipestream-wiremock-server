package ai.pipestream.wiremock.server;

import ai.pipestream.opensearch.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * High-fidelity integration test for the Direct gRPC Server's
 * OpenSearchManagerService implementation.
 */
class OpenSearchManagerHighFidelityTest {

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
    void testIndexDocument_SmartProxyFlow() {
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndexName("proxy-index")
                .setDocumentId("normal-doc-1")
                .setDocument(OpenSearchDocument.newBuilder().setTitle("Proxy Test").build())
                .build();

        IndexDocumentResponse response = blockingStub.indexDocument(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("Smart Proxy"));
    }

    @Test
    void testIndexDocument_ForcedError_ViaPoisonPill() {
        // "fail-this-doc" is hardcoded in DirectWireMockGrpcServer.java to trigger error
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndexName("any-index")
                .setDocumentId("fail-this-doc")
                .setDocument(OpenSearchDocument.newBuilder().setTitle("Fail me").build())
                .build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            blockingStub.indexDocument(request);
        });

        assertEquals(Status.Code.INTERNAL, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("Forced internal error"));
    }

    @Test
    void testIndexDocument_ForcedError_ViaHeader() {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-force-error", Metadata.ASCII_STRING_MARSHALLER), "true");
        
        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceBlockingStub headerStub = 
                blockingStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndexName("any-index")
                .setDocumentId("normal-doc")
                .setDocument(OpenSearchDocument.newBuilder().setTitle("Fail me via header").build())
                .build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            headerStub.indexDocument(request);
        });

        assertEquals(Status.Code.INTERNAL, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("Forced internal error"));
    }
}
