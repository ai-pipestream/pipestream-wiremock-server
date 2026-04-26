package ai.pipestream.wiremock.server;

import ai.pipestream.opensearch.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance and reliability test for the OpenSearch Manager streaming API mock.
 * Verifies that partial failures and high-throughput streams are handled correctly.
 */
class OpenSearchManagerBulkPerfTest {

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer directGrpcServer;
    private ManagedChannel channel;
    private OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub asyncStub;
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
        asyncStub = OpenSearchManagerServiceGrpc.newStub(channel);
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
    void testStreamIndexDocuments_PartialFailure_ViaHeader() throws Exception {
        // Set a 50% failure rate
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-bulk-fail-rate", Metadata.ASCII_STRING_MARSHALLER), "0.5");
        
        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub failStub = 
                asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        int totalDocs = 200;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        List<Throwable> errors = new ArrayList<>();

        StreamObserver<StreamIndexDocumentsRequest> requestObserver = failStub.streamIndexDocuments(new StreamObserver<StreamIndexDocumentsResponse>() {
            @Override
            public void onNext(StreamIndexDocumentsResponse value) {
                if (value.getSuccess()) {
                    successCount.incrementAndGet();
                } else {
                    failureCount.incrementAndGet();
                }
                if (successCount.get() + failureCount.get() == totalDocs) {
                    latch.countDown();
                }
            }

            @Override
            public void onError(Throwable t) {
                errors.add(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
            }
        });

        for (int i = 0; i < totalDocs; i++) {
            requestObserver.onNext(StreamIndexDocumentsRequest.newBuilder()
                    .setRequestId("req-" + i)
                    .setIndexName("perf-index")
                    .setDocumentId("doc-" + i)
                    .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId("doc-" + i).build())
                    .build());
        }
        requestObserver.onCompleted();

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Test timed out");
        assertTrue(errors.isEmpty(), "Stream encountered errors: " + errors);
        
        int failures = failureCount.get();
        // With 0.5 probability over 200 items, we expect roughly 100 failures.
        assertTrue(failures > 50 && failures < 150, "Expected roughly 100 failures, got " + failures);
        assertEquals(totalDocs, successCount.get() + failureCount.get());
    }

    @Test
    void testProvisionIndex_Success() {
        ProvisionIndexRequest request = ProvisionIndexRequest.newBuilder()
                .setIndexName("new-index")
                .addSemanticConfigIds("semantic-1")
                .addSemanticConfigIds("semantic-2")
                .build();

        ProvisionIndexResponse response = blockingStub.provisionIndex(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals(2, response.getBindingsProvisioned());
        assertTrue(response.getIndicesCreatedList().contains("new-index"));
        assertTrue(response.getIndicesCreatedList().contains("new-index--vs--semantic-1"));
    }
}
