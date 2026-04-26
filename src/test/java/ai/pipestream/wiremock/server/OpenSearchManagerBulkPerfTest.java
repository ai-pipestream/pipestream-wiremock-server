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
import java.util.concurrent.atomic.AtomicReference;

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

    /**
     * Task 1 – "Pre-Provisioning" Invariant.
     * Passing 3 semantic_config_ids must yield exactly 4 indices:
     * the parent index plus one side-index per semantic config.
     */
    @Test
    void testProvisionIndex_ThreeSemanticConfigs_ReturnsFourIndices() {
        ProvisionIndexRequest request = ProvisionIndexRequest.newBuilder()
                .setIndexName("parent-index")
                .addSemanticConfigIds("config-1")
                .addSemanticConfigIds("config-2")
                .addSemanticConfigIds("config-3")
                .build();

        ProvisionIndexResponse response = blockingStub.provisionIndex(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals(3, response.getBindingsProvisioned());
        // parent + 3 side-indices = 4 total
        assertEquals(4, response.getIndicesCreatedCount(),
                "Expected parent + 3 side indices = 4 total, got: " + response.getIndicesCreatedList());
        assertTrue(response.getIndicesCreatedList().contains("parent-index"));
        assertTrue(response.getIndicesCreatedList().contains("parent-index--vs--config-1"));
        assertTrue(response.getIndicesCreatedList().contains("parent-index--vs--config-2"));
        assertTrue(response.getIndicesCreatedList().contains("parent-index--vs--config-3"));
    }

    /**
     * Task 2 – Bulk Chaos Simulation.
     * Send 500 documents with x-bulk-fail-rate=0.2 and verify that
     * approximately 20 % of responses have success:false.
     */
    @Test
    void testStreamIndexDocuments_BulkChaos_TwentyPercentFailure() throws Exception {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-bulk-fail-rate", Metadata.ASCII_STRING_MARSHALLER), "0.2");

        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub chaosStub =
                asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        int totalDocs = 500;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        List<Throwable> errors = new ArrayList<>();

        StreamObserver<StreamIndexDocumentsRequest> requestObserver = chaosStub.streamIndexDocuments(
                new StreamObserver<StreamIndexDocumentsResponse>() {
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
                    .setRequestId("chaos-req-" + i)
                    .setIndexName("chaos-index")
                    .setDocumentId("chaos-doc-" + i)
                    .setDocument(OpenSearchDocument.newBuilder()
                            .setOriginalDocId("chaos-doc-" + i)
                            .build())
                    .build());
        }
        requestObserver.onCompleted();

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Chaos test timed out");
        assertTrue(errors.isEmpty(), "Stream encountered unexpected errors: " + errors);
        assertEquals(totalDocs, successCount.get() + failureCount.get());

        int failures = failureCount.get();
        // With p=0.2 over 500 docs the expected value is 100; a ±50 window is statistically safe
        assertTrue(failures > 50 && failures < 150,
                "Expected ~20% failures (~100), but got " + failures + " out of " + totalDocs);
    }

    /**
     * Task 3a – "Slow OpenSearch" Backpressure.
     * Send one document with x-test-delay-ms=2000 and a generous
     * client-side timeout.  The stream must complete without any error,
     * proving that a slow server does not drop the connection.
     */
    @Test
    void testStreamIndexDocuments_SlowServer_BackpressureHandled() throws Exception {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-test-delay-ms", Metadata.ASCII_STRING_MARSHALLER), "2000");

        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub slowStub =
                asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        CountDownLatch latch = new CountDownLatch(1);
        List<StreamIndexDocumentsResponse> responses = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();

        StreamObserver<StreamIndexDocumentsRequest> requestObserver = slowStub.streamIndexDocuments(
                new StreamObserver<StreamIndexDocumentsResponse>() {
                    @Override
                    public void onNext(StreamIndexDocumentsResponse value) {
                        responses.add(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        errors.add(t);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });

        requestObserver.onNext(StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("slow-req-1")
                .setIndexName("slow-index")
                .setDocumentId("slow-doc-1")
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId("slow-doc-1").build())
                .build());
        requestObserver.onCompleted();

        // Allow up to 10 s; the server should respond after its 2-second delay
        assertTrue(latch.await(10, TimeUnit.SECONDS),
                "Stream did not complete within 10 seconds under 2-second server delay");
        assertTrue(errors.isEmpty(), "Stream encountered unexpected errors: " + errors);
        assertEquals(1, responses.size(), "Expected exactly one response");
        assertTrue(responses.get(0).getSuccess(), "Response should be successful");
    }

    /**
     * Task 3b – "Slow OpenSearch" Client-Side Deadline.
     * Send a request with x-test-delay-ms=3000 but impose a 500 ms
     * client-side deadline.  The client must observe DEADLINE_EXCEEDED
     * before the server wakes up.
     */
    @Test
    void testStreamIndexDocuments_SlowServer_ClientDeadlineRespected() throws Exception {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-test-delay-ms", Metadata.ASCII_STRING_MARSHALLER), "3000");

        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub deadlineStub =
                asyncStub
                        .withDeadlineAfter(500, TimeUnit.MILLISECONDS)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> caughtError = new AtomicReference<>();

        StreamObserver<StreamIndexDocumentsRequest> requestObserver = deadlineStub.streamIndexDocuments(
                new StreamObserver<StreamIndexDocumentsResponse>() {
                    @Override
                    public void onNext(StreamIndexDocumentsResponse value) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        caughtError.set(t);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });

        requestObserver.onNext(StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("deadline-req-1")
                .setIndexName("deadline-index")
                .setDocumentId("deadline-doc-1")
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId("deadline-doc-1").build())
                .build());
        requestObserver.onCompleted();

        // The call should fail well within 5 s (expected ~500 ms)
        assertTrue(latch.await(5, TimeUnit.SECONDS),
                "Expected a deadline error but timed out waiting");
        assertNotNull(caughtError.get(), "Expected a deadline error but stream completed successfully");
        assertTrue(caughtError.get() instanceof StatusRuntimeException,
                "Error should be a StatusRuntimeException, was: " + caughtError.get().getClass());
        StatusRuntimeException sre = (StatusRuntimeException) caughtError.get();
        assertEquals(Status.Code.DEADLINE_EXCEEDED, sre.getStatus().getCode(),
                "Expected DEADLINE_EXCEEDED but got: " + sre.getStatus().getCode());
    }

    /**
     * Task 4a – Invalid Request: Missing document ID.
     * A streamed document with a blank original_doc_id and a blank
     * document_id must produce a response with success:false.
     */
    @Test
    void testStreamIndexDocuments_InvalidRequest_MissingDocId() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger failureCount = new AtomicInteger(0);
        List<Throwable> errors = new ArrayList<>();

        StreamObserver<StreamIndexDocumentsRequest> requestObserver = asyncStub.streamIndexDocuments(
                new StreamObserver<StreamIndexDocumentsResponse>() {
                    @Override
                    public void onNext(StreamIndexDocumentsResponse value) {
                        if (!value.getSuccess()) {
                            failureCount.incrementAndGet();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        errors.add(t);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });

        // Send a document with no original_doc_id and no document_id set
        requestObserver.onNext(StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("invalid-req-no-id")
                .setIndexName("valid-index")
                .setDocument(OpenSearchDocument.newBuilder()
                        .setTitle("Document without any ID")
                        .build())
                .build());
        requestObserver.onCompleted();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Test timed out");
        assertTrue(errors.isEmpty(), "Unexpected stream-level error: " + errors);
        assertEquals(1, failureCount.get(),
                "Expected exactly 1 failure response for a document missing its ID");
    }

    /**
     * Task 4b – Invalid Request: Empty embedding vectors.
     * A streamed document whose SemanticVectorSet contains an
     * OpenSearchEmbedding with no vector floats must produce a
     * response with success:false.
     */
    @Test
    void testStreamIndexDocuments_InvalidRequest_EmptyEmbeddings() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger failureCount = new AtomicInteger(0);
        List<Throwable> errors = new ArrayList<>();

        StreamObserver<StreamIndexDocumentsRequest> requestObserver = asyncStub.streamIndexDocuments(
                new StreamObserver<StreamIndexDocumentsResponse>() {
                    @Override
                    public void onNext(StreamIndexDocumentsResponse value) {
                        if (!value.getSuccess()) {
                            failureCount.incrementAndGet();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        errors.add(t);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });

        // Send a document with a SemanticVectorSet that has an embedding with zero vector floats
        requestObserver.onNext(StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("invalid-req-empty-emb")
                .setIndexName("valid-index")
                .setDocumentId("emb-doc-1")
                .setDocument(OpenSearchDocument.newBuilder()
                        .setOriginalDocId("emb-doc-1")
                        .addSemanticSets(SemanticVectorSet.newBuilder()
                                .setSourceFieldName("body")
                                .addEmbeddings(OpenSearchEmbedding.newBuilder().build()) // empty vector list – should trigger validation failure
                                .build())
                        .build())
                .build());
        requestObserver.onCompleted();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Test timed out");
        assertTrue(errors.isEmpty(), "Unexpected stream-level error: " + errors);
        assertEquals(1, failureCount.get(),
                "Expected exactly 1 failure response for an embedding with no vector floats");
    }
}
