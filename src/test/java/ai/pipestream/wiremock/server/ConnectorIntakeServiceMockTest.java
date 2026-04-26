package ai.pipestream.wiremock.server;

import ai.pipestream.connector.intake.v1.ConnectorIntakeServiceGrpc;
import ai.pipestream.connector.intake.v1.DocReference;
import ai.pipestream.connector.intake.v1.PipeDocItem;
import ai.pipestream.connector.intake.v1.StreamContext;
import ai.pipestream.connector.intake.v1.UploadBlobRequest;
import ai.pipestream.connector.intake.v1.UploadBlobResponse;
import ai.pipestream.connector.intake.v1.UploadPipeDocRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
import ai.pipestream.connector.intake.v1.DeletePipeDocRequest;
import ai.pipestream.connector.intake.v1.DeletePipeDocResponse;
import ai.pipestream.connector.intake.v1.HeartbeatRequest;
import ai.pipestream.connector.intake.v1.HeartbeatResponse;
import ai.pipestream.connector.intake.v1.StartCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.StartCrawlSessionResponse;
import ai.pipestream.connector.intake.v1.EndCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.EndCrawlSessionResponse;
import ai.pipestream.data.v1.PipeDoc;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the {@code ConnectorIntakeServiceImpl} hosted on
 * {@link DirectWireMockGrpcServer}. Each test starts the server in-process,
 * connects a real gRPC client, exercises one RPC, and verifies the
 * response shape.
 * <p>
 * Two purposes:
 * <ol>
 *   <li>Regression coverage for the mock impl — if anyone changes the
 *       handler shape these tests catch it.</li>
 *   <li>Worked examples of how each RPC is expected to be called by
 *       real consumers. The test names describe the intended call
 *       pattern (first message is StreamContext, items follow, etc.)
 *       so a reader can understand the protocol from the tests alone.</li>
 * </ol>
 * The mock is deliberately simple — it does not exercise the full intake
 * pipeline (auth resolution, repository persistence, engine handoff). It
 * exists to give services that depend on connector-intake a deterministic
 * counterparty during their own integration tests.
 */
class ConnectorIntakeServiceMockTest {

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer directGrpcServer;
    private ManagedChannel channel;
    private ConnectorIntakeServiceGrpc.ConnectorIntakeServiceBlockingStub blockingStub;
    private ConnectorIntakeServiceGrpc.ConnectorIntakeServiceStub asyncStub;

    @BeforeEach
    void setUp() throws Exception {
        wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
        wireMockServer.start();

        directGrpcServer = new DirectWireMockGrpcServer(wireMockServer, 0);
        directGrpcServer.start();

        int grpcPort = directGrpcServer.getGrpcPort();
        channel = ManagedChannelBuilder.forAddress("localhost", grpcPort)
                .usePlaintext()
                .build();
        blockingStub = ConnectorIntakeServiceGrpc.newBlockingStub(channel);
        asyncStub = ConnectorIntakeServiceGrpc.newStub(channel);
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

    // ============================================================
    // Unary RPCs — example of how each is expected to be called.
    // ============================================================

    @Test
    @DisplayName("uploadPipeDoc returns deterministic doc_id of shape <datasource>:<sourceDoc>")
    void uploadPipeDoc_happyPath_returnsDeterministicDocId() {
        UploadPipeDocRequest req = UploadPipeDocRequest.newBuilder()
                .setDatasourceId("ds-test")
                .setApiKey("test-api-key")
                .setSourceDocId("src-001")
                .setPipeDoc(PipeDoc.newBuilder().setDocId("client-supplied-id").build())
                .build();

        UploadPipeDocResponse resp = blockingStub.uploadPipeDoc(req);

        assertTrue(resp.getSuccess(), "mock should ack uploadPipeDoc as success by default");
        assertEquals("ds-test:src-001", resp.getDocId(),
                "mock derives doc_id deterministically from datasource_id + source_doc_id");
        assertNotNull(resp.getMessage());
    }

    @Test
    @DisplayName("uploadPipeDoc with x-test-scenario=force-error returns success=false")
    void uploadPipeDoc_forceErrorScenario_returnsFailure() {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-test-scenario", Metadata.ASCII_STRING_MARSHALLER), "force-error");
        var stub = blockingStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        UploadPipeDocRequest req = UploadPipeDocRequest.newBuilder()
                .setDatasourceId("ds-test")
                .setApiKey("test-api-key")
                .setSourceDocId("src-002")
                .build();

        UploadPipeDocResponse resp = stub.uploadPipeDoc(req);

        assertFalse(resp.getSuccess(), "force-error scenario must return success=false");
        assertTrue(resp.getMessage().toLowerCase().contains("forced error"),
                "message should mention the forced error trigger so callers can diagnose");
    }

    @Test
    @DisplayName("uploadBlob returns deterministic doc_id of shape <datasource>:<sourceDoc>")
    void uploadBlob_happyPath_returnsDeterministicDocId() {
        UploadBlobRequest req = UploadBlobRequest.newBuilder()
                .setDatasourceId("ds-test")
                .setApiKey("test-api-key")
                .setSourceDocId("blob-001")
                .setContent(ByteString.copyFromUtf8("hello"))
                .build();

        UploadBlobResponse resp = blockingStub.uploadBlob(req);

        assertTrue(resp.getSuccess());
        assertEquals("ds-test:blob-001", resp.getDocId());
    }

    @Test
    @DisplayName("deletePipeDoc always acks success")
    void deletePipeDoc_alwaysAcksSuccess() {
        DeletePipeDocRequest req = DeletePipeDocRequest.newBuilder()
                .setDatasourceId("ds-test")
                .setApiKey("test-api-key")
                .setRef(DocReference.newBuilder().setDocId("ds-test:src-007").build())
                .build();

        DeletePipeDocResponse resp = blockingStub.deletePipeDoc(req);

        assertTrue(resp.getSuccess());
    }

    @Test
    @DisplayName("startCrawlSession echoes the client's crawl_id when provided")
    void startCrawlSession_echoesProvidedCrawlId() {
        StartCrawlSessionRequest req = StartCrawlSessionRequest.newBuilder()
                .setDatasourceId("ds-test")
                .setApiKey("test-api-key")
                .setCrawlId("my-crawl-42")
                .build();

        StartCrawlSessionResponse resp = blockingStub.startCrawlSession(req);

        assertTrue(resp.getSuccess());
        assertEquals("my-crawl-42", resp.getCrawlId(),
                "client-provided crawl_id should be echoed so it can be used as a tracking key");
        assertTrue(resp.getSessionId().startsWith("mock-session-"),
                "mock session ids should be prefixed so they're identifiable");
    }

    @Test
    @DisplayName("startCrawlSession generates a crawl_id when client omits one")
    void startCrawlSession_generatesCrawlIdWhenMissing() {
        StartCrawlSessionRequest req = StartCrawlSessionRequest.newBuilder()
                .setDatasourceId("ds-test")
                .setApiKey("test-api-key")
                // crawl_id intentionally not set
                .build();

        StartCrawlSessionResponse resp = blockingStub.startCrawlSession(req);

        assertTrue(resp.getSuccess());
        assertTrue(resp.getCrawlId().startsWith("mock-crawl-"),
                "fallback crawl_id should be prefixed mock-crawl-");
    }

    @Test
    @DisplayName("endCrawlSession reports zero orphans by default")
    void endCrawlSession_reportsNoOrphans() {
        EndCrawlSessionRequest req = EndCrawlSessionRequest.newBuilder()
                .setSessionId("mock-session-1")
                .build();

        EndCrawlSessionResponse resp = blockingStub.endCrawlSession(req);

        assertTrue(resp.getSuccess());
        assertEquals(0, resp.getOrphansFound());
        assertEquals(0, resp.getOrphansDeleted());
    }

    @Test
    @DisplayName("heartbeat reports session_valid=true")
    void heartbeat_reportsSessionValid() {
        HeartbeatRequest req = HeartbeatRequest.newBuilder()
                .setSessionId("mock-session-1")
                .build();

        HeartbeatResponse resp = blockingStub.heartbeat(req);

        assertTrue(resp.getSessionValid());
    }

    // ============================================================
    // Bidi streaming — uploadPipeDocStream
    // ============================================================
    //
    // Expected protocol:
    //   client → server: StreamContext (first message)
    //   server → client: UploadPipeDocStreamResponse{success=true, ref unset}
    //                    — context-accept ack (no DocReference because no doc yet)
    //   client → server: PipeDocItem (1..N)
    //   server → client: UploadPipeDocStreamResponse{success=true, ref={source_doc_id, doc_id}}
    //                    — per-item ack (one per PipeDocItem)
    //   client → server: onCompleted()
    //   server → client: ... drains pending acks ... onCompleted()

    @Test
    @DisplayName("streaming happy path: context first, then N items, server acks each in order")
    void uploadPipeDocStream_happyPath_acksContextThenEachItem() throws Exception {
        StreamCollector collector = new StreamCollector();
        StreamObserver<UploadPipeDocStreamRequest> requestObs = asyncStub.uploadPipeDocStream(collector);

        // 1. Context first
        requestObs.onNext(UploadPipeDocStreamRequest.newBuilder()
                .setContext(StreamContext.newBuilder()
                        .setDatasourceId("ds-test")
                        .setApiKey("test-api-key")
                        .setCrawlId("crawl-stream-001")
                        .setSubCrawlIndex(0)
                        .setTotalSubCrawls(1)
                        .setClientId("test-client")
                        .build())
                .build());

        // 2. N items
        for (int i = 0; i < 3; i++) {
            requestObs.onNext(UploadPipeDocStreamRequest.newBuilder()
                    .setItem(PipeDocItem.newBuilder()
                            .setSourceDocId("src-" + i)
                            .setPipeDoc(PipeDoc.newBuilder().build())
                            .build())
                    .build());
        }

        requestObs.onCompleted();
        assertTrue(collector.completed.await(5, TimeUnit.SECONDS), "stream should complete within 5s");

        // 1 context-accept + 3 per-item = 4 responses
        assertEquals(4, collector.responses.size(),
                "expected one context ack + three per-item acks");

        // First response is the context ack — no DocReference set
        UploadPipeDocStreamResponse contextAck = collector.responses.get(0);
        assertTrue(contextAck.getSuccess(), "context ack should succeed");
        assertEquals("", contextAck.getRef().getSourceDocId(),
                "context ack carries an empty DocReference (no doc yet)");

        // Subsequent responses are per-item acks, one per source_doc_id, in order
        for (int i = 0; i < 3; i++) {
            UploadPipeDocStreamResponse itemAck = collector.responses.get(i + 1);
            assertTrue(itemAck.getSuccess(), "item ack " + i + " should succeed");
            assertEquals("src-" + i, itemAck.getRef().getSourceDocId(),
                    "ack should echo the source_doc_id for client correlation");
            assertEquals("ds-test:src-" + i, itemAck.getRef().getDocId(),
                    "ack carries the deterministic doc_id");
            assertFalse(itemAck.getRetryable(), "happy path is never retryable");
        }
    }

    @Test
    @DisplayName("streaming: x-test-scenario=force-error makes per-doc acks retryable=true")
    void uploadPipeDocStream_forceError_setsRetryableTrue() throws Exception {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-test-scenario", Metadata.ASCII_STRING_MARSHALLER), "force-error");
        var stub = asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        StreamCollector collector = new StreamCollector();
        StreamObserver<UploadPipeDocStreamRequest> requestObs = stub.uploadPipeDocStream(collector);

        requestObs.onNext(UploadPipeDocStreamRequest.newBuilder()
                .setContext(StreamContext.newBuilder()
                        .setDatasourceId("ds-test")
                        .setApiKey("test-api-key")
                        .build())
                .build());
        requestObs.onNext(UploadPipeDocStreamRequest.newBuilder()
                .setItem(PipeDocItem.newBuilder()
                        .setSourceDocId("src-fail-1")
                        .setPipeDoc(PipeDoc.newBuilder().build())
                        .build())
                .build());

        requestObs.onCompleted();
        assertTrue(collector.completed.await(5, TimeUnit.SECONDS));

        // Find the per-item ack (skip the context-accept which has no DocReference)
        UploadPipeDocStreamResponse itemAck = collector.responses.stream()
                .filter(r -> !r.getRef().getSourceDocId().isEmpty())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no per-item ack received"));

        assertFalse(itemAck.getSuccess(), "force-error must produce a non-success ack");
        assertTrue(itemAck.getRetryable(),
                "force-error sets retryable=true so clients can demonstrate the resend path");
        assertEquals("src-fail-1", itemAck.getRef().getSourceDocId());
    }

    @Test
    @DisplayName("streaming: x-test-scenario=reject-context fails the context ack")
    void uploadPipeDocStream_rejectContext_failsContextAck() throws Exception {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("x-test-scenario", Metadata.ASCII_STRING_MARSHALLER), "reject-context");
        var stub = asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        StreamCollector collector = new StreamCollector();
        StreamObserver<UploadPipeDocStreamRequest> requestObs = stub.uploadPipeDocStream(collector);

        requestObs.onNext(UploadPipeDocStreamRequest.newBuilder()
                .setContext(StreamContext.newBuilder()
                        .setDatasourceId("ds-test")
                        .setApiKey("bad-key")
                        .build())
                .build());

        requestObs.onCompleted();
        assertTrue(collector.completed.await(5, TimeUnit.SECONDS));

        UploadPipeDocStreamResponse contextAck = collector.responses.get(0);
        assertFalse(contextAck.getSuccess(), "context ack should fail in reject-context scenario");
        assertFalse(contextAck.getRetryable(),
                "context rejection is permanent — auth won't fix itself, retryable=false");
        assertTrue(contextAck.getMessage().toLowerCase().contains("rejected"));
    }

    @Test
    @DisplayName("streaming: a delete_ref payload is acked but documents the not-implemented contract")
    void uploadPipeDocStream_deleteRefPayload_acksWithNotImplementedNote() throws Exception {
        StreamCollector collector = new StreamCollector();
        StreamObserver<UploadPipeDocStreamRequest> requestObs = asyncStub.uploadPipeDocStream(collector);

        requestObs.onNext(UploadPipeDocStreamRequest.newBuilder()
                .setContext(StreamContext.newBuilder()
                        .setDatasourceId("ds-test")
                        .setApiKey("test-api-key")
                        .build())
                .build());
        requestObs.onNext(UploadPipeDocStreamRequest.newBuilder()
                .setDeleteRef(DocReference.newBuilder()
                        .setSourceDocId("src-to-delete")
                        .setDocId("ds-test:src-to-delete")
                        .build())
                .build());

        requestObs.onCompleted();
        assertTrue(collector.completed.await(5, TimeUnit.SECONDS));

        UploadPipeDocStreamResponse deleteAck = collector.responses.stream()
                .filter(r -> r.getRef().getSourceDocId().equals("src-to-delete"))
                .findFirst()
                .orElseThrow();

        // The mock acks the delete with success=true so callers don't fail
        // in tests, but the same handler in the real intake service will
        // route deletes through the pipeline. This test documents the
        // contract: a streamed delete is acknowledged but the mock does
        // not actually do anything with it.
        assertTrue(deleteAck.getSuccess(), "mock acks streamed deletes as success");
        assertEquals("ds-test:src-to-delete", deleteAck.getRef().getDocId());
    }

    @Test
    @DisplayName("streaming: 100 items in a single stream, all acked, demonstrates throughput shape")
    void uploadPipeDocStream_oneHundredItems_allAcked() throws Exception {
        StreamCollector collector = new StreamCollector();
        StreamObserver<UploadPipeDocStreamRequest> requestObs = asyncStub.uploadPipeDocStream(collector);

        requestObs.onNext(UploadPipeDocStreamRequest.newBuilder()
                .setContext(StreamContext.newBuilder()
                        .setDatasourceId("ds-batch")
                        .setApiKey("test-api-key")
                        .setCrawlId("bulk-crawl-001")
                        .setSubCrawlIndex(0)
                        .setTotalSubCrawls(1)
                        .setClientId("test-client")
                        .build())
                .build());

        for (int i = 0; i < 100; i++) {
            requestObs.onNext(UploadPipeDocStreamRequest.newBuilder()
                    .setItem(PipeDocItem.newBuilder()
                            .setSourceDocId("doc-" + i)
                            .setPipeDoc(PipeDoc.newBuilder().build())
                            .build())
                    .build());
        }

        requestObs.onCompleted();
        assertTrue(collector.completed.await(15, TimeUnit.SECONDS),
                "100-item stream should complete within 15s");

        // 1 context ack + 100 per-item acks = 101 total
        assertEquals(101, collector.responses.size(),
                "expected exactly one context ack plus one per-item ack per doc");

        // Spot-check that every doc id is represented in the per-item acks
        long perItemCount = collector.responses.stream()
                .filter(r -> !r.getRef().getSourceDocId().isEmpty())
                .count();
        assertEquals(100, perItemCount, "every doc must produce exactly one per-item ack");
    }

    /**
     * Aggregates streaming responses for assertion. Latches on
     * {@code onCompleted} or {@code onError} so tests can wait
     * deterministically.
     */
    private static final class StreamCollector implements StreamObserver<UploadPipeDocStreamResponse> {
        final List<UploadPipeDocStreamResponse> responses = new ArrayList<>();
        final CountDownLatch completed = new CountDownLatch(1);
        volatile Throwable error;

        @Override
        public void onNext(UploadPipeDocStreamResponse value) {
            responses.add(value);
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            completed.countDown();
        }

        @Override
        public void onCompleted() {
            completed.countDown();
        }
    }
}
