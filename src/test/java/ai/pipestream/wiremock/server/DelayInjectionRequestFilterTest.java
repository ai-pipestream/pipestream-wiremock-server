package ai.pipestream.wiremock.server;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceResponse;
import ai.pipestream.repository.pipedoc.v1.PipeDocServiceGrpc;
import ai.pipestream.wiremock.client.PipeDocServiceMock;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ForwardingClientCall;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies {@link DelayInjectionRequestFilter} delays both HTTP and gRPC stubs when
 * the {@code x-test-delay-ms} header is set, and is a no-op when the header is absent.
 */
class DelayInjectionRequestFilterTest {

    private static final long DELAY_MS = 600L;
    private static final long SLACK_MS = 250L;

    private static WireMockServer wireMockServer;
    private static WireMock wireMock;
    private static ManagedChannel channel;

    @BeforeAll
    static void setUp() {
        WireMockConfiguration cfg = WireMockConfiguration.options()
                .dynamicPort()
                .withRootDirectory("build/resources/test/wiremock");
        cfg.extensions(new GrpcExtensionFactory());
        cfg.extensions(new DelayInjectionRequestFilter());
        wireMockServer = new WireMockServer(cfg);
        wireMockServer.start();

        wireMock = new WireMock(wireMockServer.port());

        wireMock.register(get(urlEqualTo("/ping")).willReturn(aResponse().withBody("pong")));

        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    @DisplayName("HTTP stub: x-test-delay-ms causes server-side delay before responding")
    void httpDelayHeaderDelaysResponse() throws IOException {
        long t0 = System.nanoTime();
        readWithDelayHeader("/ping", DELAY_MS);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        assertTrue(elapsedMs >= DELAY_MS - SLACK_MS,
                "Expected at least " + (DELAY_MS - SLACK_MS) + "ms but got " + elapsedMs + "ms");
        assertTrue(elapsedMs < DELAY_MS + 5_000L,
                "Did not expect to wait significantly longer than " + DELAY_MS + "ms but got " + elapsedMs + "ms");
    }

    @Test
    @DisplayName("HTTP stub: missing x-test-delay-ms means no delay")
    void noHeaderMeansNoDelay() throws IOException {
        long t0 = System.nanoTime();
        readWithDelayHeader("/ping", -1);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        assertTrue(elapsedMs < DELAY_MS / 2,
                "Expected fast response without header but took " + elapsedMs + "ms");
    }

    @Test
    @DisplayName("gRPC stub (PipeDocService): x-test-delay-ms delays the gRPC response")
    void grpcDelayHeaderDelaysGrpcResponse() {
        PipeDocServiceMock mock = new PipeDocServiceMock(wireMock);
        PipeDoc doc = PipeDoc.newBuilder().setDocId("delay-doc").build();
        mock.registerPipeDoc("delay-doc", "acct", doc, "n1", "d1");

        PipeDocServiceGrpc.PipeDocServiceBlockingStub delayingStub =
                PipeDocServiceGrpc.newBlockingStub(channel)
                        .withInterceptors(headerInterceptor("x-test-delay-ms", String.valueOf(DELAY_MS)));

        DocumentReference ref = DocumentReference.newBuilder()
                .setDocId("delay-doc")
                .setAccountId("acct")
                .build();

        long t0 = System.nanoTime();
        GetPipeDocByReferenceResponse response = delayingStub.getPipeDocByReference(
                GetPipeDocByReferenceRequest.newBuilder().setDocumentRef(ref).build());
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        assertNotNull(response);
        assertTrue(response.hasPipedoc());
        assertEquals("delay-doc", response.getPipedoc().getDocId());
        assertTrue(elapsedMs >= DELAY_MS - SLACK_MS,
                "Expected at least " + (DELAY_MS - SLACK_MS) + "ms but got " + elapsedMs + "ms");
    }

    @Test
    @DisplayName("gRPC stub: client deadline shorter than configured delay surfaces DEADLINE_EXCEEDED")
    void grpcDeadlineShorterThanDelayFailsFast() {
        PipeDocServiceMock mock = new PipeDocServiceMock(wireMock);
        PipeDoc doc = PipeDoc.newBuilder().setDocId("deadline-doc").build();
        mock.registerPipeDoc("deadline-doc", "acct", doc, "n1", "d1");

        long longDelay = 5_000L;
        PipeDocServiceGrpc.PipeDocServiceBlockingStub stub =
                PipeDocServiceGrpc.newBlockingStub(channel)
                        .withInterceptors(headerInterceptor("x-test-delay-ms", String.valueOf(longDelay)))
                        .withDeadlineAfter(750, TimeUnit.MILLISECONDS);

        DocumentReference ref = DocumentReference.newBuilder()
                .setDocId("deadline-doc")
                .setAccountId("acct")
                .build();

        io.grpc.StatusRuntimeException ex = assertThrows(io.grpc.StatusRuntimeException.class,
                () -> stub.getPipeDocByReference(
                        GetPipeDocByReferenceRequest.newBuilder().setDocumentRef(ref).build()));

        assertEquals(io.grpc.Status.Code.DEADLINE_EXCEEDED, ex.getStatus().getCode(),
                "Expected DEADLINE_EXCEEDED but got: " + ex.getStatus());
    }

    private static void readWithDelayHeader(String path, long delayMs) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + wireMockServer.port() + path).toURL().openConnection();
        try {
            if (delayMs >= 0) {
                conn.setRequestProperty("x-test-delay-ms", String.valueOf(delayMs));
            }
            conn.setConnectTimeout(5_000);
            conn.setReadTimeout(15_000);
            int rc = conn.getResponseCode();
            if (rc != 200) {
                throw new IOException("Unexpected status: " + rc);
            }
            try (java.io.InputStream in = conn.getInputStream()) {
                String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                if (!"pong".equals(body)) {
                    throw new IOException("Unexpected body: " + body);
                }
            }
        } finally {
            conn.disconnect();
        }
    }

    private static ClientInterceptor headerInterceptor(String name, String value) {
        Metadata.Key<String> key = Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(key, value);
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }
}
