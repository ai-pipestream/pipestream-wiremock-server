package ai.pipestream.wiremock.server;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.v1.EngineV1ServiceGrpc;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffStreamRequest;
import ai.pipestream.engine.v1.IntakeHandoffStreamResponse;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

class EngineV1DirectGrpcServiceTest {

    private static final Logger LOG = Logger.getLogger(EngineV1DirectGrpcServiceTest.class);
    private static final Metadata.Key<String> SCENARIO_HEADER =
            Metadata.Key.of("x-test-scenario", Metadata.ASCII_STRING_MARSHALLER);

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer directGrpcServer;
    private ManagedChannel channel;
    private EngineV1ServiceGrpc.EngineV1ServiceStub asyncStub;

    @BeforeEach
    void setUp() throws Exception {
        wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
        wireMockServer.start();

        directGrpcServer = new DirectWireMockGrpcServer(wireMockServer, 0);
        directGrpcServer.start();

        channel = ManagedChannelBuilder.forAddress("localhost", directGrpcServer.getGrpcPort())
                .usePlaintext()
                .build();
        asyncStub = EngineV1ServiceGrpc.newStub(channel);
        LOG.infof("Started direct EngineV1 mock on gRPC port %d", directGrpcServer.getGrpcPort());
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
    @DisplayName("intakeHandoffStream accepts requests and echoes handoff correlation fields")
    void intakeHandoffStream_acceptsAndEchoesCorrelationFields() throws Exception {
        StreamResult result = runOneHandoff(null, "handoff-accepted", 7);

        assertThat(result.completed.await(5, TimeUnit.SECONDS))
                .as("stream should complete")
                .isTrue();
        assertThat(result.error).as("stream error").isNull();
        assertThat(result.responses).hasSize(1);

        IntakeHandoffStreamResponse response = result.responses.getFirst();
        assertThat(response.getHandoffId()).isEqualTo("handoff-accepted");
        assertThat(response.getSequence()).isEqualTo(7);
        assertThat(response.getStatus()).isEqualTo(IntakeHandoffStreamResponse.Status.STATUS_ACCEPTED);
        assertThat(response.getResponse().getAccepted()).isTrue();
        assertThat(response.getResponse().getAssignedStreamId()).isEqualTo("mock-engine-stream-7");
        assertThat(response.getResponse().getEntryNodeId()).isEqualTo("mock-entry-node");
    }

    @Test
    @DisplayName("intakeHandoffStream can emit retryable rejection acks")
    void intakeHandoffStream_retryableRejectScenario() throws Exception {
        StreamResult result = runOneHandoff("retryable-reject", "handoff-retry", 2);

        assertThat(result.completed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.error).isNull();
        assertThat(result.responses).hasSize(1);
        assertThat(result.responses.getFirst().getStatus())
                .isEqualTo(IntakeHandoffStreamResponse.Status.STATUS_RETRYABLE_REJECTED);
        assertThat(result.responses.getFirst().getResponse().getAccepted()).isFalse();
        assertThat(result.responses.getFirst().getResponse().getMessage()).contains("retryable");
    }

    @Test
    @DisplayName("intakeHandoffStream can emit permanent rejection acks")
    void intakeHandoffStream_permanentRejectScenario() throws Exception {
        StreamResult result = runOneHandoff("permanent-reject", "handoff-permanent", 3);

        assertThat(result.completed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.error).isNull();
        assertThat(result.responses).hasSize(1);
        assertThat(result.responses.getFirst().getStatus())
                .isEqualTo(IntakeHandoffStreamResponse.Status.STATUS_PERMANENT_REJECTED);
        assertThat(result.responses.getFirst().getResponse().getAccepted()).isFalse();
        assertThat(result.responses.getFirst().getResponse().getMessage()).contains("permanent");
    }

    @Test
    @DisplayName("intakeHandoffStream can fail the stream for transport-error tests")
    void intakeHandoffStream_streamErrorScenario() throws Exception {
        StreamResult result = runOneHandoff("stream-error", "handoff-error", 4);

        assertThat(result.completed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.responses).isEmpty();
        assertThat(result.error)
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> assertThat(((StatusRuntimeException) error).getStatus().getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
    }

    @Test
    @DisplayName("intakeHandoffStream honors x-test-scenario=force-error")
    void intakeHandoffStream_forceErrorScenario() throws Exception {
        StreamResult result = runOneHandoff("force-error", "handoff-force-error", 6);

        assertThat(result.completed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.responses).isEmpty();
        assertThat(result.error)
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(error -> assertThat(((StatusRuntimeException) error).getStatus().getCode())
                        .isEqualTo(Status.Code.UNAVAILABLE));
    }

    @Test
    @DisplayName("intakeHandoffStream can intentionally omit acks for timeout tests")
    void intakeHandoffStream_noAckScenario() throws Exception {
        StreamResult result = openStream("no-ack");

        result.requests.onNext(request("handoff-no-ack", 5));

        assertThat(result.firstResponse.await(200, TimeUnit.MILLISECONDS))
                .as("no-ack scenario should not emit a response while the stream remains open")
                .isFalse();
        assertThat(result.responses).as("no-ack scenario should not emit responses").isEmpty();
        assertThat(result.completed.await(200, TimeUnit.MILLISECONDS))
                .as("server should keep stream open while omitting acks")
                .isFalse();

        result.requests.onCompleted();
        assertThat(result.completed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.error).isNull();
        assertThat(result.responses).as("no-ack scenario should stay response-free after half-close").isEmpty();
    }

    private StreamResult runOneHandoff(String scenario, String handoffId, long sequence) throws Exception {
        StreamResult result = openStream(scenario);
        result.requests.onNext(request(handoffId, sequence));
        result.requests.onCompleted();
        return result;
    }

    private StreamResult openStream(String scenario) {
        StreamResult result = new StreamResult();
        EngineV1ServiceGrpc.EngineV1ServiceStub stub = asyncStub;
        if (scenario != null) {
            Metadata headers = new Metadata();
            headers.put(SCENARIO_HEADER, scenario);
            stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        result.requests = stub.intakeHandoffStream(result);
        return result;
    }

    private static IntakeHandoffStreamRequest request(String handoffId, long sequence) {
        return IntakeHandoffStreamRequest.newBuilder()
                .setHandoffId(handoffId)
                .setSequence(sequence)
                .setRequest(IntakeHandoffRequest.newBuilder()
                        .setDatasourceId("ds-engine-test")
                        .setAccountId("acct-engine-test")
                        .setStream(PipeStream.newBuilder()
                                .setStreamId("stream-" + sequence)
                                .setDocument(PipeDoc.newBuilder().setDocId("doc-" + sequence).build())
                                .build())
                        .build())
                .build();
    }

    private static final class StreamResult implements StreamObserver<IntakeHandoffStreamResponse> {
        private final List<IntakeHandoffStreamResponse> responses = new ArrayList<>();
        private final CountDownLatch firstResponse = new CountDownLatch(1);
        private final CountDownLatch completed = new CountDownLatch(1);
        private volatile Throwable error;
        private StreamObserver<IntakeHandoffStreamRequest> requests;

        @Override
        public void onNext(IntakeHandoffStreamResponse value) {
            responses.add(value);
            firstResponse.countDown();
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
