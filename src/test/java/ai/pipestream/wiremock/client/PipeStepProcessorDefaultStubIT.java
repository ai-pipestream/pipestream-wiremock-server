package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.ProcessDataRequest;
import ai.pipestream.data.module.v1.ProcessDataResponse;
import ai.pipestream.data.module.v1.ProcessingOutcome;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * End-to-end gRPC test that proves
 * {@link ServiceMockRegistry#initializeAll(WireMock)} — the same call the
 * production {@link ai.pipestream.wiremock.server.WiremockManager} makes on
 * Quarkus startup — registers a default {@code ProcessData} stub that
 * responds with {@code PROCESSING_OUTCOME_SUCCESS}.
 *
 * <p>Why this test exists: every other test in
 * {@link PipeStepProcessorMockTest} sets up its own {@code ProcessData}
 * stubs explicitly per test (via {@code processorMock.mockProcessDataSuccess()}
 * or one of the typed variants). None of them exercises the
 * {@code initializeAll} → {@code initializeDefaults} → {@code setupDefaultProcessDataStubs}
 * chain that production relies on for "out-of-the-box" success responses.
 * That gap meant a regression in any of those steps would only surface in
 * downstream integration tests (e.g. engine integration tests calling into
 * a deployed wiremock container) as a baffling
 * {@code UNIMPLEMENTED: Method not found: PipeStepProcessorService/ProcessData}.
 *
 * <p>Setup mirrors how {@link ai.pipestream.wiremock.server.WiremockManager}
 * boots in production: start a {@link WireMockServer} with the
 * {@link GrpcExtensionFactory} extension and the wiremock root directory,
 * then call {@code new ServiceMockRegistry().initializeAll(wireMock)} to
 * register every discovered {@link ServiceMockInitializer}.
 *
 * <p>The assertion is a real gRPC call from a real {@code ManagedChannel}
 * to the bound port, returning a real {@link ProcessDataResponse} parsed
 * from the wire — not an in-memory mock object. If this passes,
 * deployed-image callers (engine integration tests, transport tests) are
 * guaranteed to get a default {@code SUCCESS} response from
 * {@code ProcessData} without any per-test setup.
 */
class PipeStepProcessorDefaultStubIT {

    private static WireMockServer wireMockServer;
    private static ManagedChannel channel;
    private static PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub stub;

    @BeforeAll
    static void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new GrpcExtensionFactory())
                .withRootDirectory("build/resources/test/wiremock"));
        wireMockServer.start();

        // Production startup path: ServiceMockRegistry.initializeAll(wireMock)
        // — the EXACT same call WiremockManager.onStart makes. No per-mock
        // setup; rely entirely on what ServiceLoader-discovered initializers
        // wire up by default.
        WireMock wireMock = new WireMock(wireMockServer.port());
        ServiceMockRegistry registry = new ServiceMockRegistry();
        registry.initializeAll(wireMock);

        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
        stub = PipeStepProcessorServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    @DisplayName("ProcessData returns PROCESSING_OUTCOME_SUCCESS by default after initializeAll, no per-test setup")
    void processData_defaultStub_returnsSuccessOverRealGrpc() {
        ProcessDataResponse response = stub.processData(ProcessDataRequest.newBuilder().build());

        assertThat(response)
                .as("a real gRPC ProcessData call after ServiceMockRegistry.initializeAll must NOT throw UNIMPLEMENTED — that would mean the default stub from PipeStepProcessorMock.setupDefaultProcessDataStubs never registered, and downstream callers (engine integration tests, deployed wiremock containers) would all see Method not found")
                .isNotNull();
        assertThat(response.getOutcome())
                .as("default ProcessData stub must return SUCCESS so that pipeline tests which never set up a per-test mock still see a sensible outcome")
                .isEqualTo(ProcessingOutcome.PROCESSING_OUTCOME_SUCCESS);
    }

    @Test
    @DisplayName("ProcessData on a fresh-after-initializeAll server does not throw UNIMPLEMENTED")
    void processData_doesNotThrowUnimplemented() {
        // Belt-and-suspenders sibling test: explicitly assert that the call
        // does NOT raise StatusRuntimeException(UNIMPLEMENTED). If the
        // default-stub registration ever regresses, this is the first place
        // the failure surfaces — same diagnostic shape as the engine
        // integration tests that hit the deployed image.
        assertThatCode(() -> stub.processData(ProcessDataRequest.newBuilder().build()))
                .as("a real gRPC ProcessData call against the default-initialized server must NOT throw — the default stub IS the production behaviour the engine relies on; UNIMPLEMENTED here would mean WiremockManager → ServiceMockRegistry.initializeAll → PipeStepProcessorMock.setupDefaultProcessDataStubs regressed")
                .doesNotThrowAnyException();
    }
}
