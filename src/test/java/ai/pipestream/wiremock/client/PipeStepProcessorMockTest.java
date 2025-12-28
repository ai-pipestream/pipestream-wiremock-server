package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PipeStepProcessorMock.
 */
class PipeStepProcessorMockTest {

    private static WireMockServer wireMockServer;
    private static WireMock wireMock;
    private static PipeStepProcessorMock processorMock;
    private static ManagedChannel channel;
    private static PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub stub;

    @BeforeAll
    static void setUp() {
        // Start WireMock server with gRPC extension
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new GrpcExtensionFactory())
                .withRootDirectory("build/resources/test/wiremock"));
        wireMockServer.start();

        wireMock = new WireMock(wireMockServer.port());
        processorMock = new PipeStepProcessorMock(wireMock);

        // Create gRPC channel
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

    @BeforeEach
    void resetMocks() {
        processorMock.reset();
    }

    @Test
    @DisplayName("Should register parser module with PARSER capability")
    void testRegisterParserModule() {
        // Register a parser module
        processorMock.registerParserModule("tika-parser", "1.0.0",
                "Tika Parser", "Parses documents using Apache Tika");

        // Verify the module is registered
        Set<String> modules = processorMock.getRegisteredModules();
        assertTrue(modules.contains("tika-parser"));
        assertTrue(processorMock.moduleRequiresBlob("tika-parser"));

        // Verify the module config
        var config = processorMock.getModuleConfig("tika-parser");
        assertTrue(config.isPresent());
        assertTrue(config.get().isParser());
        assertFalse(config.get().isSink());
    }

    @Test
    @DisplayName("Should register chunker module without special capabilities")
    void testRegisterChunkerModule() {
        // Register a chunker module
        processorMock.registerChunkerModule("text-chunker", "1.0.0",
                "Text Chunker", "Splits text into chunks");

        // Verify the module is registered
        Set<String> modules = processorMock.getRegisteredModules();
        assertTrue(modules.contains("text-chunker"));
        assertFalse(processorMock.moduleRequiresBlob("text-chunker"));

        // Verify the module config
        var config = processorMock.getModuleConfig("text-chunker");
        assertTrue(config.isPresent());
        assertFalse(config.get().isParser());
        assertFalse(config.get().isSink());
    }

    @Test
    @DisplayName("Should register embedder module")
    void testRegisterEmbedderModule() {
        // Register an embedder module
        processorMock.registerEmbedderModule("openai-embedder", "1.0.0",
                "OpenAI Embedder", "Generates embeddings");

        // Verify the module is registered
        Set<String> modules = processorMock.getRegisteredModules();
        assertTrue(modules.contains("openai-embedder"));
        assertFalse(processorMock.moduleRequiresBlob("openai-embedder"));
    }

    @Test
    @DisplayName("Should register sink module with SINK capability")
    void testRegisterSinkModule() {
        // Register a sink module
        processorMock.registerSinkModule("opensearch-sink", "1.0.0",
                "OpenSearch Sink", "Writes to OpenSearch");

        // Verify the module is registered
        Set<String> modules = processorMock.getRegisteredModules();
        assertTrue(modules.contains("opensearch-sink"));
        assertFalse(processorMock.moduleRequiresBlob("opensearch-sink"));

        // Verify the module config
        var config = processorMock.getModuleConfig("opensearch-sink");
        assertTrue(config.isPresent());
        assertFalse(config.get().isParser());
        assertTrue(config.get().isSink());
    }

    @Test
    @DisplayName("Should handle unknown module gracefully")
    void testUnknownModule() {
        assertFalse(processorMock.moduleRequiresBlob("unknown-module"));
        assertTrue(processorMock.getModuleConfig("unknown-module").isEmpty());
    }

    @Test
    @DisplayName("Should reset mocks and clear registrations")
    void testReset() {
        // Register some modules
        processorMock.registerParserModule("parser1", "1.0.0", "Parser 1", "");
        processorMock.registerChunkerModule("chunker1", "1.0.0", "Chunker 1", "");

        assertEquals(2, processorMock.getRegisteredModules().size());

        // Reset
        processorMock.reset();

        // Verify all registrations are cleared
        assertTrue(processorMock.getRegisteredModules().isEmpty());
    }

    @Test
    @DisplayName("Should create module config with correct capabilities")
    void testModuleConfigCapabilities() {
        processorMock.registerParserModule("parser", "1.0.0", "Parser", "");

        var config = processorMock.getModuleConfig("parser").orElseThrow();

        assertEquals("parser", config.moduleName);
        assertEquals("1.0.0", config.version);
        assertEquals("Parser", config.displayName);
        assertTrue(config.capabilities.contains(CapabilityType.CAPABILITY_TYPE_PARSER));
        assertTrue(config.requiresBlob);
        assertTrue(config.isParser());
    }
}
