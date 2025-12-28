package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.CapabilityType;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MockModuleRegistry.
 */
class MockModuleRegistryTest {

    private static WireMockServer wireMockServer;
    private static WireMock wireMock;
    private MockModuleRegistry registry;

    @BeforeAll
    static void setUp() {
        // Start WireMock server with gRPC extension
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new GrpcExtensionFactory())
                .withRootDirectory("build/resources/test/wiremock"));
        wireMockServer.start();

        wireMock = new WireMock(wireMockServer.port());
    }

    @AfterAll
    static void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @BeforeEach
    void resetRegistry() {
        registry = new MockModuleRegistry(wireMock);
    }

    @Test
    @DisplayName("Should register parser module using builder")
    void testRegisterParserModule() {
        registry.registerParserModule("tika-parser")
                .version("1.0.0")
                .displayName("Tika Parser")
                .description("Parses documents using Apache Tika")
                .host("localhost", 50053)
                .supportedMimeTypes("application/pdf", "text/html")
                .build();

        var modules = registry.getRegisteredModules();
        assertEquals(1, modules.size());
        assertTrue(modules.containsKey("tika-parser"));

        var module = modules.get("tika-parser");
        assertEquals("1.0.0", module.version);
        assertEquals("Tika Parser", module.displayName);
        assertTrue(module.isParser());
        assertTrue(module.requiresBlob);
    }

    @Test
    @DisplayName("Should register chunker module using builder")
    void testRegisterChunkerModule() {
        registry.registerChunkerModule("text-chunker")
                .version("2.0.0")
                .displayName("Text Chunker")
                .chunkSize(512)
                .overlap(50)
                .build();

        var modules = registry.getRegisteredModules();
        assertEquals(1, modules.size());
        assertTrue(modules.containsKey("text-chunker"));

        var module = modules.get("text-chunker");
        assertEquals("2.0.0", module.version);
        assertFalse(module.isParser());
        assertFalse(module.requiresBlob);
    }

    @Test
    @DisplayName("Should register embedder module using builder")
    void testRegisterEmbedderModule() {
        registry.registerEmbedderModule("openai-embedder")
                .version("1.0.0")
                .model("text-embedding-ada-002")
                .dimensions(1536)
                .build();

        var modules = registry.getRegisteredModules();
        assertEquals(1, modules.size());
        assertTrue(modules.containsKey("openai-embedder"));

        var module = modules.get("openai-embedder");
        assertFalse(module.isParser());
        assertFalse(module.isSink());
        assertFalse(module.requiresBlob);
    }

    @Test
    @DisplayName("Should register sink module using builder")
    void testRegisterSinkModule() {
        registry.registerSinkModule("opensearch-sink")
                .version("1.0.0")
                .targetIndex("documents")
                .build();

        var modules = registry.getRegisteredModules();
        assertEquals(1, modules.size());
        assertTrue(modules.containsKey("opensearch-sink"));

        var module = modules.get("opensearch-sink");
        assertFalse(module.isParser());
        assertTrue(module.isSink());
        assertFalse(module.requiresBlob);
    }

    @Test
    @DisplayName("Should setup parser flow scenario")
    void testSetupParserFlowScenario() {
        registry.setupParserFlowScenario();

        var modules = registry.getRegisteredModules();
        assertEquals(3, modules.size());
        assertTrue(modules.containsKey("tika-parser"));
        assertTrue(modules.containsKey("text-chunker"));
        assertTrue(modules.containsKey("openai-embedder"));

        // Verify parser module requires blob
        assertTrue(registry.moduleRequiresBlob("tika-parser"));
        assertFalse(registry.moduleRequiresBlob("text-chunker"));
        assertFalse(registry.moduleRequiresBlob("openai-embedder"));
    }

    @Test
    @DisplayName("Should setup direct flow scenario")
    void testSetupDirectFlowScenario() {
        registry.setupDirectFlowScenario();

        var modules = registry.getRegisteredModules();
        assertEquals(3, modules.size());
        assertTrue(modules.containsKey("semantic-chunker"));
        assertTrue(modules.containsKey("sentence-transformer"));
        assertTrue(modules.containsKey("opensearch-sink"));

        // Verify no modules require blob in direct flow
        for (var module : modules.values()) {
            assertFalse(module.requiresBlob);
        }
    }

    @Test
    @DisplayName("Should get modules with specific capability")
    void testGetModulesWithCapability() {
        registry.registerParserModule("parser1").build();
        registry.registerParserModule("parser2").build();
        registry.registerChunkerModule("chunker1").build();
        registry.registerSinkModule("sink1").build();

        List<String> parsers = registry.getModulesWithCapability(CapabilityType.CAPABILITY_TYPE_PARSER);
        assertEquals(2, parsers.size());
        assertTrue(parsers.contains("parser1"));
        assertTrue(parsers.contains("parser2"));

        List<String> sinks = registry.getModulesWithCapability(CapabilityType.CAPABILITY_TYPE_SINK);
        assertEquals(1, sinks.size());
        assertTrue(sinks.contains("sink1"));
    }

    @Test
    @DisplayName("Should check if module requires blob")
    void testModuleRequiresBlob() {
        registry.registerParserModule("parser1").build();
        registry.registerChunkerModule("chunker1").build();

        assertTrue(registry.moduleRequiresBlob("parser1"));
        assertFalse(registry.moduleRequiresBlob("chunker1"));
        assertFalse(registry.moduleRequiresBlob("nonexistent"));
    }

    @Test
    @DisplayName("Should chain multiple module registrations")
    void testChainedRegistration() {
        registry.registerParserModule("parser")
                .version("1.0.0")
                .build()
                .registerChunkerModule("chunker")
                .version("1.0.0")
                .build()
                .registerEmbedderModule("embedder")
                .version("1.0.0")
                .build();

        assertEquals(3, registry.getRegisteredModules().size());
    }

    @Test
    @DisplayName("Should reset all mocks and registrations")
    void testReset() {
        registry.setupParserFlowScenario();
        assertEquals(3, registry.getRegisteredModules().size());

        registry.reset();

        assertTrue(registry.getRegisteredModules().isEmpty());
    }

    @Test
    @DisplayName("Should provide access to underlying mocks")
    void testGetUnderlyingMocks() {
        assertNotNull(registry.getProcessorMock());
        assertNotNull(registry.getRegistrationMock());
        assertNotNull(registry.getFilesystemMock());
    }

    @Test
    @DisplayName("Should setup error scenarios")
    void testSetupErrorScenarios() {
        // This should not throw
        assertDoesNotThrow(() -> registry.setupErrorScenarios());
    }

    @Test
    @DisplayName("Should setup large file scenario")
    void testSetupLargeFileScenario() {
        registry.setupLargeFileScenario(5);

        var modules = registry.getRegisteredModules();
        assertTrue(modules.containsKey("tika-parser"));
        assertTrue(modules.get("tika-parser").isParser());
    }

    @Test
    @DisplayName("Should handle module with custom host and port")
    void testCustomHostAndPort() {
        registry.registerParserModule("custom-parser")
                .host("192.168.1.100", 9999)
                .build();

        var module = registry.getRegisteredModules().get("custom-parser");
        assertEquals("192.168.1.100", module.host);
        assertEquals(9999, module.port);
    }
}
