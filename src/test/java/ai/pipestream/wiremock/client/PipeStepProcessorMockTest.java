package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
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

    @Test
    @DisplayName("Should set active module for GetServiceRegistration")
    void testSetActiveModule() {
        // Register multiple modules
        processorMock.registerParserModule("parser", "1.0.0", "Parser", "");
        processorMock.registerChunkerModule("chunker", "1.0.0", "Chunker", "");
        processorMock.registerSinkModule("sink", "1.0.0", "Sink", "");

        // Set parser as active - should not throw
        assertDoesNotThrow(() -> processorMock.setActiveModule("parser"));

        // Set chunker as active - should not throw
        assertDoesNotThrow(() -> processorMock.setActiveModule("chunker"));

        // Set sink as active - should not throw
        assertDoesNotThrow(() -> processorMock.setActiveModule("sink"));
    }

    @Test
    @DisplayName("Should throw exception for unknown module in setActiveModule")
    void testSetActiveModuleUnknown() {
        processorMock.registerParserModule("parser", "1.0.0", "Parser", "");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> processorMock.setActiveModule("unknown-module")
        );

        assertTrue(exception.getMessage().contains("Module not registered"));
        assertTrue(exception.getMessage().contains("unknown-module"));
    }

    @Test
    @DisplayName("Should expose MODULE_NAME_HEADER constant")
    void testModuleNameHeaderConstant() {
        assertEquals("x-module-name", PipeStepProcessorMock.MODULE_NAME_HEADER);
    }

    @Test
    @DisplayName("Should return module-specific capabilities when x-module-name header is present")
    void testGetServiceRegistrationWithHeader() {
        // Register multiple modules
        processorMock.registerParserModule("tika-parser", "1.0.0", "Tika Parser", "");
        processorMock.registerChunkerModule("text-chunker", "1.0.0", "Text Chunker", "");

        // Create metadata with x-module-name header for tika-parser
        Metadata metadata = new Metadata();
        Metadata.Key<String> moduleNameKey = Metadata.Key.of(
                PipeStepProcessorMock.MODULE_NAME_HEADER, Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(moduleNameKey, "tika-parser");

        // Create stub with header interceptor
        PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub headerStub =
                stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

        // Call GetServiceRegistration
        GetServiceRegistrationRequest request = GetServiceRegistrationRequest.newBuilder().build();
        GetServiceRegistrationResponse response = headerStub.getServiceRegistration(request);

        // Verify tika-parser capabilities are returned (PARSER)
        assertNotNull(response);
        assertTrue(response.hasCapabilities());
        assertTrue(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));
        assertEquals("tika-parser", response.getModuleName());

        // Now test with text-chunker header
        metadata = new Metadata();
        metadata.put(moduleNameKey, "text-chunker");
        headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
        response = headerStub.getServiceRegistration(request);

        // Verify text-chunker capabilities are returned (no PARSER)
        assertNotNull(response);
        assertTrue(response.hasCapabilities());
        assertFalse(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));
        assertEquals("text-chunker", response.getModuleName());
    }

    // ============================================
    // Header-based Module Selection Tests
    // ============================================

    @Test
    @DisplayName("Should return sink capabilities with x-module-name header for sink module")
    void testGetServiceRegistrationWithHeaderForSink() {
        // Register sink module
        processorMock.registerSinkModule("opensearch-sink", "2.0.0", "OpenSearch Sink", "Writes to OpenSearch");

        // Create metadata with x-module-name header for sink
        Metadata metadata = new Metadata();
        Metadata.Key<String> moduleNameKey = Metadata.Key.of(
                PipeStepProcessorMock.MODULE_NAME_HEADER, Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(moduleNameKey, "opensearch-sink");

        // Create stub with header interceptor
        PipeStepProcessorServiceGrpc.PipeStepProcessorServiceBlockingStub headerStub =
                stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

        // Call GetServiceRegistration
        GetServiceRegistrationRequest request = GetServiceRegistrationRequest.newBuilder().build();
        GetServiceRegistrationResponse response = headerStub.getServiceRegistration(request);

        // Verify sink capabilities are returned
        assertNotNull(response);
        assertTrue(response.hasCapabilities());
        assertTrue(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_SINK));
        assertFalse(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));
        assertEquals("opensearch-sink", response.getModuleName());
        assertEquals("2.0.0", response.getVersion());
        assertEquals("OpenSearch Sink", response.getDisplayName());
    }

    @Test
    @DisplayName("Should switch between modules using headers without setActiveModule")
    void testHeaderBasedModuleSwitching() {
        // Register all types of modules
        processorMock.registerParserModule("parser-a", "1.0.0", "Parser A", "");
        processorMock.registerChunkerModule("chunker-b", "1.0.0", "Chunker B", "");
        processorMock.registerEmbedderModule("embedder-c", "1.0.0", "Embedder C", "");
        processorMock.registerSinkModule("sink-d", "1.0.0", "Sink D", "");

        GetServiceRegistrationRequest request = GetServiceRegistrationRequest.newBuilder().build();
        Metadata.Key<String> moduleNameKey = Metadata.Key.of(
                PipeStepProcessorMock.MODULE_NAME_HEADER, Metadata.ASCII_STRING_MARSHALLER);

        // Test parser
        Metadata metadata = new Metadata();
        metadata.put(moduleNameKey, "parser-a");
        var headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
        var response = headerStub.getServiceRegistration(request);
        assertEquals("parser-a", response.getModuleName());
        assertTrue(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));

        // Test chunker
        metadata = new Metadata();
        metadata.put(moduleNameKey, "chunker-b");
        headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
        response = headerStub.getServiceRegistration(request);
        assertEquals("chunker-b", response.getModuleName());
        assertFalse(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));
        assertFalse(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_SINK));

        // Test embedder
        metadata = new Metadata();
        metadata.put(moduleNameKey, "embedder-c");
        headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
        response = headerStub.getServiceRegistration(request);
        assertEquals("embedder-c", response.getModuleName());

        // Test sink
        metadata = new Metadata();
        metadata.put(moduleNameKey, "sink-d");
        headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
        response = headerStub.getServiceRegistration(request);
        assertEquals("sink-d", response.getModuleName());
        assertTrue(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_SINK));
    }

    // ============================================
    // setActiveModule with gRPC Verification Tests
    // ============================================

    @Test
    @DisplayName("Should return active module capabilities via gRPC when no header present")
    void testSetActiveModuleWithGrpcVerification() {
        // Register multiple modules
        processorMock.registerParserModule("parser-mod", "1.0.0", "Parser Module", "");
        processorMock.registerChunkerModule("chunker-mod", "1.0.0", "Chunker Module", "");

        // Set parser as active
        processorMock.setActiveModule("parser-mod");

        // Call without header - should get parser capabilities
        GetServiceRegistrationRequest request = GetServiceRegistrationRequest.newBuilder().build();
        GetServiceRegistrationResponse response = stub.getServiceRegistration(request);

        assertNotNull(response);
        assertEquals("parser-mod", response.getModuleName());
        assertTrue(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));

        // Now set chunker as active
        processorMock.setActiveModule("chunker-mod");

        // Call without header - should now get chunker capabilities
        response = stub.getServiceRegistration(request);
        assertEquals("chunker-mod", response.getModuleName());
        assertFalse(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));
    }

    @Test
    @DisplayName("Should prioritize header over active module")
    void testHeaderTakesPriorityOverActiveModule() {
        // Register modules
        processorMock.registerParserModule("parser-x", "1.0.0", "Parser X", "");
        processorMock.registerSinkModule("sink-y", "1.0.0", "Sink Y", "");

        // Set parser as active
        processorMock.setActiveModule("parser-x");

        // But use header to request sink
        Metadata metadata = new Metadata();
        Metadata.Key<String> moduleNameKey = Metadata.Key.of(
                PipeStepProcessorMock.MODULE_NAME_HEADER, Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(moduleNameKey, "sink-y");
        var headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

        GetServiceRegistrationRequest request = GetServiceRegistrationRequest.newBuilder().build();
        GetServiceRegistrationResponse response = headerStub.getServiceRegistration(request);

        // Header should take priority - we should get sink, not parser
        assertEquals("sink-y", response.getModuleName());
        assertTrue(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_SINK));
        assertFalse(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));
    }

    // ============================================
    // ProcessData Mock Tests
    // ============================================

    @Test
    @DisplayName("Should mock successful ProcessData response")
    void testMockProcessDataSuccess() {
        processorMock.mockProcessDataSuccess();

        ProcessDataRequest request = ProcessDataRequest.newBuilder().build();
        ProcessDataResponse response = stub.processData(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
    }

    @Test
    @DisplayName("Should mock parser ProcessData with extracted text")
    void testMockParserProcessData() {
        processorMock.mockParserProcessData("This is the extracted text from the document");

        ProcessDataRequest request = ProcessDataRequest.newBuilder().build();
        ProcessDataResponse response = stub.processData(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertTrue(response.hasOutputDoc());
        assertTrue(response.getProcessorLogsList().stream()
                .anyMatch(log -> log.contains("Parser extracted")));
    }

    @Test
    @DisplayName("Should mock chunker ProcessData with chunk count")
    void testMockChunkerProcessData() {
        processorMock.mockChunkerProcessData(5);

        ProcessDataRequest request = ProcessDataRequest.newBuilder().build();
        ProcessDataResponse response = stub.processData(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertTrue(response.getProcessorLogsList().stream()
                .anyMatch(log -> log.contains("Chunker created 5 chunks")));
    }

    @Test
    @DisplayName("Should mock embedder ProcessData with embedding dimensions")
    void testMockEmbedderProcessData() {
        processorMock.mockEmbedderProcessData(1536, 10);

        ProcessDataRequest request = ProcessDataRequest.newBuilder().build();
        ProcessDataResponse response = stub.processData(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertTrue(response.getProcessorLogsList().stream()
                .anyMatch(log -> log.contains("10 embeddings with dimension 1536")));
    }

    @Test
    @DisplayName("Should mock sink ProcessData with document count")
    void testMockSinkProcessData() {
        processorMock.mockSinkProcessData("my-index", 25);

        ProcessDataRequest request = ProcessDataRequest.newBuilder().build();
        ProcessDataResponse response = stub.processData(request);

        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertTrue(response.getProcessorLogsList().stream()
                .anyMatch(log -> log.contains("25 documents to my-index")));
    }

    @Test
    @DisplayName("Should mock ProcessData failure with error details")
    void testMockProcessDataFailure() {
        processorMock.mockProcessDataFailure("Document parsing failed", "PARSE_ERROR");

        ProcessDataRequest request = ProcessDataRequest.newBuilder().build();
        ProcessDataResponse response = stub.processData(request);

        assertNotNull(response);
        assertFalse(response.getSuccess());
        assertTrue(response.hasErrorDetails());
        assertTrue(response.getProcessorLogsList().stream()
                .anyMatch(log -> log.contains("Processing failed")));
    }

    // ============================================
    // gRPC Error Scenario Tests
    // ============================================

    @Test
    @DisplayName("Should mock ProcessData FAILED_PRECONDITION for blob not hydrated")
    void testMockProcessDataBlobNotHydrated() {
        processorMock.mockProcessDataBlobNotHydrated("Blob data not hydrated - cannot process document");

        ProcessDataRequest request = ProcessDataRequest.newBuilder().build();

        io.grpc.StatusRuntimeException exception = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.processData(request)
        );

        assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("Blob data not hydrated"));
    }

    @Test
    @DisplayName("Should mock ProcessData UNAVAILABLE for module unavailability")
    void testMockProcessDataUnavailable() {
        processorMock.mockProcessDataUnavailable("Module is temporarily unavailable");

        ProcessDataRequest request = ProcessDataRequest.newBuilder().build();

        io.grpc.StatusRuntimeException exception = assertThrows(
                io.grpc.StatusRuntimeException.class,
                () -> stub.processData(request)
        );

        assertEquals(io.grpc.Status.Code.UNAVAILABLE, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("temporarily unavailable"));
    }

    // ============================================
    // End-to-End Integration Tests
    // ============================================

    @Test
    @DisplayName("Should support complete parser module workflow")
    void testCompleteParserModuleWorkflow() {
        // 1. Register parser module
        processorMock.registerParserModule("workflow-parser", "1.0.0", "Workflow Parser", "Test parser");

        // 2. Verify registration via gRPC with header
        Metadata metadata = new Metadata();
        Metadata.Key<String> moduleNameKey = Metadata.Key.of(
                PipeStepProcessorMock.MODULE_NAME_HEADER, Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(moduleNameKey, "workflow-parser");
        var headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

        GetServiceRegistrationRequest regRequest = GetServiceRegistrationRequest.newBuilder().build();
        GetServiceRegistrationResponse regResponse = headerStub.getServiceRegistration(regRequest);

        assertTrue(regResponse.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));
        assertTrue(regResponse.getHealthCheckPassed());

        // 3. Mock successful processing
        processorMock.mockParserProcessData("Extracted document content");

        // 4. Process data
        ProcessDataRequest dataRequest = ProcessDataRequest.newBuilder().build();
        ProcessDataResponse dataResponse = stub.processData(dataRequest);

        assertTrue(dataResponse.getSuccess());
    }

    @Test
    @DisplayName("Should support complete sink module workflow")
    void testCompleteSinkModuleWorkflow() {
        // 1. Register sink module
        processorMock.registerSinkModule("workflow-sink", "1.0.0", "Workflow Sink", "Test sink");

        // 2. Set as active module
        processorMock.setActiveModule("workflow-sink");

        // 3. Verify registration without header (uses active module)
        GetServiceRegistrationRequest regRequest = GetServiceRegistrationRequest.newBuilder().build();
        GetServiceRegistrationResponse regResponse = stub.getServiceRegistration(regRequest);

        assertEquals("workflow-sink", regResponse.getModuleName());
        assertTrue(regResponse.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_SINK));

        // 4. Mock sink processing
        processorMock.mockSinkProcessData("documents-index", 100);

        // 5. Process data
        ProcessDataRequest dataRequest = ProcessDataRequest.newBuilder().build();
        ProcessDataResponse dataResponse = stub.processData(dataRequest);

        assertTrue(dataResponse.getSuccess());
        assertTrue(dataResponse.getProcessorLogsList().stream()
                .anyMatch(log -> log.contains("100 documents")));
    }

    @Test
    @DisplayName("Should verify module config details through gRPC")
    void testModuleConfigDetailsViaGrpc() {
        String moduleName = "detailed-parser";
        String version = "2.1.0";
        String displayName = "Detailed Parser Module";
        String description = "A detailed description of the parser functionality";

        processorMock.registerParserModule(moduleName, version, displayName, description);

        Metadata metadata = new Metadata();
        Metadata.Key<String> moduleNameKey = Metadata.Key.of(
                PipeStepProcessorMock.MODULE_NAME_HEADER, Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(moduleNameKey, moduleName);
        var headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

        GetServiceRegistrationRequest request = GetServiceRegistrationRequest.newBuilder().build();
        GetServiceRegistrationResponse response = headerStub.getServiceRegistration(request);

        assertEquals(moduleName, response.getModuleName());
        assertEquals(version, response.getVersion());
        assertEquals(displayName, response.getDisplayName());
        assertEquals(description, response.getDescription());
        assertTrue(response.getHealthCheckPassed());
        assertEquals("Module is healthy", response.getHealthCheckMessage());
    }

    @Test
    @DisplayName("Should handle multiple concurrent module registrations")
    void testMultipleConcurrentModuleRegistrations() {
        // Register many modules
        for (int i = 1; i <= 5; i++) {
            processorMock.registerParserModule("parser-" + i, "1." + i + ".0", "Parser " + i, "");
            processorMock.registerChunkerModule("chunker-" + i, "1." + i + ".0", "Chunker " + i, "");
            processorMock.registerSinkModule("sink-" + i, "1." + i + ".0", "Sink " + i, "");
        }

        // Verify we can access each one via header
        GetServiceRegistrationRequest request = GetServiceRegistrationRequest.newBuilder().build();
        Metadata.Key<String> moduleNameKey = Metadata.Key.of(
                PipeStepProcessorMock.MODULE_NAME_HEADER, Metadata.ASCII_STRING_MARSHALLER);

        for (int i = 1; i <= 5; i++) {
            // Check parser
            Metadata metadata = new Metadata();
            metadata.put(moduleNameKey, "parser-" + i);
            var headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
            var response = headerStub.getServiceRegistration(request);
            assertEquals("parser-" + i, response.getModuleName());
            assertEquals("1." + i + ".0", response.getVersion());
            assertTrue(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_PARSER));

            // Check sink
            metadata = new Metadata();
            metadata.put(moduleNameKey, "sink-" + i);
            headerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
            response = headerStub.getServiceRegistration(request);
            assertEquals("sink-" + i, response.getModuleName());
            assertTrue(response.getCapabilities().getTypesList().contains(CapabilityType.CAPABILITY_TYPE_SINK));
        }
    }
}
