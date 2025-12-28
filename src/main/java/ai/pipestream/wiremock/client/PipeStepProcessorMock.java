package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.*;
import ai.pipestream.data.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.util.*;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring PipeStepProcessorService mocks in WireMock.
 * <p>
 * This mock supports testing of module capabilities and processing behaviors including:
 * <ul>
 *   <li><b>Parser modules</b>: Require blob data, validate hydration, simulate text extraction</li>
 *   <li><b>Chunker modules</b>: Work with parsed metadata, don't require blob data</li>
 *   <li><b>Embedder modules</b>: Generate embeddings from text chunks</li>
 *   <li><b>Sink modules</b>: Write to external systems (simulated)</li>
 * </ul>
 * <p>
 * This class implements {@link ServiceMockInitializer} and will be automatically
 * discovered and initialized at server startup by the {@link ServiceMockRegistry}.
 */
public class PipeStepProcessorMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(PipeStepProcessorMock.class);

    private static final String SERVICE_NAME = PipeStepProcessorServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService processorService;

    // Track registered modules for dynamic behavior
    private final Map<String, ModuleConfig> registeredModules = new HashMap<>();

    /**
     * Create a helper for the given WireMock client.
     *
     * @param wireMock The WireMock client instance (connected to WireMock server)
     */
    public PipeStepProcessorMock(WireMock wireMock) {
        this.processorService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    /**
     * Default constructor for ServiceLoader discovery.
     */
    public PipeStepProcessorMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.processorService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default PipeStepProcessorService stubs");

        // Register default parser modules
        registerParserModule("tika-parser", "1.0.0", "Tika Document Parser",
                "Extracts text and metadata from various document formats using Apache Tika");
        registerParserModule("docling-parser", "1.0.0", "Docling Parser",
                "Parses documents into structured content using Docling");

        // Register default chunker modules
        registerChunkerModule("text-chunker", "1.0.0", "Text Chunker",
                "Splits text into manageable chunks for processing");
        registerChunkerModule("semantic-chunker", "1.0.0", "Semantic Chunker",
                "Splits text based on semantic boundaries");

        // Register default embedder modules
        registerEmbedderModule("openai-embedder", "1.0.0", "OpenAI Embedder",
                "Generates embeddings using OpenAI API");
        registerEmbedderModule("sentence-transformer", "1.0.0", "Sentence Transformer",
                "Generates embeddings using sentence transformers");

        // Register default sink modules
        registerSinkModule("opensearch-sink", "1.0.0", "OpenSearch Sink",
                "Writes documents to OpenSearch index");
        registerSinkModule("elasticsearch-sink", "1.0.0", "Elasticsearch Sink",
                "Writes documents to Elasticsearch index");

        // Set up default ProcessData stubs for each module type
        setupDefaultProcessDataStubs();

        LOG.info("Added {} module stubs for PipeStepProcessorService", registeredModules.size());
    }

    // ============================================
    // Module Registration Methods
    // ============================================

    /**
     * Register a parser module that requires blob data.
     *
     * @param moduleName Module name
     * @param version Module version
     * @param displayName Human-readable name
     * @param description Module description
     */
    public void registerParserModule(String moduleName, String version, String displayName, String description) {
        ModuleConfig config = new ModuleConfig(moduleName, version, displayName, description,
                List.of(CapabilityType.CAPABILITY_TYPE_PARSER), true);
        registeredModules.put(moduleName, config);
        mockGetServiceRegistration(config);
        LOG.debug("Registered parser module: {}", moduleName);
    }

    /**
     * Register a chunker module that works with parsed metadata.
     *
     * @param moduleName Module name
     * @param version Module version
     * @param displayName Human-readable name
     * @param description Module description
     */
    public void registerChunkerModule(String moduleName, String version, String displayName, String description) {
        ModuleConfig config = new ModuleConfig(moduleName, version, displayName, description,
                List.of(), false);
        registeredModules.put(moduleName, config);
        mockGetServiceRegistration(config);
        LOG.debug("Registered chunker module: {}", moduleName);
    }

    /**
     * Register an embedder module that generates embeddings.
     *
     * @param moduleName Module name
     * @param version Module version
     * @param displayName Human-readable name
     * @param description Module description
     */
    public void registerEmbedderModule(String moduleName, String version, String displayName, String description) {
        ModuleConfig config = new ModuleConfig(moduleName, version, displayName, description,
                List.of(), false);
        registeredModules.put(moduleName, config);
        mockGetServiceRegistration(config);
        LOG.debug("Registered embedder module: {}", moduleName);
    }

    /**
     * Register a sink module that writes to external systems.
     *
     * @param moduleName Module name
     * @param version Module version
     * @param displayName Human-readable name
     * @param description Module description
     */
    public void registerSinkModule(String moduleName, String version, String displayName, String description) {
        ModuleConfig config = new ModuleConfig(moduleName, version, displayName, description,
                List.of(CapabilityType.CAPABILITY_TYPE_SINK), false);
        registeredModules.put(moduleName, config);
        mockGetServiceRegistration(config);
        LOG.debug("Registered sink module: {}", moduleName);
    }

    // ============================================
    // GetServiceRegistration Mocks
    // ============================================

    /**
     * Mock GetServiceRegistration response for a module.
     *
     * @param config Module configuration
     */
    private void mockGetServiceRegistration(ModuleConfig config) {
        Capabilities.Builder capabilitiesBuilder = Capabilities.newBuilder();
        for (CapabilityType cap : config.capabilities) {
            capabilitiesBuilder.addTypes(cap);
        }

        GetServiceRegistrationResponse response = GetServiceRegistrationResponse.newBuilder()
                .setModuleName(config.moduleName)
                .setVersion(config.version)
                .setDisplayName(config.displayName)
                .setDescription(config.description)
                .setCapabilities(capabilitiesBuilder.build())
                .setHealthCheckPassed(true)
                .setHealthCheckMessage("Module is healthy")
                .build();

        // Note: WireMock gRPC matching is tricky - we use any request matching here
        // In a real scenario, you might want to set up module-specific endpoints
        processorService.stubFor(
                method("GetServiceRegistration")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetServiceRegistration to return a specific capability response.
     *
     * @param moduleName Module name
     * @param version Module version
     * @param capabilities List of capability types
     */
    public void mockGetServiceRegistrationWithCapabilities(String moduleName, String version,
                                                            List<CapabilityType> capabilities) {
        Capabilities.Builder capabilitiesBuilder = Capabilities.newBuilder();
        for (CapabilityType cap : capabilities) {
            capabilitiesBuilder.addTypes(cap);
        }

        GetServiceRegistrationResponse response = GetServiceRegistrationResponse.newBuilder()
                .setModuleName(moduleName)
                .setVersion(version)
                .setCapabilities(capabilitiesBuilder.build())
                .setHealthCheckPassed(true)
                .build();

        processorService.stubFor(
                method("GetServiceRegistration")
                        .willReturn(message(response))
        );
    }

    // ============================================
    // ProcessData Mocks
    // ============================================

    /**
     * Set up default ProcessData stubs for different module behaviors.
     */
    private void setupDefaultProcessDataStubs() {
        // Default successful processing response
        mockProcessDataSuccess();
    }

    /**
     * Mock a successful ProcessData response that passes through the document.
     */
    public void mockProcessDataSuccess() {
        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setSuccess(true)
                .build();

        processorService.stubFor(
                method("ProcessData")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a ProcessData response for a parser that extracts text from blob.
     * This simulates a parser module processing a document with inline blob data.
     *
     * @param extractedText The text to return as parsed content
     */
    public void mockParserProcessData(String extractedText) {
        // Build a simple response with output document
        PipeDoc outputDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .build();

        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setSuccess(true)
                .setOutputDoc(outputDoc)
                .addProcessorLogs("Parser extracted " + extractedText.length() + " characters")
                .build();

        processorService.stubFor(
                method("ProcessData")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a ProcessData response for a chunker that splits text into chunks.
     *
     * @param chunkCount Number of chunks to simulate
     */
    public void mockChunkerProcessData(int chunkCount) {
        PipeDoc outputDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .build();

        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setSuccess(true)
                .setOutputDoc(outputDoc)
                .addProcessorLogs("Chunker created " + chunkCount + " chunks")
                .build();

        processorService.stubFor(
                method("ProcessData")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a ProcessData response for an embedder that generates embeddings.
     *
     * @param embeddingDimension Dimension of each embedding vector
     * @param chunkCount Number of chunks to embed
     */
    public void mockEmbedderProcessData(int embeddingDimension, int chunkCount) {
        PipeDoc outputDoc = PipeDoc.newBuilder()
                .setDocId(UUID.randomUUID().toString())
                .build();

        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setSuccess(true)
                .setOutputDoc(outputDoc)
                .addProcessorLogs("Embedder generated " + chunkCount + " embeddings with dimension " + embeddingDimension)
                .build();

        processorService.stubFor(
                method("ProcessData")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a ProcessData response for a sink that writes to external systems.
     *
     * @param indexName Name of the index/collection written to
     * @param documentCount Number of documents written
     */
    public void mockSinkProcessData(String indexName, int documentCount) {
        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setSuccess(true)
                .addProcessorLogs("Sink wrote " + documentCount + " documents to " + indexName)
                .build();

        processorService.stubFor(
                method("ProcessData")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a ProcessData failure response.
     *
     * @param errorMessage The error message
     * @param errorCode The error code
     */
    public void mockProcessDataFailure(String errorMessage, String errorCode) {
        Struct errorDetails = Struct.newBuilder()
                .putFields("error_code", Value.newBuilder().setStringValue(errorCode).build())
                .putFields("error_message", Value.newBuilder().setStringValue(errorMessage).build())
                .build();

        ProcessDataResponse response = ProcessDataResponse.newBuilder()
                .setSuccess(false)
                .setErrorDetails(errorDetails)
                .addProcessorLogs("Processing failed: " + errorMessage)
                .build();

        processorService.stubFor(
                method("ProcessData")
                        .willReturn(message(response))
        );
    }

    /**
     * Mock a ProcessData response that returns gRPC FAILED_PRECONDITION error.
     * Used to test parser modules that receive documents without hydrated blobs.
     *
     * @param message Error message
     */
    public void mockProcessDataBlobNotHydrated(String message) {
        processorService.stubFor(
                method("ProcessData")
                        .willReturn(WireMockGrpc.Status.FAILED_PRECONDITION, message)
        );
    }

    /**
     * Mock a ProcessData response that returns gRPC UNAVAILABLE error.
     * Used to test module unavailability scenarios.
     *
     * @param message Error message
     */
    public void mockProcessDataUnavailable(String message) {
        processorService.stubFor(
                method("ProcessData")
                        .willReturn(WireMockGrpc.Status.UNAVAILABLE, message)
        );
    }

    // ============================================
    // Utility Methods
    // ============================================

    /**
     * Get a registered module configuration.
     *
     * @param moduleName Module name
     * @return Optional containing the module config, or empty if not found
     */
    public Optional<ModuleConfig> getModuleConfig(String moduleName) {
        return Optional.ofNullable(registeredModules.get(moduleName));
    }

    /**
     * Check if a module requires blob data (is a parser).
     *
     * @param moduleName Module name
     * @return true if the module requires blob data
     */
    public boolean moduleRequiresBlob(String moduleName) {
        return registeredModules.containsKey(moduleName) &&
               registeredModules.get(moduleName).requiresBlob;
    }

    /**
     * Get all registered module names.
     *
     * @return Set of module names
     */
    public Set<String> getRegisteredModules() {
        return Collections.unmodifiableSet(registeredModules.keySet());
    }

    /**
     * Reset all WireMock stubs for the processor service.
     */
    public void reset() {
        processorService.resetAll();
        registeredModules.clear();
    }

    // ============================================
    // Inner Classes
    // ============================================

    /**
     * Configuration for a mock module.
     */
    public static class ModuleConfig {
        public final String moduleName;
        public final String version;
        public final String displayName;
        public final String description;
        public final List<CapabilityType> capabilities;
        public final boolean requiresBlob;

        public ModuleConfig(String moduleName, String version, String displayName, String description,
                           List<CapabilityType> capabilities, boolean requiresBlob) {
            this.moduleName = moduleName;
            this.version = version;
            this.displayName = displayName;
            this.description = description;
            this.capabilities = List.copyOf(capabilities);
            this.requiresBlob = requiresBlob;
        }

        public boolean isParser() {
            return capabilities.contains(CapabilityType.CAPABILITY_TYPE_PARSER);
        }

        public boolean isSink() {
            return capabilities.contains(CapabilityType.CAPABILITY_TYPE_SINK);
        }
    }
}
