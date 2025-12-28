package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.*;
import ai.pipestream.data.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
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
 *
 * <h2>Module Identification via gRPC Metadata</h2>
 * <p>
 * This mock supports two ways to identify which module's capabilities should be returned:
 *
 * <h3>1. Header-Based Matching (Recommended)</h3>
 * <p>
 * Clients can send the {@value #MODULE_NAME_HEADER} gRPC metadata header with the module name.
 * The mock will return the correct capabilities for that specific module, regardless of which
 * module was last registered or set as active.
 * <p>
 * <b>Client Example (Java gRPC):</b>
 * <pre>{@code
 * Metadata metadata = new Metadata();
 * metadata.put(Metadata.Key.of("x-module-name", Metadata.ASCII_STRING_MARSHALLER), "tika-parser");
 * stub = PipeStepProcessorServiceGrpc.newBlockingStub(channel)
 *     .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
 * GetServiceRegistrationResponse response = stub.getServiceRegistration(request);
 * // Returns tika-parser capabilities (PARSER)
 * }</pre>
 *
 * <h3>2. Active Module Fallback</h3>
 * <p>
 * If no header is provided, the mock returns the "active module's" capabilities.
 * Use {@link #setActiveModule(String)} to set which module is active.
 * <p>
 * <b>Example:</b>
 * <pre>{@code
 * processorMock.setActiveModule("tika-parser");
 * // Calls without x-module-name header return PARSER capability
 *
 * processorMock.setActiveModule("text-chunker");
 * // Calls without x-module-name header return no special capabilities
 * }</pre>
 *
 * <h2>Priority Order</h2>
 * <ol>
 *   <li>Header-based stubs (priority 1) - matched when {@code x-module-name} header is present</li>
 *   <li>Active module stub (lower priority) - fallback when no header is present</li>
 * </ol>
 * <p>
 * This class implements {@link ServiceMockInitializer} and will be automatically
 * discovered and initialized at server startup by the {@link ServiceMockRegistry}.
 */
public class PipeStepProcessorMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(PipeStepProcessorMock.class);

    private static final String SERVICE_NAME = PipeStepProcessorServiceGrpc.SERVICE_NAME;

    /**
     * gRPC metadata header name for module identification.
     * Clients should send this header with the module name to get module-specific responses.
     * Header names in gRPC metadata are lowercase.
     */
    public static final String MODULE_NAME_HEADER = "x-module-name";

    private WireMock wireMock;
    private WireMockGrpcService processorService;

    // Track registered modules for dynamic behavior
    private final Map<String, ModuleConfig> registeredModules = new HashMap<>();

    // Track header-based stub mappings for cleanup
    private final List<UUID> headerBasedStubIds = new ArrayList<>();

    /**
     * Create a helper for the given WireMock client.
     *
     * @param wireMock The WireMock client instance (connected to WireMock server)
     */
    public PipeStepProcessorMock(WireMock wireMock) {
        this.wireMock = wireMock;
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
        this.wireMock = wireMock;
        this.processorService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default PipeStepProcessorService stubs");

        // IMPORTANT: Registration order matters!
        // The last registered module's capabilities will be returned by GetServiceRegistration.
        // We register sinks last so that by default, GetServiceRegistration returns SINK capability.
        // Tests should call setActiveModule() to explicitly set the module under test.

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
     * Set the active module whose capabilities will be returned by GetServiceRegistration.
     * <p>
     * This method should be called before each test that needs specific module capabilities.
     * It updates the WireMock stub to return the specified module's capabilities for all
     * subsequent GetServiceRegistration calls.
     *
     * @param moduleName Name of the module to activate (must have been previously registered)
     * @throws IllegalArgumentException if the module has not been registered
     */
    public void setActiveModule(String moduleName) {
        ModuleConfig config = registeredModules.get(moduleName);
        if (config == null) {
            throw new IllegalArgumentException("Module not registered: " + moduleName +
                    ". Available modules: " + registeredModules.keySet());
        }
        mockGetServiceRegistration(config);
        LOG.info("Set active module to: {} (capabilities: {})", moduleName, config.capabilities);
    }

    /**
     * Mock GetServiceRegistration response for a module.
     * <p>
     * This method sets up two stubs:
     * <ol>
     *   <li>A header-based stub that matches requests with the {@code x-module-name} header
     *       set to this module's name. This stub has higher priority.</li>
     *   <li>A fallback stub that matches any request without the header (or with an unknown
     *       module name). This is the "active module" behavior.</li>
     * </ol>
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

        // Create header-based stub with higher priority for module-specific matching
        // gRPC requests to GetServiceRegistration go to: POST /{service}/GetServiceRegistration
        String grpcPath = "/" + SERVICE_NAME + "/GetServiceRegistration";
        if (wireMock != null) {
            try {
                String responseJson = JsonFormat.printer().print(response);

                // Higher priority stub that matches on x-module-name header
                // Use the wireMock instance to register on the correct server (not static stubFor!)
                StubMapping headerStub = wireMock.register(
                        post(urlPathEqualTo(grpcPath))
                                .withHeader(MODULE_NAME_HEADER, equalTo(config.moduleName))
                                .atPriority(1) // Higher priority (lower number = higher priority)
                                .willReturn(okJson(responseJson))
                );
                headerBasedStubIds.add(headerStub.getId());
                LOG.debug("Created header-based stub for module: {} (id: {})", config.moduleName, headerStub.getId());
            } catch (Exception e) {
                LOG.warn("Failed to create header-based stub for module {}: {}", config.moduleName, e.getMessage());
            }
        } else {
            LOG.debug("WireMock instance not set, skipping header-based stub for module: {}", config.moduleName);
        }

        // Fallback stub using gRPC DSL (lower priority - matches when no header present)
        // This is the "active module" stub that gets overwritten
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
        // Remove header-based stubs by ID using the stub mapping we saved
        if (wireMock != null) {
            for (UUID stubId : headerBasedStubIds) {
                try {
                    // Use removeStub with a MappingBuilder that has the ID
                    wireMock.removeStub(post(anyUrl()).withId(stubId));
                } catch (Exception e) {
                    LOG.debug("Could not remove stub {}: {}", stubId, e.getMessage());
                }
            }
        }
        headerBasedStubIds.clear();

        // Reset gRPC service stubs
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
