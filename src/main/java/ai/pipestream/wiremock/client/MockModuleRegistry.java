package ai.pipestream.wiremock.client;

import ai.pipestream.data.module.v1.CapabilityType;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Central registry for configuring mock module behaviors.
 * <p>
 * This registry coordinates between different mock services to provide a unified
 * API for setting up test scenarios. It manages:
 * <ul>
 *   <li>Module capabilities via {@link PipeStepProcessorMock}</li>
 *   <li>Module discovery via {@link PlatformRegistrationServiceMock}</li>
 *   <li>Blob storage via {@link FilesystemServiceMock}</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * MockModuleRegistry registry = new MockModuleRegistry(wireMock);
 *
 * // Register a parser module that requires blob data
 * registry.registerParserModule("tika-parser")
 *     .version("1.0.0")
 *     .host("localhost", 50053)
 *     .supportedMimeTypes("application/pdf", "text/plain")
 *     .build();
 *
 * // Set up a complete parser flow scenario
 * registry.setupParserFlowScenario();
 * }</pre>
 */
public class MockModuleRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(MockModuleRegistry.class);

    private final WireMock wireMock;
    private final PipeStepProcessorMock processorMock;
    private final PlatformRegistrationServiceMock registrationMock;
    private final FilesystemServiceMock filesystemMock;

    // Track all registered modules
    private final Map<String, RegisteredModule> modules = new HashMap<>();

    /**
     * Create a new registry with the given WireMock client.
     *
     * @param wireMock The WireMock client instance
     */
    public MockModuleRegistry(WireMock wireMock) {
        this.wireMock = wireMock;
        this.processorMock = new PipeStepProcessorMock(wireMock);
        this.registrationMock = new PlatformRegistrationServiceMock(wireMock);
        this.filesystemMock = new FilesystemServiceMock(wireMock);
    }

    // ============================================
    // Module Registration Builders
    // ============================================

    /**
     * Start registering a parser module.
     *
     * @param moduleName Module name
     * @return Builder for parser module configuration
     */
    public ParserModuleBuilder registerParserModule(String moduleName) {
        return new ParserModuleBuilder(moduleName, this);
    }

    /**
     * Start registering a chunker module.
     *
     * @param moduleName Module name
     * @return Builder for chunker module configuration
     */
    public ChunkerModuleBuilder registerChunkerModule(String moduleName) {
        return new ChunkerModuleBuilder(moduleName, this);
    }

    /**
     * Start registering an embedder module.
     *
     * @param moduleName Module name
     * @return Builder for embedder module configuration
     */
    public EmbedderModuleBuilder registerEmbedderModule(String moduleName) {
        return new EmbedderModuleBuilder(moduleName, this);
    }

    /**
     * Start registering a sink module.
     *
     * @param moduleName Module name
     * @return Builder for sink module configuration
     */
    public SinkModuleBuilder registerSinkModule(String moduleName) {
        return new SinkModuleBuilder(moduleName, this);
    }

    // ============================================
    // Scenario Setup Methods
    // ============================================

    /**
     * Set up a complete parser flow scenario.
     * <p>
     * This creates:
     * <ul>
     *   <li>Parser module (tika-parser) with PARSER capability</li>
     *   <li>Chunker module (text-chunker)</li>
     *   <li>Embedder module (openai-embedder)</li>
     *   <li>Test blobs for hydration testing</li>
     * </ul>
     */
    public void setupParserFlowScenario() {
        LOG.info("Setting up parser flow scenario");

        // Register parser module
        registerParserModule("tika-parser")
                .version("1.0.0")
                .displayName("Tika Document Parser")
                .description("Extracts text and metadata using Apache Tika")
                .host("localhost", 50053)
                .supportedMimeTypes("application/pdf", "text/plain", "text/html")
                .build();

        // Register chunker module
        registerChunkerModule("text-chunker")
                .version("1.0.0")
                .displayName("Text Chunker")
                .description("Splits text into chunks")
                .host("localhost", 50055)
                .chunkSize(512)
                .overlap(50)
                .build();

        // Register embedder module
        registerEmbedderModule("openai-embedder")
                .version("1.0.0")
                .displayName("OpenAI Embedder")
                .description("Generates embeddings using OpenAI")
                .host("localhost", 50057)
                .model("text-embedding-ada-002")
                .dimensions(1536)
                .build();

        // Register test blobs
        filesystemMock.registerTestBlob("test-pdf-1mb", "application/pdf", 1024 * 1024);
        filesystemMock.registerTestBlob("test-text-1kb", "text/plain", 1024);

        LOG.info("Parser flow scenario configured with {} modules", modules.size());
    }

    /**
     * Set up a direct flow scenario (no parser, no hydration needed).
     * <p>
     * This creates:
     * <ul>
     *   <li>Chunker module that works with pre-parsed metadata</li>
     *   <li>Embedder module</li>
     *   <li>Sink module</li>
     * </ul>
     */
    public void setupDirectFlowScenario() {
        LOG.info("Setting up direct flow scenario (no parser)");

        // Register chunker module
        registerChunkerModule("semantic-chunker")
                .version("1.0.0")
                .displayName("Semantic Chunker")
                .description("Splits text based on semantic boundaries")
                .host("localhost", 50056)
                .chunkSize(1024)
                .build();

        // Register embedder module
        registerEmbedderModule("sentence-transformer")
                .version("1.0.0")
                .displayName("Sentence Transformer Embedder")
                .description("Generates embeddings using sentence transformers")
                .host("localhost", 50058)
                .model("all-MiniLM-L6-v2")
                .dimensions(384)
                .build();

        // Register sink module
        registerSinkModule("opensearch-sink")
                .version("1.0.0")
                .displayName("OpenSearch Sink")
                .description("Writes documents to OpenSearch")
                .host("localhost", 50059)
                .targetIndex("documents")
                .build();

        LOG.info("Direct flow scenario configured with {} modules", modules.size());
    }

    /**
     * Set up error scenarios for testing error handling.
     */
    public void setupErrorScenarios() {
        LOG.info("Setting up error scenarios");

        // Module not found
        registrationMock.mockGetModuleNotFound("nonexistent-module");

        // Blob not found
        filesystemMock.mockGetFilesystemNodeNotFound("nonexistent-blob", "test-drive");

        // Storage unavailable
        filesystemMock.mockGetFilesystemNodeUnavailable("unavailable-blob", "test-drive");

        // Processor failure
        processorMock.mockProcessDataFailure("Simulated processing error", "PROCESSING_ERROR");

        // Blob not hydrated error for parser
        processorMock.mockProcessDataBlobNotHydrated("Parser requires inline blob data - blob not hydrated");

        LOG.info("Error scenarios configured");
    }

    /**
     * Set up large file testing scenario.
     *
     * @param sizeMb Size in megabytes
     */
    public void setupLargeFileScenario(int sizeMb) {
        LOG.info("Setting up large file scenario ({}MB)", sizeMb);

        // Register parser module
        registerParserModule("tika-parser")
                .version("1.0.0")
                .host("localhost", 50053)
                .build();

        // Register large test blob
        int sizeBytes = sizeMb * 1024 * 1024;
        filesystemMock.registerTestBlob("large-test-blob", "application/pdf", sizeBytes);

        LOG.info("Large file scenario configured with {}MB blob", sizeMb);
    }

    // ============================================
    // Utility Methods
    // ============================================

    /**
     * Set the active module whose capabilities will be returned by GetServiceRegistration.
     * <p>
     * This is a convenience method that delegates to {@link PipeStepProcessorMock#setActiveModule(String)}.
     * Call this before each test that needs specific module capabilities.
     *
     * @param moduleName Name of the module to activate
     * @throws IllegalArgumentException if the module has not been registered
     */
    public void setActiveModule(String moduleName) {
        if (!modules.containsKey(moduleName)) {
            throw new IllegalArgumentException("Module not registered in MockModuleRegistry: " + moduleName +
                    ". Available modules: " + modules.keySet());
        }
        processorMock.setActiveModule(moduleName);
        LOG.info("Set active module to: {}", moduleName);
    }

    /**
     * Get all registered modules.
     *
     * @return Map of module name to registered module
     */
    public Map<String, RegisteredModule> getRegisteredModules() {
        return Collections.unmodifiableMap(modules);
    }

    /**
     * Get modules with a specific capability.
     *
     * @param capability Capability type
     * @return List of module names with the capability
     */
    public List<String> getModulesWithCapability(CapabilityType capability) {
        return modules.entrySet().stream()
                .filter(e -> e.getValue().capabilities.contains(capability))
                .map(Map.Entry::getKey)
                .toList();
    }

    /**
     * Check if a module requires blob data.
     *
     * @param moduleName Module name
     * @return true if the module requires blob data
     */
    public boolean moduleRequiresBlob(String moduleName) {
        RegisteredModule module = modules.get(moduleName);
        return module != null && module.requiresBlob;
    }

    /**
     * Get the processor mock for advanced configuration.
     *
     * @return PipeStepProcessorMock instance
     */
    public PipeStepProcessorMock getProcessorMock() {
        return processorMock;
    }

    /**
     * Get the registration mock for advanced configuration.
     *
     * @return PlatformRegistrationServiceMock instance
     */
    public PlatformRegistrationServiceMock getRegistrationMock() {
        return registrationMock;
    }

    /**
     * Get the filesystem mock for advanced configuration.
     *
     * @return FilesystemServiceMock instance
     */
    public FilesystemServiceMock getFilesystemMock() {
        return filesystemMock;
    }

    /**
     * Reset all mocks and registered modules.
     */
    public void reset() {
        processorMock.reset();
        registrationMock.reset();
        filesystemMock.reset();
        modules.clear();
        LOG.info("All mocks reset");
    }

    // ============================================
    // Internal Registration
    // ============================================

    void registerModule(RegisteredModule module) {
        modules.put(module.moduleName, module);
        LOG.debug("Registered module: {}", module.moduleName);
    }

    // ============================================
    // Builder Classes
    // ============================================

    /**
     * Builder for parser module registration.
     */
    public static class ParserModuleBuilder {
        private final String moduleName;
        private final MockModuleRegistry registry;
        private String version = "1.0.0";
        private String displayName;
        private String description = "";
        private String host = "localhost";
        private int port = 50053;
        private final List<String> supportedMimeTypes = new ArrayList<>();

        ParserModuleBuilder(String moduleName, MockModuleRegistry registry) {
            this.moduleName = moduleName;
            this.displayName = moduleName;
            this.registry = registry;
        }

        public ParserModuleBuilder version(String version) {
            this.version = version;
            return this;
        }

        public ParserModuleBuilder displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public ParserModuleBuilder description(String description) {
            this.description = description;
            return this;
        }

        public ParserModuleBuilder host(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        public ParserModuleBuilder supportedMimeTypes(String... mimeTypes) {
            this.supportedMimeTypes.addAll(Arrays.asList(mimeTypes));
            return this;
        }

        public MockModuleRegistry build() {
            // Register with processor mock
            registry.processorMock.registerParserModule(moduleName, version, displayName, description);

            // Register with platform registration mock
            registry.registrationMock.registerModule(
                    moduleName,
                    moduleName + "-service-1",
                    host,
                    port,
                    List.of("PARSER"),
                    Map.of("supported_mime_types", String.join(",", supportedMimeTypes))
            );

            // Track registration
            RegisteredModule module = new RegisteredModule(
                    moduleName, version, displayName, description,
                    List.of(CapabilityType.CAPABILITY_TYPE_PARSER),
                    true, host, port
            );
            registry.registerModule(module);

            return registry;
        }
    }

    /**
     * Builder for chunker module registration.
     */
    public static class ChunkerModuleBuilder {
        private final String moduleName;
        private final MockModuleRegistry registry;
        private String version = "1.0.0";
        private String displayName;
        private String description = "";
        private String host = "localhost";
        private int port = 50055;
        private int chunkSize = 512;
        private int overlap = 50;

        ChunkerModuleBuilder(String moduleName, MockModuleRegistry registry) {
            this.moduleName = moduleName;
            this.displayName = moduleName;
            this.registry = registry;
        }

        public ChunkerModuleBuilder version(String version) {
            this.version = version;
            return this;
        }

        public ChunkerModuleBuilder displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public ChunkerModuleBuilder description(String description) {
            this.description = description;
            return this;
        }

        public ChunkerModuleBuilder host(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        public ChunkerModuleBuilder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public ChunkerModuleBuilder overlap(int overlap) {
            this.overlap = overlap;
            return this;
        }

        public MockModuleRegistry build() {
            // Register with processor mock
            registry.processorMock.registerChunkerModule(moduleName, version, displayName, description);

            // Register with platform registration mock
            registry.registrationMock.registerModule(
                    moduleName,
                    moduleName + "-service-1",
                    host,
                    port,
                    List.of(),
                    Map.of("chunk_size", String.valueOf(chunkSize), "overlap", String.valueOf(overlap))
            );

            // Track registration
            RegisteredModule module = new RegisteredModule(
                    moduleName, version, displayName, description,
                    List.of(),
                    false, host, port
            );
            registry.registerModule(module);

            return registry;
        }
    }

    /**
     * Builder for embedder module registration.
     */
    public static class EmbedderModuleBuilder {
        private final String moduleName;
        private final MockModuleRegistry registry;
        private String version = "1.0.0";
        private String displayName;
        private String description = "";
        private String host = "localhost";
        private int port = 50057;
        private String model = "text-embedding-ada-002";
        private int dimensions = 1536;

        EmbedderModuleBuilder(String moduleName, MockModuleRegistry registry) {
            this.moduleName = moduleName;
            this.displayName = moduleName;
            this.registry = registry;
        }

        public EmbedderModuleBuilder version(String version) {
            this.version = version;
            return this;
        }

        public EmbedderModuleBuilder displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public EmbedderModuleBuilder description(String description) {
            this.description = description;
            return this;
        }

        public EmbedderModuleBuilder host(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        public EmbedderModuleBuilder model(String model) {
            this.model = model;
            return this;
        }

        public EmbedderModuleBuilder dimensions(int dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public MockModuleRegistry build() {
            // Register with processor mock
            registry.processorMock.registerEmbedderModule(moduleName, version, displayName, description);

            // Register with platform registration mock
            registry.registrationMock.registerModule(
                    moduleName,
                    moduleName + "-service-1",
                    host,
                    port,
                    List.of(),
                    Map.of("model", model, "dimensions", String.valueOf(dimensions))
            );

            // Track registration
            RegisteredModule module = new RegisteredModule(
                    moduleName, version, displayName, description,
                    List.of(),
                    false, host, port
            );
            registry.registerModule(module);

            return registry;
        }
    }

    /**
     * Builder for sink module registration.
     */
    public static class SinkModuleBuilder {
        private final String moduleName;
        private final MockModuleRegistry registry;
        private String version = "1.0.0";
        private String displayName;
        private String description = "";
        private String host = "localhost";
        private int port = 50059;
        private String targetIndex = "documents";

        SinkModuleBuilder(String moduleName, MockModuleRegistry registry) {
            this.moduleName = moduleName;
            this.displayName = moduleName;
            this.registry = registry;
        }

        public SinkModuleBuilder version(String version) {
            this.version = version;
            return this;
        }

        public SinkModuleBuilder displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public SinkModuleBuilder description(String description) {
            this.description = description;
            return this;
        }

        public SinkModuleBuilder host(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        public SinkModuleBuilder targetIndex(String targetIndex) {
            this.targetIndex = targetIndex;
            return this;
        }

        public MockModuleRegistry build() {
            // Register with processor mock
            registry.processorMock.registerSinkModule(moduleName, version, displayName, description);

            // Register with platform registration mock
            registry.registrationMock.registerModule(
                    moduleName,
                    moduleName + "-service-1",
                    host,
                    port,
                    List.of("SINK"),
                    Map.of("target_index", targetIndex)
            );

            // Track registration
            RegisteredModule module = new RegisteredModule(
                    moduleName, version, displayName, description,
                    List.of(CapabilityType.CAPABILITY_TYPE_SINK),
                    false, host, port
            );
            registry.registerModule(module);

            return registry;
        }
    }

    // ============================================
    // Data Classes
    // ============================================

    /**
     * Represents a registered module.
     */
    public static class RegisteredModule {
        public final String moduleName;
        public final String version;
        public final String displayName;
        public final String description;
        public final List<CapabilityType> capabilities;
        public final boolean requiresBlob;
        public final String host;
        public final int port;

        public RegisteredModule(String moduleName, String version, String displayName,
                               String description, List<CapabilityType> capabilities,
                               boolean requiresBlob, String host, int port) {
            this.moduleName = moduleName;
            this.version = version;
            this.displayName = displayName;
            this.description = description;
            this.capabilities = List.copyOf(capabilities);
            this.requiresBlob = requiresBlob;
            this.host = host;
            this.port = port;
        }

        public boolean isParser() {
            return capabilities.contains(CapabilityType.CAPABILITY_TYPE_PARSER);
        }

        public boolean isSink() {
            return capabilities.contains(CapabilityType.CAPABILITY_TYPE_SINK);
        }
    }
}
