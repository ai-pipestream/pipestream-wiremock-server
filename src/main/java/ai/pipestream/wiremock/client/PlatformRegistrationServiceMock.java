package ai.pipestream.wiremock.client;

import ai.pipestream.platform.registration.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.time.Instant;
import java.util.*;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * Server-side helper for configuring PlatformRegistrationService mocks in WireMock.
 * <p>
 * This mock supports testing of module discovery and capability queries including:
 * <ul>
 *   <li><b>GetModule</b>: Returns module details with capabilities</li>
 *   <li><b>ListPlatformModules</b>: Returns list of available modules</li>
 *   <li><b>GetService</b>: Returns service details</li>
 *   <li><b>ListServices</b>: Returns list of registered services</li>
 * </ul>
 * <p>
 * This class implements {@link ServiceMockInitializer} and will be automatically
 * discovered and initialized at server startup by the {@link ServiceMockRegistry}.
 */
public class PlatformRegistrationServiceMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(PlatformRegistrationServiceMock.class);

    private static final String SERVICE_NAME = PlatformRegistrationServiceGrpc.SERVICE_NAME;
    private static final String DEFAULT_HTTP_SCHEMA_VERSION = "1.0.0";

    private WireMockGrpcService registrationService;

    // Track registered modules
    private final Map<String, ModuleInfo> registeredModules = new HashMap<>();

    // Track registered services
    private final Map<String, ServiceInfo> registeredServices = new HashMap<>();

    /**
     * Create a helper for the given WireMock client.
     *
     * @param wireMock The WireMock client instance (connected to WireMock server)
     */
    public PlatformRegistrationServiceMock(WireMock wireMock) {
        this.registrationService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    /**
     * Default constructor for ServiceLoader discovery.
     */
    public PlatformRegistrationServiceMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.registrationService = new WireMockGrpcService(wireMock, SERVICE_NAME);
        MockConfig config = new MockConfig();

        LOG.info("Initializing default PlatformRegistrationService stubs");

        // Register default parser modules with PARSER capability
        registerModule("tika-parser", "tika-parser-service-1",
                "localhost", 50053,
                List.of("PARSER"),
                Map.of("input_format", "binary", "output_format", "pipedoc"));

        registerModule("docling-parser", "docling-parser-service-1",
                "localhost", 50054,
                List.of("PARSER"),
                Map.of("input_format", "binary", "output_format", "structured"));

        // Register chunker modules (no special capabilities)
        registerModule("text-chunker", "text-chunker-service-1",
                "localhost", 50055,
                List.of(),
                Map.of("chunk_size", "512", "overlap", "50"));

        registerModule("semantic-chunker", "semantic-chunker-service-1",
                "localhost", 50056,
                List.of(),
                Map.of("model", "sentence-transformers"));

        // Register embedder modules (no special capabilities)
        registerModule("openai-embedder", "openai-embedder-service-1",
                "localhost", 50057,
                List.of(),
                Map.of("model", "text-embedding-ada-002", "dimensions", "1536"));

        // Register sink modules with SINK capability
        registerModule("opensearch-sink", "opensearch-sink-service-1",
                "localhost", 50058,
                List.of("SINK"),
                Map.of("index", "documents"));

        // Set up default stubs
        setupDefaultStubs();
        mockGetConnectorSchema(
                config.get("wiremock.registration.GetConnectorSchema.default.connectorType", "s3"),
                config.get("wiremock.registration.GetConnectorSchema.default.schemaJson", "{\"type\":\"object\",\"properties\":{}}"),
                config.get("wiremock.registration.GetConnectorSchema.default.schemaVersion", DEFAULT_HTTP_SCHEMA_VERSION),
                config.get("wiremock.registration.GetConnectorSchema.default.artifactId", "connector-schema-s3"),
                List.of(DEFAULT_HTTP_SCHEMA_VERSION)
        );

        LOG.info("Added {} module stubs for PlatformRegistrationService", registeredModules.size());
    }

    // ============================================
    // Module Registration Methods
    // ============================================

    /**
     * Register a module with the mock registry.
     *
     * @param moduleName   Module name (logical name)
     * @param serviceId    Consul service ID
     * @param host         Host address
     * @param port         Port number
     * @param capabilities List of capability strings (e.g., "PARSER", "SINK")
     * @param metadata     Additional metadata
     */
    public void registerModule(String moduleName, String serviceId, String host, int port,
                                List<String> capabilities, Map<String, String> metadata) {
        ModuleInfo moduleInfo = new ModuleInfo(moduleName, serviceId, host, port, capabilities, metadata);
        registeredModules.put(moduleName, moduleInfo);
        mockGetModule(moduleInfo);
        LOG.debug("Registered module: {} at {}:{}", moduleName, host, port);
    }

    /**
     * Register a service with the mock registry.
     *
     * @param serviceName Service name
     * @param serviceId   Consul service ID
     * @param host        Host address
     * @param port        Port number
     * @param serviceType Service type
     */
    public void registerService(String serviceName, String serviceId, String host, int port,
                                 ServiceType serviceType) {
        ServiceInfo serviceInfo = new ServiceInfo(serviceName, serviceId, host, port, serviceType);
        registeredServices.put(serviceName, serviceInfo);
        mockGetService(serviceInfo);
        LOG.debug("Registered service: {} at {}:{}", serviceName, host, port);
    }

    // ============================================
    // GetModule Mocks
    // ============================================

    /**
     * Set up default stubs.
     */
    private void setupDefaultStubs() {
        // Set up ListPlatformModules to return all registered modules
        mockListPlatformModules();
    }

    /**
     * Mock GetModule to return module details.
     *
     * @param moduleInfo Module information
     */
    private void mockGetModule(ModuleInfo moduleInfo) {
        // Mock lookup by module name
        GetModuleRequest requestByName = GetModuleRequest.newBuilder()
                .setModuleName(moduleInfo.moduleName)
                .build();

        GetModuleResponse response = buildGetModuleResponse(moduleInfo);

        registrationService.stubFor(
                method("GetModule")
                        .withRequestMessage(WireMockGrpc.equalToMessage(requestByName))
                        .willReturn(message(response))
        );

        // Mock lookup by service ID
        GetModuleRequest requestByServiceId = GetModuleRequest.newBuilder()
                .setServiceId(moduleInfo.serviceId)
                .build();

        registrationService.stubFor(
                method("GetModule")
                        .withRequestMessage(WireMockGrpc.equalToMessage(requestByServiceId))
                        .willReturn(message(response))
        );
    }

    /**
     * Build GetModuleResponse from module info.
     */
    private GetModuleResponse buildGetModuleResponse(ModuleInfo moduleInfo) {
        GetModuleResponse.Builder responseBuilder = GetModuleResponse.newBuilder()
                .setModuleName(moduleInfo.moduleName)
                .setServiceId(moduleInfo.serviceId)
                .setHost(moduleInfo.host)
                .setPort(moduleInfo.port)
                .setIsHealthy(true);

        // Add metadata
        for (Map.Entry<String, String> entry : moduleInfo.metadata.entrySet()) {
            responseBuilder.putMetadata(entry.getKey(), entry.getValue());
        }

        return responseBuilder.build();
    }

    /**
     * Mock GetModule to return NOT_FOUND for a specific module.
     *
     * @param moduleName Module name
     */
    public void mockGetModuleNotFound(String moduleName) {
        GetModuleRequest request = GetModuleRequest.newBuilder()
                .setModuleName(moduleName)
                .build();

        registrationService.stubFor(
                method("GetModule")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "Module not found: " + moduleName)
        );
    }

    // ============================================
    // ListPlatformModules Mocks
    // ============================================

    /**
     * Mock ListPlatformModules to return all registered modules.
     */
    public void mockListPlatformModules() {
        ListPlatformModulesRequest request = ListPlatformModulesRequest.getDefaultInstance();

        ListPlatformModulesResponse.Builder responseBuilder = ListPlatformModulesResponse.newBuilder();

        for (ModuleInfo moduleInfo : registeredModules.values()) {
            responseBuilder.addModules(buildGetModuleResponse(moduleInfo));
        }

        registrationService.stubFor(
                method("ListPlatformModules")
                        .willReturn(message(responseBuilder.build()))
        );
    }

    // ============================================
    // GetService Mocks
    // ============================================

    /**
     * Mock GetService to return service details.
     *
     * @param serviceInfo Service information
     */
    private void mockGetService(ServiceInfo serviceInfo) {
        GetServiceRequest request = GetServiceRequest.newBuilder()
                .setServiceName(serviceInfo.serviceName)
                .build();

        GetServiceResponse response = buildGetServiceResponse(serviceInfo);

        registrationService.stubFor(
                method("GetService")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );
    }

    /**
     * Mock GetService to return NOT_FOUND for a specific service.
     *
     * @param serviceName Service name
     */
    public void mockGetServiceNotFound(String serviceName) {
        GetServiceRequest request = GetServiceRequest.newBuilder()
                .setServiceName(serviceName)
                .build();

        registrationService.stubFor(
                method("GetService")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(WireMockGrpc.Status.NOT_FOUND, "Service not found: " + serviceName)
        );
    }

    // ============================================
    // ListServices Mocks
    // ============================================

    /**
     * Mock ListServices to return all registered services.
     */
    public void mockListServices() {
        ListServicesRequest request = ListServicesRequest.getDefaultInstance();

        ListServicesResponse.Builder responseBuilder = ListServicesResponse.newBuilder();

        for (ServiceInfo serviceInfo : registeredServices.values()) {
            responseBuilder.addServices(buildGetServiceResponse(serviceInfo));
        }

        registrationService.stubFor(
                method("ListServices")
                        .willReturn(message(responseBuilder.build()))
        );
    }

    /**
     * Build GetServiceResponse from service info.
     */
    private GetServiceResponse buildGetServiceResponse(ServiceInfo serviceInfo) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        return GetServiceResponse.newBuilder()
                .setServiceId(serviceInfo.serviceId)
                .setServiceName(serviceInfo.serviceName)
                .setHost(serviceInfo.host)
                .setPort(serviceInfo.port)
                .setIsHealthy(true)
                .setRegisteredAt(timestamp)
                .addHttpEndpoints(buildDefaultHttpEndpoint(serviceInfo))
                .setHttpSchemaArtifactId(serviceInfo.serviceName + "-http-schema")
                .setHttpSchemaVersion(DEFAULT_HTTP_SCHEMA_VERSION)
                .build();
    }

    private HttpEndpoint buildDefaultHttpEndpoint(ServiceInfo serviceInfo) {
        return HttpEndpoint.newBuilder()
                .setScheme("http")
                .setHost(serviceInfo.host)
                .setPort(serviceInfo.port)
                .setBasePath("/" + serviceInfo.serviceName)
                .setHealthPath("/q/health")
                .setTlsEnabled(false)
                .build();
    }

    /**
     * Mock GetConnectorSchema to return connector schema details.
     */
    public void mockGetConnectorSchema(String connectorType, String schemaJson, String schemaVersion,
                                       String artifactId, List<String> availableVersions) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        GetConnectorSchemaResponse response = GetConnectorSchemaResponse.newBuilder()
                .setConnectorType(connectorType)
                .setSchemaJson(schemaJson)
                .setSchemaVersion(schemaVersion)
                .setArtifactId(artifactId)
                .setUpdatedAt(timestamp)
                .addAllAvailableVersions(availableVersions)
                .build();

        GetConnectorSchemaRequest request = GetConnectorSchemaRequest.newBuilder()
                .setConnectorType(connectorType)
                .build();

        registrationService.stubFor(
                method("GetConnectorSchema")
                        .withRequestMessage(WireMockGrpc.equalToMessage(request))
                        .willReturn(message(response))
        );

        if (schemaVersion != null && !schemaVersion.isBlank()) {
            GetConnectorSchemaRequest versionedRequest = GetConnectorSchemaRequest.newBuilder()
                    .setConnectorType(connectorType)
                    .setVersion(schemaVersion)
                    .build();
            registrationService.stubFor(
                    method("GetConnectorSchema")
                            .withRequestMessage(WireMockGrpc.equalToMessage(versionedRequest))
                            .willReturn(message(response))
            );
        }
    }

    // ============================================
    // Utility Methods
    // ============================================

    /**
     * Check if a module has a specific capability.
     *
     * @param moduleName Module name
     * @param capability Capability string (e.g., "PARSER")
     * @return true if the module has the capability
     */
    public boolean moduleHasCapability(String moduleName, String capability) {
        ModuleInfo moduleInfo = registeredModules.get(moduleName);
        if (moduleInfo == null) return false;

        String normalizedCap = normalizeCapability(capability);
        return moduleInfo.capabilities.stream()
                .map(this::normalizeCapability)
                .anyMatch(c -> c.equals(normalizedCap));
    }

    /**
     * Normalize capability string (remove prefixes, uppercase).
     */
    private String normalizeCapability(String capability) {
        if (capability == null) return "";
        String normalized = capability.toUpperCase();
        // Remove common prefixes
        if (normalized.startsWith("CAPABILITY_TYPE_")) {
            normalized = normalized.substring("CAPABILITY_TYPE_".length());
        } else if (normalized.startsWith("CAPABILITY:")) {
            normalized = normalized.substring("CAPABILITY:".length());
        }
        return normalized;
    }

    /**
     * Get a registered module by name.
     *
     * @param moduleName Module name
     * @return Optional containing the module info, or empty if not found
     */
    public Optional<ModuleInfo> getModule(String moduleName) {
        return Optional.ofNullable(registeredModules.get(moduleName));
    }

    /**
     * Get all registered module names.
     *
     * @return Set of module names
     */
    public Set<String> getRegisteredModuleNames() {
        return Collections.unmodifiableSet(registeredModules.keySet());
    }

    /**
     * Get all modules with a specific capability.
     *
     * @param capability Capability string
     * @return List of module names with the capability
     */
    public List<String> getModulesWithCapability(String capability) {
        return registeredModules.entrySet().stream()
                .filter(e -> moduleHasCapability(e.getKey(), capability))
                .map(Map.Entry::getKey)
                .toList();
    }

    /**
     * Reset all WireMock stubs for the registration service.
     */
    public void reset() {
        registrationService.resetAll();
        registeredModules.clear();
        registeredServices.clear();
    }

    // ============================================
    // Inner Classes
    // ============================================

    /**
     * Module information for registration.
     */
    public static class ModuleInfo {
        public final String moduleName;
        public final String serviceId;
        public final String host;
        public final int port;
        public final List<String> capabilities;
        public final Map<String, String> metadata;

        public ModuleInfo(String moduleName, String serviceId, String host, int port,
                         List<String> capabilities, Map<String, String> metadata) {
            this.moduleName = moduleName;
            this.serviceId = serviceId;
            this.host = host;
            this.port = port;
            this.capabilities = List.copyOf(capabilities);
            this.metadata = Map.copyOf(metadata);
        }

        public boolean isParser() {
            return capabilities.stream()
                    .map(String::toUpperCase)
                    .anyMatch(c -> c.contains("PARSER"));
        }

        public boolean isSink() {
            return capabilities.stream()
                    .map(String::toUpperCase)
                    .anyMatch(c -> c.contains("SINK"));
        }
    }

    /**
     * Service information for registration.
     */
    public static class ServiceInfo {
        public final String serviceName;
        public final String serviceId;
        public final String host;
        public final int port;
        public final ServiceType serviceType;

        public ServiceInfo(String serviceName, String serviceId, String host, int port,
                          ServiceType serviceType) {
            this.serviceName = serviceName;
            this.serviceId = serviceId;
            this.host = host;
            this.port = port;
            this.serviceType = serviceType;
        }
    }
}
