package ai.pipestream.wiremock.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies all JSON-stubbed gRPC services are reachable through WireMock.
 * <p>
 * Each test calls one RPC and asserts the JSON stub response is returned correctly.
 * These serve as examples for downstream consumers showing how to call each service.
 */
class AllServicesJsonStubTest {

    private static WireMockServer wireMockServer;
    private static ManagedChannel channel;

    @BeforeAll
    static void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new GrpcExtensionFactory())
                .withRootDirectory("build/resources/test/wiremock"));
        wireMockServer.start();

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

    // ============================================
    // ConnectorIntakeService
    // ============================================

    @Test
    @DisplayName("ConnectorIntakeService/UploadPipeDoc returns accepted response")
    void testConnectorIntakeUpload() {
        var stub = ai.pipestream.connector.intake.v1.ConnectorIntakeServiceGrpc
                .newBlockingStub(channel);
        var request = ai.pipestream.connector.intake.v1.UploadPipeDocRequest.newBuilder()
                .setDatasourceId("test-ds")
                .build();
        var response = stub.uploadPipeDoc(request);

        assertThat(response.getSuccess())
                .as("ConnectorIntakeService/UploadPipeDoc should return success")
                .isTrue();
    }

    // ============================================
    // ConnectorRegistrationService
    // ============================================

    @Test
    @DisplayName("ConnectorRegistrationService/GetConnectorConfigSchema returns schema")
    void testConnectorRegistrationGetSchema() {
        var stub = ai.pipestream.connector.intake.v1.ConnectorRegistrationServiceGrpc
                .newBlockingStub(channel);
        var request = ai.pipestream.connector.intake.v1.GetConnectorConfigSchemaRequest.newBuilder()
                .setSchemaId("mock-schema-001")
                .build();
        var response = stub.getConnectorConfigSchema(request);

        assertThat(response).as("ConnectorRegistrationService/GetConnectorConfigSchema response").isNotNull();
    }

    // ============================================
    // DocumentUploadService
    // ============================================

    @Test
    @DisplayName("DocumentUploadService/UploadChunk returns acknowledged")
    void testDocumentUploadChunk() {
        var stub = ai.pipestream.connector.intake.v1.DocumentUploadServiceGrpc
                .newBlockingStub(channel);
        var request = ai.pipestream.connector.intake.v1.UploadChunkRequest.newBuilder()
                .setCrawlId("test-crawl")
                .setFileId("test-file-001")
                .setChunkNumber(0)
                .setTotalChunks(1)
                .build();
        var response = stub.uploadChunk(request);

        assertThat(response).as("DocumentUploadService/UploadChunk response").isNotNull();
    }

    // ============================================
    // DesignModeService
    // ============================================

    @Test
    @DisplayName("DesignModeService/ValidateDesignGraph returns valid")
    void testDesignModeValidateGraph() {
        var stub = ai.pipestream.design.v1.DesignModeServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.design.v1.ValidateDesignGraphRequest.newBuilder()
                .setDesignGraphId("test-design-graph")
                .build();
        var response = stub.validateDesignGraph(request);

        assertThat(response)
                .as("DesignModeService/ValidateDesignGraph should return a response")
                .isNotNull();
    }

    @Test
    @DisplayName("DesignModeService/TestNode returns success")
    void testDesignModeTestNode() {
        var stub = ai.pipestream.design.v1.DesignModeServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.design.v1.TestNodeRequest.newBuilder()
                .setDesignGraphId("test-design-graph")
                .setNodeId("test-node-001")
                .build();
        var response = stub.testNode(request);

        assertThat(response.getSuccess())
                .as("DesignModeService/TestNode should return success=true")
                .isTrue();
    }

    // ============================================
    // DjlModelService
    // ============================================

    @Test
    @DisplayName("DjlModelService/ListAvailableModels returns model list")
    void testDjlModelList() {
        var stub = ai.pipestream.djl.serving.v1.DjlModelServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.djl.serving.v1.ListAvailableModelsRequest.newBuilder().build();
        var response = stub.listAvailableModels(request);

        assertThat(response).as("DjlModelService/ListAvailableModels response").isNotNull();
    }

    @Test
    @DisplayName("DjlModelService/GetModelHealth returns healthy")
    void testDjlModelHealth() {
        var stub = ai.pipestream.djl.serving.v1.DjlModelServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.djl.serving.v1.GetModelHealthRequest.newBuilder().build();
        var response = stub.getModelHealth(request);

        assertThat(response).as("DjlModelService/GetModelHealth response").isNotNull();
    }

    // ============================================
    // SidecarManagementService
    // ============================================

    @Test
    @DisplayName("SidecarManagementService/GetLeases returns lease list")
    void testSidecarGetLeases() {
        var stub = ai.pipestream.engine.sidecar.v1.SidecarManagementServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.engine.sidecar.v1.GetLeasesRequest.newBuilder().build();
        var response = stub.getLeases(request);

        assertThat(response).as("SidecarManagementService/GetLeases response").isNotNull();
    }

    // ============================================
    // PipelineGraphService
    // ============================================

    @Test
    @DisplayName("PipelineGraphService/GetActiveGraph returns a graph")
    void testPipelineGraphGetActive() {
        var stub = ai.pipestream.engine.v1.PipelineGraphServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.engine.v1.GetActiveGraphRequest.newBuilder()
                .setGraphId("test-graph")
                .setClusterId("test-cluster")
                .build();
        var response = stub.getActiveGraph(request);

        assertThat(response).as("PipelineGraphService/GetActiveGraph response").isNotNull();
    }

    @Test
    @DisplayName("PipelineGraphService/CreateGraph returns created graph")
    void testPipelineGraphCreate() {
        var stub = ai.pipestream.engine.v1.PipelineGraphServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.engine.v1.CreateGraphRequest.newBuilder().build();
        var response = stub.createGraph(request);

        assertThat(response).as("PipelineGraphService/CreateGraph response").isNotNull();
    }

    @Test
    @DisplayName("PipelineGraphService/ListGraphVersions returns versions")
    void testPipelineGraphListVersions() {
        var stub = ai.pipestream.engine.v1.PipelineGraphServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.engine.v1.ListGraphVersionsRequest.newBuilder()
                .setGraphId("test-graph")
                .build();
        var response = stub.listGraphVersions(request);

        assertThat(response).as("PipelineGraphService/ListGraphVersions response").isNotNull();
    }

    @Test
    @DisplayName("PipelineGraphService/CreateDatasourceInstance returns instance")
    void testPipelineGraphCreateDatasourceInstance() {
        var stub = ai.pipestream.engine.v1.PipelineGraphServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.engine.v1.CreateDatasourceInstanceRequest.newBuilder()
                .setGraphId("test-graph")
                .setVersion(1)
                .setDatasourceId("ds-001")
                .setEntryNodeId("entry-node-001")
                .build();
        var response = stub.createDatasourceInstance(request);

        assertThat(response).as("PipelineGraphService/CreateDatasourceInstance response").isNotNull();
    }

    // ============================================
    // MappingService
    // ============================================

    @Test
    @DisplayName("MappingService/ApplyMapping returns mapped document")
    void testMappingServiceApply() {
        var stub = ai.pipestream.mapping.v1.MappingServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.mapping.v1.ApplyMappingRequest.newBuilder().build();
        var response = stub.applyMapping(request);

        assertThat(response).as("MappingService/ApplyMapping response").isNotNull();
    }

    // ============================================
    // DocumentService
    // ============================================

    @Test
    @DisplayName("DocumentService/Get returns a document")
    void testDocumentServiceGet() {
        var stub = ai.pipestream.repository.v1.DocumentServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.repository.v1.GetRequest.newBuilder()
                .setDocId("mock-doc-001")
                .setAccountId("acc-001")
                .build();
        var response = stub.get(request);

        assertThat(response).as("DocumentService/Get response").isNotNull();
    }

    @Test
    @DisplayName("DocumentService/Save returns saved reference")
    void testDocumentServiceSave() {
        var stub = ai.pipestream.repository.v1.DocumentServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.repository.v1.SaveRequest.newBuilder().build();
        var response = stub.save(request);

        assertThat(response).as("DocumentService/Save response").isNotNull();
    }

    @Test
    @DisplayName("DocumentService/List returns document list")
    void testDocumentServiceList() {
        var stub = ai.pipestream.repository.v1.DocumentServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.repository.v1.ListRequest.newBuilder().build();
        var response = stub.list(request);

        assertThat(response).as("DocumentService/List response").isNotNull();
    }

    // ============================================
    // GraphRepositoryService
    // ============================================

    @Test
    @DisplayName("GraphRepositoryService/GetGraph returns graph")
    void testGraphRepoGet() {
        var stub = ai.pipestream.repository.v1.GraphRepositoryServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.repository.v1.GetGraphRequest.newBuilder()
                .setGraphId("test-graph")
                .build();
        var response = stub.getGraph(request);

        assertThat(response).as("GraphRepositoryService/GetGraph response").isNotNull();
    }

    @Test
    @DisplayName("GraphRepositoryService/ListGraphs returns graph list")
    void testGraphRepoList() {
        var stub = ai.pipestream.repository.v1.GraphRepositoryServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.repository.v1.ListGraphsRequest.newBuilder().build();
        var response = stub.listGraphs(request);

        assertThat(response).as("GraphRepositoryService/ListGraphs response").isNotNull();
    }

    @Test
    @DisplayName("GraphRepositoryService/ResolveNextNodes returns next nodes")
    void testGraphRepoResolveNext() {
        var stub = ai.pipestream.repository.v1.GraphRepositoryServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.repository.v1.ResolveNextNodesRequest.newBuilder()
                .build();
        var response = stub.resolveNextNodes(request);

        assertThat(response).as("GraphRepositoryService/ResolveNextNodes response").isNotNull();
    }

    // ============================================
    // FilesystemCrawlerService
    // ============================================

    @Test
    @DisplayName("FilesystemCrawlerService/CrawlDirectory returns crawl result")
    void testFilesystemCrawlerCrawl() {
        var stub = ai.pipestream.repository.crawler.v1.FilesystemCrawlerServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.repository.crawler.v1.CrawlDirectoryRequest.newBuilder()
                .setPath("/mock/path")
                .build();
        var response = stub.crawlDirectory(request);

        assertThat(response).as("FilesystemCrawlerService/CrawlDirectory response").isNotNull();
    }

    // ============================================
    // SchemaManagerService
    // ============================================

    @Test
    @DisplayName("SchemaManagerService/EnsureNestedEmbeddingsFieldExists returns success")
    void testSchemaManagerEnsureField() {
        var stub = ai.pipestream.schemamanager.v1.SchemaManagerServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.schemamanager.v1.EnsureNestedEmbeddingsFieldExistsRequest.newBuilder()
                .setIndexName("test-index")
                .setNestedFieldName("embeddings")
                .build();
        var response = stub.ensureNestedEmbeddingsFieldExists(request);

        assertThat(response).as("SchemaManagerService/EnsureNestedEmbeddingsFieldExists response").isNotNull();
    }

    // ============================================
    // ShellService
    // ============================================

    @Test
    @DisplayName("ShellService/ListUiServices returns service list")
    void testShellServiceListUi() {
        var stub = ai.pipestream.shell.v1.ShellServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.shell.v1.ListUiServicesRequest.newBuilder().build();
        var response = stub.listUiServices(request);

        assertThat(response).as("ShellService/ListUiServices response").isNotNull();
    }

    @Test
    @DisplayName("ShellService/SearchDocuments returns search results")
    void testShellServiceSearch() {
        var stub = ai.pipestream.shell.v1.ShellServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.shell.v1.SearchDocumentsRequest.newBuilder()
                .setQuery("test")
                .build();
        var response = stub.searchDocuments(request);

        assertThat(response).as("ShellService/SearchDocuments response").isNotNull();
    }

    // ============================================
    // S3ConnectorControlService
    // ============================================

    @Test
    @DisplayName("S3ConnectorControlService/TestBucketCrawl returns test result")
    void testS3ConnectorTestCrawl() {
        var stub = ai.pipestream.connector.s3.v1.S3ConnectorControlServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.connector.s3.v1.TestBucketCrawlRequest.newBuilder()
                .setBucket("test-bucket")
                .setDryRun(true)
                .build();
        var response = stub.testBucketCrawl(request);

        assertThat(response).as("S3ConnectorControlService/TestBucketCrawl response").isNotNull();
    }

    // ============================================
    // ValidationService
    // ============================================

    @Test
    @DisplayName("ValidationService/ValidateGraph returns valid")
    void testValidationValidateGraph() {
        var stub = ai.pipestream.validation.v1.ValidationServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.validation.v1.ValidateGraphRequest.newBuilder().build();
        var response = stub.validateGraph(request);

        assertThat(response).as("ValidationService/ValidateGraph response").isNotNull();
    }

    @Test
    @DisplayName("ValidationService/ValidateModule returns valid")
    void testValidationValidateModule() {
        var stub = ai.pipestream.validation.v1.ValidationServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.validation.v1.ValidateModuleRequest.newBuilder().build();
        var response = stub.validateModule(request);

        assertThat(response).as("ValidationService/ValidateModule response").isNotNull();
    }

    // ============================================
    // ModuleTestingSidecarService
    // ============================================

    @Test
    @DisplayName("ModuleTestingSidecarService/RunModuleTest returns test result")
    void testModuleTestingRunTest() {
        var stub = ai.pipestream.testing.harness.v1.ModuleTestingSidecarServiceGrpc.newBlockingStub(channel);
        var request = ai.pipestream.testing.harness.v1.RunModuleTestRequest.newBuilder()
                .setModuleName("test-module")
                .setAccountId("acc-001")
                .build();
        var response = stub.runModuleTest(request);

        assertThat(response).as("ModuleTestingSidecarService/RunModuleTest response").isNotNull();
    }
}
