package ai.pipestream.wiremock.server;

import ai.pipestream.config.v1.*;
import ai.pipestream.opensearch.v1.*;
import ai.pipestream.connector.intake.v1.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * High-fidelity integration test to verify that declarative JSON stubs
 * are correctly loaded and mapped for all platform services.
 */
class PlatformServicesMockTest {

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer directGrpcServer;
    private ManagedChannel declarativeChannel;
    private ManagedChannel directChannel;

    @BeforeEach
    void setUp() throws Exception {
        // Point to the build directory where descriptors are copied
        wireMockServer = new WireMockServer(wireMockConfig()
                .dynamicPort()
                .withRootDirectory("build/resources/test/wiremock")
                .extensions(new GrpcExtensionFactory()));
        wireMockServer.start();

        directGrpcServer = new DirectWireMockGrpcServer(wireMockServer, 0);
        directGrpcServer.start();

        declarativeChannel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();
        
        directChannel = ManagedChannelBuilder.forAddress("localhost", directGrpcServer.getGrpcPort())
                .usePlaintext()
                .build();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (declarativeChannel != null) declarativeChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        if (directChannel != null) directChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        if (directGrpcServer != null) directGrpcServer.stop();
        if (wireMockServer != null) wireMockServer.stop();
    }

    @Test
    void testPipelineConfig_GetConfig() {
        PipelineConfigServiceGrpc.PipelineConfigServiceBlockingStub stub = PipelineConfigServiceGrpc.newBlockingStub(declarativeChannel);
        GetPipelineConfigRequest request = GetPipelineConfigRequest.newBuilder()
                .setClusterName("default")
                .setGraphName("test")
                .build();
        GetPipelineConfigResponse response = stub.getPipelineConfig(request);

        assertNotNull(response.getGraph());
        assertEquals("test-pipeline", response.getGraph().getGraphId());
    }

    @Test
    void testVectorSet_Get() {
        VectorSetServiceGrpc.VectorSetServiceBlockingStub stub = VectorSetServiceGrpc.newBlockingStub(declarativeChannel);
        GetVectorSetRequest request = GetVectorSetRequest.newBuilder().setId("vs-1").build();
        GetVectorSetResponse response = stub.getVectorSet(request);

        assertNotNull(response.getVectorSet());
        assertEquals("vs-1", response.getVectorSet().getId());
        assertEquals("body", response.getVectorSet().getSourceCel());
        assertEquals(VectorSetProvenance.VECTOR_SET_PROVENANCE_REGISTERED, response.getVectorSet().getProvenance());
    }

    @Test
    void testVectorSet_CreateAndListAndResolve() {
        VectorSetServiceGrpc.VectorSetServiceBlockingStub stub = VectorSetServiceGrpc.newBlockingStub(declarativeChannel);

        CreateVectorSetResponse created = stub.createVectorSet(CreateVectorSetRequest.newBuilder()
                .setName("new-vs")
                .setChunkerConfigId("chunk-1")
                .setEmbeddingModelConfigId("embed-1")
                .setIndexName("pipestream-docs")
                .setFieldName("f1")
                .build());
        assertNotNull(created.getVectorSet());
        assertEquals("vs-created-stub", created.getVectorSet().getId());

        ListVectorSetsResponse listed = stub.listVectorSets(ListVectorSetsRequest.newBuilder().setPageSize(10).build());
        assertFalse(listed.getVectorSetsList().isEmpty());
        assertEquals("vs-1", listed.getVectorSets(0).getId());

        ResolveVectorSetResponse resolved = stub.resolveVectorSet(ResolveVectorSetRequest.newBuilder()
                .setIndexName("pipestream-docs")
                .setFieldName("vs_default")
                .setResultSetName("default")
                .build());
        assertTrue(resolved.getFound());
        assertEquals("vs-1", resolved.getVectorSet().getId());

        ResolveVectorSetFromDirectiveResponse directive = stub.resolveVectorSetFromDirective(
                ResolveVectorSetFromDirectiveRequest.newBuilder().setVectorSetId("vs-1").build());
        assertTrue(directive.getResolved());
        assertEquals("vs-1", directive.getVectorSet().getId());

        UpdateVectorSetResponse updated = stub.updateVectorSet(UpdateVectorSetRequest.newBuilder()
                .setId("vs-1")
                .setName("renamed")
                .build());
        assertEquals("vs-1", updated.getVectorSet().getId());

        DeleteVectorSetResponse deleted = stub.deleteVectorSet(DeleteVectorSetRequest.newBuilder().setId("vs-1").build());
        assertTrue(deleted.getSuccess());
    }

    @Test
    void testDataSourceAdmin_Get() {
        DataSourceAdminServiceGrpc.DataSourceAdminServiceBlockingStub stub = DataSourceAdminServiceGrpc.newBlockingStub(declarativeChannel);
        GetDataSourceRequest request = GetDataSourceRequest.newBuilder().setDatasourceId("ds-1").build();
        GetDataSourceResponse response = stub.getDataSource(request);

        assertNotNull(response.getDatasource());
        assertEquals("ds-1", response.getDatasource().getDatasourceId());
    }

    @Test
    void testOpenSearchManager_IndexDocument_Success() {
        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceBlockingStub stub = OpenSearchManagerServiceGrpc.newBlockingStub(directChannel);
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndexName("normal-index")
                .setDocumentId("normal-doc")
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId("normal-doc").setTitle("Valid").build())
                .build();
        
        IndexDocumentResponse response = stub.indexDocument(request);
        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("High-Fidelity"));
    }

    @Test
    void testOpenSearchManager_IndexDocument_ForcedError() {
        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceBlockingStub stub = OpenSearchManagerServiceGrpc.newBlockingStub(directChannel);
        
        // This request triggers the error matching logic in DirectWireMockGrpcServer.java
        IndexDocumentRequest request = IndexDocumentRequest.newBuilder()
                .setIndexName("any-index")
                .setDocumentId("fail-this-doc")
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId("fail-this-doc").setTitle("This will fail").build())
                .build();
        
        IndexDocumentResponse response = stub.indexDocument(request);

        assertNotNull(response);
        assertFalse(response.getSuccess());
        assertTrue(response.getMessage().contains("Forced internal error"));
    }

    @Test
    void testOpenSearchManager_StreamIndexDocuments_Success() throws Exception {
        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub stub = OpenSearchManagerServiceGrpc.newStub(directChannel);
        java.util.concurrent.CompletableFuture<StreamIndexDocumentsResponse> future = new java.util.concurrent.CompletableFuture<>();

        io.grpc.stub.StreamObserver<StreamIndexDocumentsRequest> requestObserver = stub.streamIndexDocuments(new io.grpc.stub.StreamObserver<StreamIndexDocumentsResponse>() {
            @Override
            public void onNext(StreamIndexDocumentsResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {}
        });

        requestObserver.onNext(StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("bulk-1")
                .setIndexName("bulk-index")
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId("bulk-doc").setTitle("Bulk").build())
                .build());
        requestObserver.onCompleted();

        StreamIndexDocumentsResponse response = future.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("Streamed"));
    }

    @Test
    void testOpenSearchManager_StreamIndexDocuments_ForcedError() throws Exception {
        OpenSearchManagerServiceGrpc.OpenSearchManagerServiceStub stub = OpenSearchManagerServiceGrpc.newStub(directChannel);
        java.util.concurrent.CompletableFuture<StreamIndexDocumentsResponse> future = new java.util.concurrent.CompletableFuture<>();

        io.grpc.stub.StreamObserver<StreamIndexDocumentsRequest> requestObserver = stub.streamIndexDocuments(new io.grpc.stub.StreamObserver<StreamIndexDocumentsResponse>() {
            @Override
            public void onNext(StreamIndexDocumentsResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {}
        });

        requestObserver.onNext(StreamIndexDocumentsRequest.newBuilder()
                .setRequestId("bulk-fail")
                .setIndexName("fail-this-index-bulk")
                .setDocument(OpenSearchDocument.newBuilder().setOriginalDocId("fail-doc").setTitle("Fail").build())
                .build());
        requestObserver.onCompleted();

        StreamIndexDocumentsResponse response = future.get(5, TimeUnit.SECONDS);
        assertNotNull(response);
        assertFalse(response.getSuccess());
        assertEquals("bulk-fail", response.getRequestId());
        assertTrue(response.getMessage().contains("Forced internal error"));
    }
}
