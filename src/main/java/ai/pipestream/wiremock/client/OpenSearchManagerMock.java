package ai.pipestream.wiremock.client;

import ai.pipestream.opensearch.v1.*;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.time.Instant;
import java.util.UUID;

import static org.wiremock.grpc.dsl.WireMockGrpc.method;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;

/**
 * WireMock stub helper for OpenSearchManagerService.
 * <p>
 * Provides dummy data for admin search APIs (SearchDocumentUploads, SearchFilesystemMeta)
 * and index management (ListIndices, GetIndexStats).
 */
public class OpenSearchManagerMock implements ServiceMockInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchManagerMock.class);

    private static final String SERVICE_NAME = OpenSearchManagerServiceGrpc.SERVICE_NAME;

    private WireMockGrpcService openSearchManagerService;

    public OpenSearchManagerMock(WireMock wireMock) {
        this.openSearchManagerService = new WireMockGrpcService(wireMock, SERVICE_NAME);
    }

    public OpenSearchManagerMock() {
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void initializeDefaults(WireMock wireMock) {
        this.openSearchManagerService = new WireMockGrpcService(wireMock, SERVICE_NAME);

        LOG.info("Initializing default OpenSearchManagerService stubs");

        long nowSeconds = Instant.now().getEpochSecond();

        // 1. Mock SearchDocumentUploads
        DocumentUploadResult doc1 = DocumentUploadResult.newBuilder()
                .setDocId("mock-doc-1")
                .setFilename("financial_report_2025.pdf")
                .setPath("/nas/finance/reports/financial_report_2025.pdf")
                .setMimeType("application/pdf")
                .setAccountId("admin")
                .setConnectorId("nas-connector-1")
                .setStorageKey("s3://pipestream/uploads/fin_2025.pdf")
                .setUploadedAt(nowSeconds)
                .setScore(1.0f)
                .build();

        DocumentUploadResult doc2 = DocumentUploadResult.newBuilder()
                .setDocId("mock-doc-2")
                .setFilename("onboarding_guide.pdf")
                .setPath("/nas/hr/onboarding_guide.pdf")
                .setMimeType("application/pdf")
                .setAccountId("admin")
                .setConnectorId("nas-connector-1")
                .setStorageKey("s3://pipestream/uploads/hr_onboarding.pdf")
                .setUploadedAt(nowSeconds)
                .setScore(0.85f)
                .build();

        SearchDocumentUploadsResponse searchResponse = SearchDocumentUploadsResponse.newBuilder()
                .addResults(doc1)
                .addResults(doc2)
                .setTotalCount(2)
                .setMaxScore(1.0f)
                .build();

        // Stub SearchDocumentUploads for any request (generic mock)
        openSearchManagerService.stubFor(
                method("SearchDocumentUploads")
                        .willReturn(message(searchResponse))
        );

        // Stub for specific empty query
        SearchDocumentUploadsResponse emptyResponse = SearchDocumentUploadsResponse.newBuilder()
                .setTotalCount(0)
                .build();
        
        openSearchManagerService.stubFor(
                method("SearchDocumentUploads")
                        .withRequestMessage(WireMockGrpc.equalToMessage(SearchDocumentUploadsRequest.newBuilder().setQuery("empty").build()))
                        .willReturn(message(emptyResponse))
        );

        // 2. Mock SearchFilesystemMeta
        FilesystemSearchResult fsResult1 = FilesystemSearchResult.newBuilder()
                .setNodeId(UUID.randomUUID().toString())
                .setName("important_doc.pdf")
                .setNodeType("FILE")
                .setDrive("nas-storage")
                .setScore(1.0f)
                .build();

        SearchFilesystemMetaResponse fsSearchResponse = SearchFilesystemMetaResponse.newBuilder()
                .addResults(fsResult1)
                .setTotalCount(1)
                .build();

        openSearchManagerService.stubFor(
                method("SearchFilesystemMeta")
                .willReturn(message(fsSearchResponse))
        );

        // 3. Mock ListIndices
        ListIndicesResponse listIndicesResponse = ListIndicesResponse.newBuilder()
                .addIndices(OpenSearchIndexInfo.newBuilder().setName("repository-document-uploads").build())
                .addIndices(OpenSearchIndexInfo.newBuilder().setName("filesystem-nodes").build())
                .build();

        openSearchManagerService.stubFor(
                method("ListIndices")
                        .willReturn(message(listIndicesResponse))
        );

        LOG.info("OpenSearchManagerService stubs ready: SearchDocumentUploads, SearchFilesystemMeta, ListIndices");
    }
}
