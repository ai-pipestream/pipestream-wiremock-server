package ai.pipestream.wiremock.testing;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.DeleteLogicalDocumentCommand;
import ai.pipestream.repository.pipedoc.v1.DeletePipeDocRequest;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.PipeDocMetadata;
import ai.pipestream.repository.pipedoc.v1.RemovedPipeDocNode;

/**
 * Small factories for PipeDoc gRPC messages used in tests and WireMock stubs.
 * Keeps call sites readable when exercising {@code logical_document} deletes and references.
 */
public final class PipeDocGrpcFixtures {

    private PipeDocGrpcFixtures() {
    }

    public static DocumentReference documentRef(String docId, String accountId) {
        return DocumentReference.newBuilder()
                .setDocId(docId)
                .setAccountId(accountId)
                .build();
    }

    public static DocumentReference documentRef(String docId, String accountId, String graphAddressId) {
        return DocumentReference.newBuilder()
                .setDocId(docId)
                .setAccountId(accountId)
                .setGraphAddressId(graphAddressId)
                .build();
    }

    public static GetPipeDocByReferenceRequest getByRefRequest(DocumentReference ref) {
        return GetPipeDocByReferenceRequest.newBuilder()
                .setDocumentRef(ref)
                .build();
    }

    public static GetPipeDocByReferenceRequest getByRefRequest(String docId, String accountId) {
        return getByRefRequest(documentRef(docId, accountId));
    }

    public static DeleteLogicalDocumentCommand logicalDeleteCommand(String docId, String accountId, String datasourceId) {
        return DeleteLogicalDocumentCommand.newBuilder()
                .setDocId(docId)
                .setAccountId(accountId)
                .setDatasourceId(datasourceId)
                .build();
    }

    public static DeletePipeDocRequest deletePipeDocLogical(String docId, String accountId, String datasourceId) {
        return deletePipeDocLogical(docId, accountId, datasourceId, false, false);
    }

    public static DeletePipeDocRequest deletePipeDocLogical(
            String docId,
            String accountId,
            String datasourceId,
            boolean purgeStorage,
            boolean omitRemovedNodes) {
        return DeletePipeDocRequest.newBuilder()
                .setLogicalDocument(logicalDeleteCommand(docId, accountId, datasourceId))
                .setPurgeStorage(purgeStorage)
                .setOmitRemovedNodes(omitRemovedNodes)
                .build();
    }

    public static RemovedPipeDocNode removedNode(String nodeId) {
        return RemovedPipeDocNode.newBuilder()
                .setNodeId(nodeId)
                .build();
    }

    public static RemovedPipeDocNode removedNode(String nodeId, String detail) {
        return RemovedPipeDocNode.newBuilder()
                .setNodeId(nodeId)
                .setDetail(detail)
                .build();
    }

    public static PipeDoc pipeDoc(String docId) {
        return PipeDoc.newBuilder()
                .setDocId(docId)
                .build();
    }

    public static PipeDocMetadata pipeDocMetadata(String nodeId, String docId, String drive, String connectorId) {
        return PipeDocMetadata.newBuilder()
                .setNodeId(nodeId)
                .setDocId(docId)
                .setDrive(drive)
                .setConnectorId(connectorId)
                .setSizeBytes(1)
                .setCreatedAtEpochMs(System.currentTimeMillis())
                .build();
    }
}
