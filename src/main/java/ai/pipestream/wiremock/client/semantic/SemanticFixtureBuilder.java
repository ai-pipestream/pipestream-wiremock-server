package ai.pipestream.wiremock.client.semantic;

import ai.pipestream.data.v1.ChunkEmbedding;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.SemanticChunk;
import ai.pipestream.data.v1.SemanticProcessingResult;
import ai.pipestream.data.v1.SourceFieldAnalytics;
import ai.pipestream.data.v1.NlpDocumentAnalysis;
import ai.pipestream.data.v1.NamedEmbedderConfig;
import ai.pipestream.data.v1.VectorDirective;
import ai.pipestream.data.v1.VectorSetDirectives;
import ai.pipestream.data.v1.GranularityLevel;
import ai.pipestream.data.v1.CentroidMetadata;
import com.google.protobuf.Value;

/**
 * Builds canonical stage-1 / stage-2 / stage-3 {@link PipeDoc} fixtures for the
 * semantic pipeline step mocks.
 *
 * <p>These builders produce the exact shapes required by the invariants defined
 * in {@code SemanticPipelineInvariants} (in pipestream-test-support). Wiremock
 * mocks serve them as canned gRPC responses; tests verify them against the
 * invariants via pipestream-test-support (testImplementation scope).
 *
 * <p>Kept inside wiremock-server's own main source on purpose — the wiremock
 * runtime container has no pipestream-test-support dependency at main scope
 * and must build its canned responses self-sufficiently.
 */
public final class SemanticFixtureBuilder {

    private SemanticFixtureBuilder() {}

    /**
     * Builds a minimal valid stage-1 {@link PipeDoc} for the chunker step mock's
     * success scenario. One SPR at source_field="body" with chunk_config_id="sentence_v1",
     * empty embedding_config_id (placeholder), one chunk with empty vector,
     * directive_key stamped in metadata, nlp_analysis attached, one matching
     * source_field_analytics entry.
     */
    public static PipeDoc buildStage1PipeDoc() {
        SemanticChunk chunk = SemanticChunk.newBuilder()
                .setChunkId("chunk-0")
                .setChunkNumber(0)
                .setEmbeddingInfo(ChunkEmbedding.newBuilder()
                        .setTextContent("The quick brown fox jumps over the lazy dog.")
                        .setOriginalCharStartOffset(0)
                        .setOriginalCharEndOffset(44)
                        .build())
                .build();

        SemanticProcessingResult spr = SemanticProcessingResult.newBuilder()
                .setResultId("stage1:fixture-doc-001:body:sentence_v1:")
                .setSourceFieldName("body")
                .setChunkConfigId("sentence_v1")
                .setEmbeddingConfigId("")
                .addChunks(chunk)
                .putMetadata("directive_key", Value.newBuilder()
                        .setStringValue("sha256b64url-fixture-directive-001").build())
                .setNlpAnalysis(NlpDocumentAnalysis.getDefaultInstance())
                .build();

        SearchMetadata sm = SearchMetadata.newBuilder()
                .addSemanticResults(spr)
                .addSourceFieldAnalytics(SourceFieldAnalytics.newBuilder()
                        .setSourceField("body")
                        .setChunkConfigId("sentence_v1")
                        .build())
                .build();

        return PipeDoc.newBuilder()
                .setDocId("fixture-doc-001")
                .setSearchMetadata(sm)
                .build();
    }

    /**
     * Builds a minimal valid stage-2 {@link PipeDoc} for the embedder step mock's
     * success scenario. Same shape as stage 1 but with embedding_config_id="minilm"
     * and a populated 4-element deterministic vector on the chunk. Includes
     * vector_set_directives advertising the minilm embedder config so the
     * assertPostEmbedder cross-check passes.
     */
    public static PipeDoc buildStage2PipeDoc() {
        ChunkEmbedding embedding = ChunkEmbedding.newBuilder()
                .setTextContent("The quick brown fox jumps over the lazy dog.")
                .setOriginalCharStartOffset(0)
                .setOriginalCharEndOffset(44)
                .addVector(0.1f)
                .addVector(0.2f)
                .addVector(0.3f)
                .addVector(0.4f)
                .build();

        SemanticChunk chunk = SemanticChunk.newBuilder()
                .setChunkId("chunk-0")
                .setChunkNumber(0)
                .setEmbeddingInfo(embedding)
                .build();

        SemanticProcessingResult spr = SemanticProcessingResult.newBuilder()
                .setResultId("stage2:fixture-doc-001:body:sentence_v1:minilm")
                .setSourceFieldName("body")
                .setChunkConfigId("sentence_v1")
                .setEmbeddingConfigId("minilm")
                .addChunks(chunk)
                .putMetadata("directive_key", Value.newBuilder()
                        .setStringValue("sha256b64url-fixture-directive-001").build())
                .setNlpAnalysis(NlpDocumentAnalysis.getDefaultInstance())
                .build();

        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(VectorDirective.newBuilder()
                        .setSourceLabel("body")
                        .setCelSelector("document.body")
                        .addEmbedderConfigs(NamedEmbedderConfig.newBuilder()
                                .setConfigId("minilm")
                                .build())
                        .build())
                .build();

        SearchMetadata sm = SearchMetadata.newBuilder()
                .addSemanticResults(spr)
                .addSourceFieldAnalytics(SourceFieldAnalytics.newBuilder()
                        .setSourceField("body")
                        .setChunkConfigId("sentence_v1")
                        .build())
                .setVectorSetDirectives(directives)
                .build();

        return PipeDoc.newBuilder()
                .setDocId("fixture-doc-001")
                .setSearchMetadata(sm)
                .build();
    }

    /**
     * Builds a minimal valid stage-3 {@link PipeDoc} for the semantic-graph step
     * mock's success scenario. Contains the stage-2 SPR plus an appended
     * document-centroid SPR and a semantic-boundary SPR, all lex-sorted on
     * (source_field, chunk_config_id, embedding_config_id, result_id).
     *
     * <p>At source_field="body", chunk_config_ids sort as:
     * {@code document_centroid < semantic < sentence_v1}. The result list is
     * ordered accordingly.
     */
    public static PipeDoc buildStage3PipeDoc() {
        ChunkEmbedding stage2Embedding = ChunkEmbedding.newBuilder()
                .setTextContent("The quick brown fox jumps over the lazy dog.")
                .setOriginalCharStartOffset(0)
                .setOriginalCharEndOffset(44)
                .addVector(0.1f).addVector(0.2f).addVector(0.3f).addVector(0.4f)
                .build();

        SemanticChunk stage2Chunk = SemanticChunk.newBuilder()
                .setChunkId("chunk-0")
                .setChunkNumber(0)
                .setEmbeddingInfo(stage2Embedding)
                .build();

        SemanticProcessingResult stage2Spr = SemanticProcessingResult.newBuilder()
                .setResultId("stage2:fixture-doc-001:body:sentence_v1:minilm")
                .setSourceFieldName("body")
                .setChunkConfigId("sentence_v1")
                .setEmbeddingConfigId("minilm")
                .addChunks(stage2Chunk)
                .putMetadata("directive_key", Value.newBuilder()
                        .setStringValue("sha256b64url-fixture-directive-001").build())
                .setNlpAnalysis(NlpDocumentAnalysis.getDefaultInstance())
                .build();

        ChunkEmbedding centroidEmbedding = ChunkEmbedding.newBuilder()
                .setTextContent("Document centroid vector")
                .setOriginalCharStartOffset(0)
                .setOriginalCharEndOffset(24)
                .addVector(0.1f).addVector(0.2f).addVector(0.3f).addVector(0.4f)
                .build();

        SemanticChunk centroidChunk = SemanticChunk.newBuilder()
                .setChunkId("centroid-0")
                .setChunkNumber(0)
                .setEmbeddingInfo(centroidEmbedding)
                .build();

        SemanticProcessingResult centroidSpr = SemanticProcessingResult.newBuilder()
                .setResultId("stage3:fixture-doc-001:body:document_centroid:minilm")
                .setSourceFieldName("body")
                .setChunkConfigId("document_centroid")
                .setEmbeddingConfigId("minilm")
                .addChunks(centroidChunk)
                .putMetadata("directive_key", Value.newBuilder()
                        .setStringValue("sha256b64url-fixture-directive-001").build())
                .setCentroidMetadata(CentroidMetadata.newBuilder()
                        .setGranularity(GranularityLevel.GRANULARITY_LEVEL_DOCUMENT)
                        .setSourceVectorCount(1)
                        .build())
                .build();

        ChunkEmbedding boundaryEmbedding = ChunkEmbedding.newBuilder()
                .setTextContent("Semantic boundary chunk text")
                .setOriginalCharStartOffset(0)
                .setOriginalCharEndOffset(28)
                .addVector(0.5f).addVector(0.6f).addVector(0.7f).addVector(0.8f)
                .build();

        SemanticChunk boundaryChunk = SemanticChunk.newBuilder()
                .setChunkId("semantic-0")
                .setChunkNumber(0)
                .setEmbeddingInfo(boundaryEmbedding)
                .build();

        SemanticProcessingResult boundarySpr = SemanticProcessingResult.newBuilder()
                .setResultId("stage3:fixture-doc-001:body:semantic:minilm")
                .setSourceFieldName("body")
                .setChunkConfigId("semantic")
                .setEmbeddingConfigId("minilm")
                .setGranularity(GranularityLevel.GRANULARITY_LEVEL_SEMANTIC_CHUNK)
                .setSemanticConfigId("semantic_v1")
                .addChunks(boundaryChunk)
                .putMetadata("directive_key", Value.newBuilder()
                        .setStringValue("sha256b64url-fixture-directive-001").build())
                .build();

        VectorSetDirectives directives = VectorSetDirectives.newBuilder()
                .addDirectives(VectorDirective.newBuilder()
                        .setSourceLabel("body")
                        .setCelSelector("document.body")
                        .addEmbedderConfigs(NamedEmbedderConfig.newBuilder()
                                .setConfigId("minilm")
                                .build())
                        .build())
                .build();

        // lex order on (source_field, chunk_config_id, embedding_config_id, result_id):
        // body|document_centroid|minilm < body|semantic|minilm < body|sentence_v1|minilm
        SearchMetadata sm = SearchMetadata.newBuilder()
                .addSemanticResults(centroidSpr)
                .addSemanticResults(boundarySpr)
                .addSemanticResults(stage2Spr)
                .addSourceFieldAnalytics(SourceFieldAnalytics.newBuilder()
                        .setSourceField("body")
                        .setChunkConfigId("sentence_v1")
                        .build())
                .setVectorSetDirectives(directives)
                .build();

        return PipeDoc.newBuilder()
                .setDocId("fixture-doc-001")
                .setSearchMetadata(sm)
                .build();
    }
}
