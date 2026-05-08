package ai.pipestream.wiremock.server;

import ai.pipestream.opensearch.v1.ChunkerConfig;
import ai.pipestream.opensearch.v1.EmbeddingModelConfig;
import ai.pipestream.opensearch.v1.VectorSet;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Shared in-memory state for the Direct gRPC mock services. Holds the
 * registered chunker configs, embedding model configs, vector set recipes
 * and (vector_set, index) bindings so the streaming index-documents path
 * can validate the {@code (source_field, chunker, embedder)} tuples it
 * sees on inbound docs against what the test setup registered.
 *
 * <p>Lookup keys:
 * <ul>
 *   <li>{@link #vectorSetByName} — keyed by {@link VectorSet#getName()}; the
 *       authoritative recipe row.</li>
 *   <li>{@link #vectorSetByLookupKey} — keyed by
 *       {@code <sourceField>_<chunker>_<embedder>}; the lookup the deployed
 *       0.1.58 image used when validating inbound documents in
 *       {@code streamIndexDocuments}. Same key is recreated on the wire by
 *       reading the inbound {@code SemanticVectorSet}'s
 *       {@code source_field_name + chunk_config_id + embedding_id} fields.</li>
 *   <li>{@link #bindings} — set of {@code <vsId>|<indexName>} strings; the
 *       streaming path requires both a registered VS AND a binding to the
 *       target index before it accepts a document.</li>
 * </ul>
 *
 * <p>Validation in the streaming path is conditional: only triggered when
 * the inbound {@code SemanticVectorSet} has all three identifying fields
 * populated. Pre-existing tests that send docs without semantic content
 * (e.g. {@link DirectWireMockGrpcServer}'s {@code testStreamIndexDocuments_Success}
 * which sets only title) pass through untouched.
 *
 * <p>Singleton: holds process-wide state. Tests should call {@link #clear()}
 * in their setup hook to start each test with an empty registry.
 */
public final class MockServiceState {

    private static final MockServiceState INSTANCE = new MockServiceState();

    /** Singleton accessor. */
    public static MockServiceState get() {
        return INSTANCE;
    }

    private final Map<String, ChunkerConfig> chunkerConfigsById = new ConcurrentHashMap<>();
    private final Map<String, EmbeddingModelConfig> embeddingConfigsById = new ConcurrentHashMap<>();
    private final Map<String, VectorSet> vectorSetByName = new ConcurrentHashMap<>();
    private final Map<String, VectorSet> vectorSetByLookupKey = new ConcurrentHashMap<>();
    private final Map<String, VectorSet> vectorSetById = new ConcurrentHashMap<>();
    /** {@code <vsId>|<indexName>} pairs for which BindVectorSetToIndex has been called. */
    private final Set<String> bindings = new CopyOnWriteArraySet<>();

    private MockServiceState() {}

    /**
     * Resets the entire mock registry. Call from {@code @BeforeEach} so test
     * order doesn't leak fixtures across cases.
     */
    public void clear() {
        chunkerConfigsById.clear();
        embeddingConfigsById.clear();
        vectorSetByName.clear();
        vectorSetByLookupKey.clear();
        vectorSetById.clear();
        bindings.clear();
    }

    // ---- ChunkerConfig ------------------------------------------------------

    /** Stores a chunker config by id. */
    public void putChunkerConfig(ChunkerConfig config) {
        chunkerConfigsById.put(config.getId(), config);
    }

    /** Fetches a chunker config by id, or {@code null} when absent. */
    public ChunkerConfig getChunkerConfig(String id) {
        return chunkerConfigsById.get(id);
    }

    // ---- EmbeddingModelConfig ----------------------------------------------

    /** Stores an embedding model config by id. */
    public void putEmbeddingConfig(EmbeddingModelConfig config) {
        embeddingConfigsById.put(config.getId(), config);
    }

    /** Fetches an embedding model config by id, or {@code null} when absent. */
    public EmbeddingModelConfig getEmbeddingConfig(String id) {
        return embeddingConfigsById.get(id);
    }

    // ---- VectorSet ----------------------------------------------------------

    /**
     * Stores a vector set under both its name and the
     * {@code <sourceField>_<chunker>_<embedder>} lookup key (when those
     * three fields are populated). The lookup-key registration is what
     * {@code streamIndexDocuments} validation uses.
     */
    public void putVectorSet(VectorSet vs) {
        vectorSetByName.put(vs.getName(), vs);
        if (!vs.getId().isEmpty()) {
            vectorSetById.put(vs.getId(), vs);
        }
        String key = lookupKey(vs.getSourceField(), vs.getChunkerConfigId(),
                vs.getEmbeddingModelConfigId());
        if (key != null) {
            vectorSetByLookupKey.put(key, vs);
        }
    }

    /** Fetches a vector set by name, or {@code null} when absent. */
    public VectorSet getVectorSetByName(String name) {
        return vectorSetByName.get(name);
    }

    /** Fetches a vector set by id, or {@code null} when absent. */
    public VectorSet getVectorSetById(String id) {
        return vectorSetById.get(id);
    }

    /**
     * Fetches a vector set by the
     * {@code <sourceField>_<chunker>_<embedder>} lookup tuple, or
     * {@code null} when no recipe was registered for that tuple.
     */
    public VectorSet getVectorSetByLookup(String sourceField, String chunker, String embedder) {
        String key = lookupKey(sourceField, chunker, embedder);
        return key == null ? null : vectorSetByLookupKey.get(key);
    }

    // ---- VectorSetIndexBinding ---------------------------------------------

    /** Records a (vector_set_id, index_name) binding. */
    public void putBinding(String vectorSetId, String indexName) {
        bindings.add(bindingKey(vectorSetId, indexName));
    }

    /** Returns whether a binding exists for the given (vector_set_id, index_name) pair. */
    public boolean hasBinding(String vectorSetId, String indexName) {
        return bindings.contains(bindingKey(vectorSetId, indexName));
    }

    // ---- Internals ----------------------------------------------------------

    /**
     * Builds the {@code <sourceField>_<chunker>_<embedder>} lookup key
     * matching the deployed 0.1.58 image's error format
     * ("VectorSet 'body_chunker_v1_embed_v1' not found"). Returns
     * {@code null} when any of the three fields is blank — callers treat
     * that as "no validation needed for this row".
     */
    public static String lookupKey(String sourceField, String chunker, String embedder) {
        if (sourceField == null || sourceField.isBlank()) return null;
        if (chunker == null || chunker.isBlank()) return null;
        if (embedder == null || embedder.isBlank()) return null;
        return sourceField + "_" + chunker.replace('-', '_') + "_" + embedder.replace('-', '_');
    }

    private static String bindingKey(String vectorSetId, String indexName) {
        return vectorSetId + "|" + indexName;
    }
}
