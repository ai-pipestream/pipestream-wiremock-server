package ai.pipestream.wiremock.server;

import ai.pipestream.repository.v1.filesystem.upload.NodeUploadServiceGrpc;
import ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocRequest;
import ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocResponse;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the DirectWireMockGrpcServer can handle large payloads
 * (simulating performance benchmarks) without RESOURCE_EXHAUSTED errors.
 */
class DirectWireMockPerformanceTest {

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer directGrpcServer;
    private ManagedChannel channel;
    private NodeUploadServiceGrpc.NodeUploadServiceBlockingStub stub;

    @BeforeEach
    void setUp() throws Exception {
        // Start WireMock (needed by DirectServer constructor, though not used for this specific test)
        wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
        wireMockServer.start();

        // Start DirectWireMockGrpcServer on a random port (0)
        directGrpcServer = new DirectWireMockGrpcServer(wireMockServer, 0);
        directGrpcServer.start();

        // Create gRPC client with UNLIMITED message size to verify server capacity
        int grpcPort = directGrpcServer.getGrpcPort();
        channel = NettyChannelBuilder.forAddress("localhost", grpcPort)
                .usePlaintext()
                .maxInboundMessageSize(Integer.MAX_VALUE) // Allow large responses (though response is small)
                // Important: Client doesn't restrict outbound size by default, but good to be explicit if needed
                .build();
        stub = NodeUploadServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (directGrpcServer != null) {
            directGrpcServer.stop();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void testUploadLargePayload_100MB() {
        // Create 100MB payload
        int size = 100 * 1024 * 1024;
        byte[] largeData = new byte[size];
        // No need to fill with data, zeros are fine for size test
        
        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
                .setDocument(PipeDoc.newBuilder()
                        .setBlobBag(BlobBag.newBuilder()
                                .setBlob(Blob.newBuilder()
                                        .setData(ByteString.copyFrom(largeData))
                                        .build())
                                .build())
                        .build())
                .build();

        long start = System.nanoTime();
        UploadPipeDocResponse response = stub.uploadPipeDoc(request);
        long duration = System.nanoTime() - start;

        assertTrue(response.getSuccess(), "Upload should succeed");
        System.out.printf("Uploaded 100MB in %.3f ms%n", duration / 1_000_000.0);
    }

    @Test
    void testUploadLargePayload_1point2GB() {
        // Create 1.2GB payload (approx 1288 MB)
        // 1.2 * 1024 * 1024 * 1024 = 1288490188
        int size = 1288490188;
        
        System.out.println("Allocating 1.2GB payload...");
        // Note: This requires sufficient heap space (set maxHeapSize = "4g" in build.gradle)
        byte[] largeData = new byte[size];
        
        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
                .setDocument(PipeDoc.newBuilder()
                        .setBlobBag(BlobBag.newBuilder()
                                .setBlob(Blob.newBuilder()
                                        .setData(ByteString.copyFrom(largeData))
                                        .build())
                                .build())
                        .build())
                .build();

        System.out.println("Sending 1.2GB payload...");
        long start = System.nanoTime();
        UploadPipeDocResponse response = stub.uploadPipeDoc(request);
        long duration = System.nanoTime() - start;

        assertTrue(response.getSuccess(), "Upload should succeed");
        System.out.printf("Uploaded 1.2GB in %.3f ms%n", duration / 1_000_000.0);
    }
}
