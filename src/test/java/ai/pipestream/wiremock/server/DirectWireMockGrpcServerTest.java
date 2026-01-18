package ai.pipestream.wiremock.server;

import ai.pipestream.platform.registration.v1.*;
import ai.pipestream.repository.account.v1.AccountServiceGrpc;
import ai.pipestream.repository.account.v1.StreamAllAccountsRequest;
import com.github.tomakehurst.wiremock.WireMockServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DirectWireMockGrpcServer to verify it works correctly
 * with the new unified Register proto interface.
 */
class DirectWireMockGrpcServerTest {

    private WireMockServer wireMockServer;
    private DirectWireMockGrpcServer directGrpcServer;
    private ManagedChannel channel;
    private PlatformRegistrationServiceGrpc.PlatformRegistrationServiceStub asyncStub;
    private PlatformRegistrationServiceGrpc.PlatformRegistrationServiceBlockingStub blockingStub;
    private AccountServiceGrpc.AccountServiceBlockingStub accountBlockingStub;

    @BeforeEach
    void setUp() throws Exception {
        // Start WireMock server
        wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
        wireMockServer.start();

        // Start DirectWireMockGrpcServer on a random port
        directGrpcServer = new DirectWireMockGrpcServer(wireMockServer, 0);
        directGrpcServer.start();

        // Create gRPC client
        int grpcPort = directGrpcServer.getGrpcPort();
        channel = ManagedChannelBuilder.forAddress("localhost", grpcPort)
                .usePlaintext()
                .build();
        asyncStub = PlatformRegistrationServiceGrpc.newStub(channel);
        blockingStub = PlatformRegistrationServiceGrpc.newBlockingStub(channel);
        accountBlockingStub = AccountServiceGrpc.newBlockingStub(channel);
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
    void testRegisterService_Streaming() throws Exception {
        // Create a service registration request
        RegisterRequest request = RegisterRequest.newBuilder()
                .setName("test-service")
                .setType(ServiceType.SERVICE_TYPE_SERVICE)
                .setConnectivity(Connectivity.newBuilder()
                        .setAdvertisedHost("localhost")
                        .setAdvertisedPort(8080)
                        .setTlsEnabled(false)
                        .build())
                .setVersion("1.0.0")
                .build();

        // Collect all responses
        List<RegisterResponse> responses = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<Throwable> errors = new ArrayList<>();

        StreamObserver<RegisterResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(RegisterResponse value) {
                responses.add(value);
            }

            @Override
            public void onError(Throwable t) {
                errors.add(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        // Make the call
        asyncStub.register(request, responseObserver);

        // Wait for completion (with timeout)
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Stream should complete within 5 seconds");

        // Verify no errors
        assertTrue(errors.isEmpty(), "Should not have errors: " + errors);

        // Verify we received expected number of events
        // Service registration should emit 6 events: STARTED, VALIDATED, CONSUL_REGISTERED, 
        // HEALTH_CHECK_CONFIGURED, CONSUL_HEALTHY, COMPLETED
        assertEquals(6, responses.size(), "Should receive 6 registration events");

        // Verify event sequence
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_STARTED, responses.get(0).getEvent().getEventType());
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED, responses.get(1).getEvent().getEventType());
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED, responses.get(2).getEvent().getEventType());
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_HEALTH_CHECK_CONFIGURED, responses.get(3).getEvent().getEventType());
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY, responses.get(4).getEvent().getEventType());
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED, responses.get(5).getEvent().getEventType());

        // Verify all events have timestamps
        responses.forEach(response -> {
            assertTrue(response.getEvent().hasTimestamp(), "Each event should have a timestamp");
            assertFalse(response.getEvent().getMessage().isEmpty(), "Each event should have a message");
        });
    }

    @Test
    void testRegisterModule_Streaming() throws Exception {
        // Create a module registration request
        RegisterRequest request = RegisterRequest.newBuilder()
                .setName("test-module")
                .setType(ServiceType.SERVICE_TYPE_MODULE)
                .setConnectivity(Connectivity.newBuilder()
                        .setAdvertisedHost("localhost")
                        .setAdvertisedPort(8081)
                        .setTlsEnabled(false)
                        .build())
                .setVersion("1.0.0")
                .build();

        // Collect all responses
        List<RegisterResponse> responses = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<Throwable> errors = new ArrayList<>();

        StreamObserver<RegisterResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(RegisterResponse value) {
                responses.add(value);
            }

            @Override
            public void onError(Throwable t) {
                errors.add(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        // Make the call
        asyncStub.register(request, responseObserver);

        // Wait for completion (with timeout)
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Stream should complete within 5 seconds");

        // Verify no errors
        assertTrue(errors.isEmpty(), "Should not have errors: " + errors);

        // Verify we received expected number of events
        // Module registration should emit 10 events (including schema validation and Apicurio registration)
        assertEquals(10, responses.size(), "Should receive 10 registration events for module");

        // Verify event sequence starts correctly
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_STARTED, responses.get(0).getEvent().getEventType());
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED, responses.get(1).getEvent().getEventType());
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED, responses.get(2).getEvent().getEventType());

        // Verify module-specific events are present
        boolean hasMetadataRetrieved = responses.stream()
                .anyMatch(r -> r.getEvent().getEventType() == PlatformEventType.PLATFORM_EVENT_TYPE_METADATA_RETRIEVED);
        boolean hasSchemaValidated = responses.stream()
                .anyMatch(r -> r.getEvent().getEventType() == PlatformEventType.PLATFORM_EVENT_TYPE_SCHEMA_VALIDATED);
        boolean hasApicurioRegistered = responses.stream()
                .anyMatch(r -> r.getEvent().getEventType() == PlatformEventType.PLATFORM_EVENT_TYPE_APICURIO_REGISTERED);

        assertTrue(hasMetadataRetrieved, "Should have METADATA_RETRIEVED event");
        assertTrue(hasSchemaValidated, "Should have SCHEMA_VALIDATED event");
        assertTrue(hasApicurioRegistered, "Should have APICURIO_REGISTERED event");

        // Verify ends with COMPLETED
        assertEquals(PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED, responses.getLast().getEvent().getEventType());

        // Verify all events have timestamps
        responses.forEach(response -> {
            assertTrue(response.getEvent().hasTimestamp(), "Each event should have a timestamp");
            assertFalse(response.getEvent().getMessage().isEmpty(), "Each event should have a message");
        });
    }

    @Test
    void testListServices() {
        ListServicesRequest request = ListServicesRequest.newBuilder().build();
        ListServicesResponse response = blockingStub.listServices(request);

        assertNotNull(response);
        assertEquals(2, response.getTotalCount());
        assertEquals(2, response.getServicesCount());
        assertTrue(response.hasAsOf(), "Should have as_of timestamp");

        // Verify mock services
        GetServiceResponse service1 = response.getServices(0);
        assertEquals("repository-service", service1.getServiceName());
        assertEquals("localhost", service1.getHost());
        assertEquals(8080, service1.getPort());
        assertTrue(service1.getIsHealthy());

        GetServiceResponse service2 = response.getServices(1);
        assertEquals("account-manager", service2.getServiceName());
        assertEquals("localhost", service2.getHost());
        assertEquals(38105, service2.getPort());
        assertTrue(service2.getIsHealthy());
    }

    @Test
    void testListModules() {
        ListPlatformModulesRequest request = ListPlatformModulesRequest.newBuilder().build();
        ListPlatformModulesResponse response = blockingStub.listPlatformModules(request);

        assertNotNull(response);
        assertEquals(2, response.getTotalCount());
        assertEquals(2, response.getModulesCount());
        assertTrue(response.hasAsOf(), "Should have as_of timestamp");

        // Verify mock modules
        GetModuleResponse module1 = response.getModules(0);
        assertEquals("parser", module1.getModuleName());
        assertEquals("localhost", module1.getHost());
        assertEquals(8081, module1.getPort());
        assertEquals("text/plain", module1.getInputFormat());
        assertEquals("application/json", module1.getOutputFormat());
        assertTrue(module1.getIsHealthy());

        GetModuleResponse module2 = response.getModules(1);
        assertEquals("chunker", module2.getModuleName());
        assertEquals("localhost", module2.getHost());
        assertEquals(8082, module2.getPort());
        assertEquals("application/json", module2.getInputFormat());
        assertEquals("application/json", module2.getOutputFormat());
        assertTrue(module2.getIsHealthy());
    }

    @Test
    void testStreamAllAccounts_DefaultsExcludeInactive() {
        var iterator = accountBlockingStub.streamAllAccounts(StreamAllAccountsRequest.newBuilder().build());
        boolean foundActive = false;
        boolean foundInactive = false;

        while (iterator.hasNext()) {
            var account = iterator.next().getAccount();
            if ("valid-account".equals(account.getAccountId())) {
                foundActive = true;
            }
            if ("inactive-account".equals(account.getAccountId())) {
                foundInactive = true;
            }
        }

        assertTrue(foundActive, "Stream should include active accounts by default");
        assertFalse(foundInactive, "Stream should exclude inactive accounts by default");
    }
}
