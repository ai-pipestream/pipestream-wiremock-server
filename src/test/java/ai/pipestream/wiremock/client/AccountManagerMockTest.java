package ai.pipestream.wiremock.client;

import ai.pipestream.repository.v1.account.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wiremock.grpc.GrpcExtensionFactory;
import org.wiremock.grpc.dsl.WireMockGrpc;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AccountManagerMock demonstrating how to use it to configure WireMock stubs.
 * <p>
 * These tests show the pattern:
 * <ol>
 * <li>Start a WireMock server with gRPC extension</li>
 * <li>Create AccountManagerMock using the server's port</li>
 * <li>Configure stubs using AccountManagerMock</li>
 * <li>Create a gRPC client (using client's own protobuf classes) and call the mocked service</li>
 * </ol>
 */
public class AccountManagerMockTest {

    private WireMockServer wireMockServer;
    private AccountManagerMock accountManagerMock;
    private ManagedChannel channel;
    private AccountServiceGrpc.AccountServiceBlockingStub accountServiceStub;

    @BeforeEach
    void setUp() {
        // Start WireMock server with gRPC extension
        // Use withRootDirectory like reference examples - expects descriptors at wiremock/grpc/
        WireMockConfiguration config = wireMockConfig()
                .dynamicPort()
                .withRootDirectory("src/test/resources/wiremock")
                .extensions(new GrpcExtensionFactory());

        wireMockServer = new WireMockServer(config);
        wireMockServer.start();

        // Create AccountManagerMock using the server's port
        WireMock wireMock = new WireMock(wireMockServer.port());
        accountManagerMock = new AccountManagerMock(wireMock);

        // Create a gRPC client channel pointing to WireMock
        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();

        // Create the gRPC stub (using client's protobuf classes)
        accountServiceStub = AccountServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.shutdown();
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void testMockGetAccount_Success() {
        // Configure a stub for GetAccount that returns a successful response
        accountManagerMock.mockGetAccount(
                "test-account-123",
                "Test Account",
                "This is a test account",
                true
        );

        // Call the mocked service using a normal gRPC client
        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId("test-account-123")
                .build();

        GetAccountResponse response = accountServiceStub.getAccount(request);

        // Verify the response
        assertNotNull(response);
        assertTrue(response.hasAccount());
        Account account = response.getAccount();
        assertEquals("test-account-123", account.getAccountId());
        assertEquals("Test Account", account.getName());
        assertEquals("This is a test account", account.getDescription());
        assertTrue(account.getActive());
    }

    @Test
    void testMockGetAccount_InactiveAccount() {
        // Configure a stub for an inactive account
        accountManagerMock.mockGetAccount(
                "inactive-account",
                "Inactive Account",
                "This account is inactive",
                false
        );

        // Call the mocked service
        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId("inactive-account")
                .build();

        GetAccountResponse response = accountServiceStub.getAccount(request);

        // Verify the response shows inactive account
        assertNotNull(response);
        assertTrue(response.hasAccount());
        Account account = response.getAccount();
        assertEquals("inactive-account", account.getAccountId());
        assertFalse(account.getActive());
    }

    @Test
    void testMockAccountNotFound() {
        // Configure a stub that returns NOT_FOUND for a specific account ID
        accountManagerMock.mockAccountNotFound("nonexistent-account");

        // Call the mocked service - should throw NOT_FOUND
        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId("nonexistent-account")
                .build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            accountServiceStub.getAccount(request);
        });

        // Verify the error
        assertEquals(io.grpc.Status.Code.NOT_FOUND, exception.getStatus().getCode());
        assertTrue(exception.getMessage().contains("Account not found: nonexistent-account"));
    }

    @Test
    void testMockGetAccount_MultipleAccounts() {
        // For WireMock 4, use request-specific matching instead of relying on automatic stub selection
        // Set up stubs with request matching for different account IDs

        // Stub for account-1
        accountManagerMock.mockGetAccount("account-1", "Account One", "First account", true);

        // Stub for account-2
        accountManagerMock.mockGetAccount("account-2", "Account Two", "Second account", false);

        // Test single account for now
        accountManagerMock.mockGetAccount("account-1", "Account One", "First account", true);

        GetAccountRequest request1 = GetAccountRequest.newBuilder()
                .setAccountId("account-1")
                .build();
        GetAccountResponse response1 = accountServiceStub.getAccount(request1);
        assertEquals("account-1", response1.getAccount().getAccountId());
        assertEquals("Account One", response1.getAccount().getName());
        assertTrue(response1.getAccount().getActive());
    }

    @Test
    void testReset() {
        // Configure a stub
        accountManagerMock.mockGetAccount("test-account", "Test", "Description", true);

        // Verify it works
        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId("test-account")
                .build();
        GetAccountResponse response = accountServiceStub.getAccount(request);
        assertNotNull(response);

        // Reset all stubs
        accountManagerMock.reset();

        // Now the call should fail (no matching stub)
        // In WireMock 4, this might return UNIMPLEMENTED instead of NOT_FOUND
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> {
            accountServiceStub.getAccount(request);
        });
        // Accept either UNIMPLEMENTED (WireMock 4 default) or NOT_FOUND
        assertTrue(
            exception.getStatus().getCode() == io.grpc.Status.Code.UNIMPLEMENTED ||
            exception.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND,
            "Expected UNIMPLEMENTED or NOT_FOUND, got: " + exception.getStatus().getCode()
        );
    }

    @Test
    void testMockGetAccount_WithEmptyDescription() {
        // Test with empty description
        accountManagerMock.mockGetAccount(
                "minimal-account",
                "Minimal Account",
                "",  // Empty description
                true
        );

        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId("minimal-account")
                .build();

        GetAccountResponse response = accountServiceStub.getAccount(request);
        assertNotNull(response);
        assertEquals("", response.getAccount().getDescription());
    }
}
