package ai.pipestream.wiremock.client;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import com.google.protobuf.MessageOrBuilder;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Provides compatibility methods for creating WireMock stubs for gRPC services.
 * This class serves as a client-side utility to define gRPC mocks on an external
 * WireMock server, using the project's Protobuf definitions and standard WireMock features.
 * <p>
 * It converts Protobuf messages to JSON strings (using {@link ProtoJson}) for matching
 * and response bodies, as the external WireMock server will interpret gRPC requests
 * as HTTP POSTs with JSON bodies.
 */
public final class WireMockGrpcClient {
    private WireMockGrpcClient() {
        // Private constructor for utility class
    }

    /**
     * Creates a URL path pattern for a gRPC method.
     * gRPC methods are exposed as HTTP POST requests to `/ServiceFullName/MethodName`.
     *
     * @param serviceFullName The full name of the gRPC service (e.g., "ai.pipestream.repository.account.AccountService")
     * @param methodName      The name of the gRPC method (e.g., "GetAccount")
     * @return A URL path pattern.
     */
    public static String urlPathForGrpcMethod(String serviceFullName, String methodName) {
        return "/" + serviceFullName + "/" + methodName;
    }

    /**
     * Creates a {@link StringValuePattern} that matches a gRPC request body (Protobuf message)
     * by converting it to a JSON string and using WireMock's {@code equalToJson}.
     * <p>
     * This method is suitable for matching on a subset of fields in the request body,
     * allowing for additional fields to be present in the actual request.
     *
     * @param messageOrBuilder The Protobuf message or builder representing the expected request.
     * @return A {@link StringValuePattern} for matching the request body.
     */
    public static StringValuePattern equalToGrpcMessage(MessageOrBuilder messageOrBuilder) {
        // Use toJsonWithoutDefaults for request matching, as gRPC clients often omit default values
        final String json = ProtoJson.toJsonWithoutDefaults(messageOrBuilder);
        // Use ignoreExtraElements=true to allow actual requests to have additional fields
        return equalToJson(json, true, false);
    }

    /**
     * Creates a {@link StringValuePattern} that exactly matches a gRPC request body (Protobuf message).
     * <p>
     * This method requires an exact match of all fields, with no extra elements allowed.
     * Use this for precise matching when you need to ensure the entire request body matches.
     *
     * @param messageOrBuilder The Protobuf message or builder representing the expected request.
     * @return A {@link StringValuePattern} for exact matching of the request body.
     */
    public static StringValuePattern equalToGrpcMessageExact(MessageOrBuilder messageOrBuilder) {
        final String json = ProtoJson.toJsonWithoutDefaults(messageOrBuilder);
        // Use ignoreExtraElements=false for exact matching
        return equalToJson(json, false, false);
    }

    /**
     * Creates a stub mapping builder for a gRPC unary method.
     * This sets up an HTTP POST request to the gRPC method's URL.
     *
     * @param serviceFullName The full name of the gRPC service.
     * @param methodName      The name of the gRPC method.
     * @return A {@link MappingBuilder} for further stub configuration.
     */
    public static MappingBuilder grpcStubFor(String serviceFullName, String methodName) {
        return post(urlPathForGrpcMethod(serviceFullName, methodName));
    }

    /**
     * Configures a WireMock response to return a Protobuf message as a JSON body.
     *
     * @param messageOrBuilder The Protobuf message or builder for the response body.
     * @return A {@link ResponseDefinitionBuilder} configured with the JSON response.
     */
    public static ResponseDefinitionBuilder aGrpcResponseWith(MessageOrBuilder messageOrBuilder) {
        return aResponse()
                .withHeader("Content-Type", "application/json") // gRPC extension expects JSON
                .withBody(ProtoJson.toJson(messageOrBuilder));
    }

    /**
     * Configures a WireMock response to return a gRPC status code.
     *
     * @param statusCode The gRPC status code (e.g., {@code Status.Code.NOT_FOUND.value()}).
     * @param message    An optional error message.
     * @return A {@link ResponseDefinitionBuilder} configured with the gRPC status.
     */
    public static ResponseDefinitionBuilder aGrpcErrorResponse(int statusCode, String message) {
        return aResponse()
                .withStatus(200) // gRPC uses HTTP 200 for all responses, status in trailers
                .withHeader("grpc-status", String.valueOf(statusCode))
                .withHeader("grpc-message", message != null ? message : "")
                .withBody("");
    }
}



