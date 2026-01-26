package ai.pipestream.wiremock.server;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WireMockHttpMappingsTest {

    private WireMockServer wireMockServer;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(
            wireMockConfig()
                .dynamicPort()
                .usingFilesUnderClasspath("wiremock")
        );
        wireMockServer.start();
        httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    }

    @AfterEach
    void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void healthCheckEndpoint_returnsUpStatus() throws Exception {
        String url = "http://localhost:" + wireMockServer.port() + "/q/health";
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(), "Health check should return HTTP 200");
        assertTrue(response.headers().firstValue("Content-Type").orElse("").contains("application/json"),
            "Health check should return JSON content");
        assertTrue(response.body().contains("\"status\""), "Health check body should include status field");
        assertTrue(response.body().contains("UP"), "Health check should report UP");
    }

    @Test
    void openApiEndpoint_returnsSchemaDocument() throws Exception {
        String url = "http://localhost:" + wireMockServer.port() + "/openapi.json";
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(), "OpenAPI should return HTTP 200");
        assertTrue(response.headers().firstValue("Content-Type").orElse("").contains("application/json"),
            "OpenAPI should return JSON content");
        assertTrue(response.body().contains("\"openapi\""), "OpenAPI document should include openapi field");
        assertTrue(response.body().contains("\"/q/health\""),
            "OpenAPI document should include /q/health path");
    }
}
