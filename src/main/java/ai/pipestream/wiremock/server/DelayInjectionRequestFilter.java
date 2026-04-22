package ai.pipestream.wiremock.server;

import com.github.tomakehurst.wiremock.extension.requestfilter.RequestFilterAction;
import com.github.tomakehurst.wiremock.extension.requestfilter.StubRequestFilterV2;
import com.github.tomakehurst.wiremock.http.HttpHeader;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.jboss.logging.Logger;

/**
 * WireMock stub request filter that honors the {@code x-test-delay-ms} request header
 * by sleeping the matching server thread for that many milliseconds <em>before</em>
 * stub matching/response generation runs.
 *
 * <p>This applies uniformly to every stub registered against the main WireMock server,
 * including HTTP stubs and gRPC stubs served via {@code wiremock-grpc-extension}. It is
 * the canonical mechanism for tests that need to simulate hung or slow downstream
 * dependencies (gRPC and HTTP alike) without writing per-mock {@code fixedDelayMilliseconds}
 * in stub definitions.</p>
 *
 * <p>Behaviour:</p>
 * <ul>
 *   <li>Header missing or non-numeric: filter is a no-op.</li>
 *   <li>Header {@code <= 0}: no-op.</li>
 *   <li>Header {@code > 0}: current thread sleeps for that many milliseconds, then the
 *       request continues unchanged. If interrupted, the interrupt flag is restored and
 *       processing continues so the client sees a normal (fast) response rather than a
 *       hang.</li>
 * </ul>
 *
 * <p>Admin endpoints are unaffected so {@code /__admin/*} stays responsive even when a
 * test has set a global delay header on its client interceptor.</p>
 */
public class DelayInjectionRequestFilter implements StubRequestFilterV2 {

    private static final Logger LOG = Logger.getLogger(DelayInjectionRequestFilter.class);

    /** Header name clients send to request a delay. Lowercased per HTTP/2 / gRPC custom-metadata convention. */
    public static final String DELAY_HEADER = "x-test-delay-ms";

    @Override
    public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
        HttpHeader header = request.header(DELAY_HEADER);
        if (header == null || !header.isPresent()) {
            return RequestFilterAction.continueWith(request);
        }
        String raw = header.firstValue();
        long delayMs;
        try {
            delayMs = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            LOG.debugf("Ignoring non-numeric %s header value: %s", DELAY_HEADER, raw);
            return RequestFilterAction.continueWith(request);
        }
        if (delayMs <= 0) {
            return RequestFilterAction.continueWith(request);
        }

        LOG.debugf("Honoring %s=%dms for %s request",
                DELAY_HEADER, delayMs, request.getMethod());
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debugf("Interrupted while sleeping for %s=%dms; continuing without further delay",
                    DELAY_HEADER, delayMs);
        }
        return RequestFilterAction.continueWith(request);
    }

    @Override
    public boolean applyToAdmin() {
        return false;
    }

    @Override
    public boolean applyToStubs() {
        return true;
    }

    @Override
    public String getName() {
        return "delay-injection-request-filter";
    }
}
