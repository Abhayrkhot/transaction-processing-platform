package io.github.abhayrkhot.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abhayrkhot.ledger.account.Account;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;

class TransferApiIT extends IntegrationTest {

    @Value("${local.server.port}")
    int port;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void transferLifecycleOverHttp() throws Exception {
        Account a = customer(10_000);
        Account b = customer(0);
        String key = UUID.randomUUID().toString();
        String body = transferJson(a, b, 1_200);

        var created = post("/api/v1/transfers", key, body);
        assertThat(created.statusCode()).isEqualTo(201);
        assertThat(created.headers().firstValue("Idempotent-Replayed")).contains("false");
        assertThat(created.body()).contains("\"status\":\"POSTED\"").doesNotContain("requestHash");

        var replay = post("/api/v1/transfers", key, body);
        assertThat(replay.statusCode()).isEqualTo(201);
        assertThat(replay.headers().firstValue("Idempotent-Replayed")).contains("true");

        assertThat(post("/api/v1/transfers", key, transferJson(a, b, 1_300)).statusCode()).isEqualTo(409);
        assertThat(post("/api/v1/transfers", UUID.randomUUID().toString(), transferJson(a, b, 999_999)).statusCode()).isEqualTo(422);
        assertThat(post("/api/v1/transfers", null, body).statusCode()).isEqualTo(400);
        assertThat(post("/api/v1/transfers", UUID.randomUUID().toString(), transferJson(a, b, 0)).statusCode()).isEqualTo(400);

        var account = get("/api/v1/accounts/" + b.id());
        assertThat(account.statusCode()).isEqualTo(200);
        assertThat(account.body()).contains("\"balance\":1200");
        assertThat(get("/api/v1/accounts/" + UUID.randomUUID()).statusCode()).isEqualTo(404);
    }

    private static String transferJson(Account from, Account to, long amount) {
        return """
                {"fromAccountId":"%s","toAccountId":"%s","amount":%d,"currency":"USD"}
                """.formatted(from.id(), to.id(), amount);
    }

    private HttpResponse<String> post(String path, String key, String json) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json));
        if (key != null) {
            request.header("Idempotency-Key", key);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
