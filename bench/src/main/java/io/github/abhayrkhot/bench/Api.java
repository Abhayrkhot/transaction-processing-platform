package io.github.abhayrkhot.bench;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Minimal client for the ledger REST API. */
final class Api {

    static final JsonMapper JSON = JsonMapper.builder().build();

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final String base;

    Api(String base) {
        this.base = base;
    }

    UUID openAccount(String owner, String type) {
        String body = "{\"ownerId\":\"%s\",\"currency\":\"USD\",\"type\":\"%s\"}".formatted(owner, type);
        JsonNode node = JSON.readTree(send(post("/api/v1/accounts", body).build(), 201));
        return UUID.fromString(node.get("id").asString());
    }

    void transfer(String key, UUID from, UUID to, long amount, String reference, Instant occurredAt) {
        StringBuilder body = new StringBuilder()
                .append("{\"fromAccountId\":\"").append(from)
                .append("\",\"toAccountId\":\"").append(to)
                .append("\",\"amount\":").append(amount)
                .append(",\"currency\":\"USD\"");
        if (reference != null) {
            body.append(",\"reference\":\"").append(reference).append('"');
        }
        if (occurredAt != null) {
            body.append(",\"occurredAt\":\"").append(occurredAt).append('"');
        }
        body.append('}');
        send(post("/api/v1/transfers", body.toString()).header("Idempotency-Key", key).build(), 201);
    }

    JsonNode get(String path) {
        return JSON.readTree(send(HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofMinutes(5)).build(), 200));
    }

    JsonNode postCsv(String path, String csv) {
        return JSON.readTree(send(HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofMinutes(5))
                .header("Content-Type", "text/csv")
                .POST(HttpRequest.BodyPublishers.ofString(csv)).build(), 200));
    }

    private HttpRequest.Builder post(String path, String json) {
        return HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json));
    }

    private String send(HttpRequest request, int expected) {
        for (int attempt = 1; ; attempt++) {
            try {
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != expected) {
                    throw new IllegalStateException(request.method() + " " + request.uri() + " -> " + response.statusCode() + ": " + response.body());
                }
                return response.body();
            } catch (IOException e) {
                if (attempt == 5) {
                    throw new IllegalStateException(e);
                }
                sleep(200L * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    /** Runs task(0..n-1) on virtual threads with bounded concurrency; rethrows the first failure. */
    static void parallel(int n, int concurrency, IntConsumer task) {
        Semaphore permits = new Semaphore(concurrency);
        AtomicReference<RuntimeException> failure = new AtomicReference<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n && failure.get() == null; i++) {
                int index = i;
                permits.acquireUninterruptibly();
                pool.submit(() -> {
                    try {
                        task.accept(index);
                    } catch (RuntimeException e) {
                        failure.compareAndSet(null, e);
                    } finally {
                        permits.release();
                    }
                });
            }
        }
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
