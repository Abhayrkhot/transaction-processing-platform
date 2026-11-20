package io.github.abhayrkhot.ledger.transfer;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transfers")
@Validated
class TransferController {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    static final String REPLAYED = "Idempotent-Replayed";

    private final PostingService posting;

    TransferController(PostingService posting) {
        this.posting = posting;
    }

    record TransferRequest(
            @NotNull UUID fromAccountId,
            @NotNull UUID toAccountId,
            @Positive long amount,
            @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
            @Size(max = 128) String reference,
            Instant occurredAt) {}

    /**
     * POSTED -> 201, REJECTED (insufficient funds) -> 422. A retry with the same key returns
     * the original status and body with {@code Idempotent-Replayed: true}.
     */
    @PostMapping
    ResponseEntity<Transaction> transfer(
            @RequestHeader(IDEMPOTENCY_KEY) @Size(min = 1, max = 128) String idempotencyKey,
            @Valid @RequestBody TransferRequest request) {
        var cmd = new TransferCommand(idempotencyKey, request.fromAccountId(), request.toAccountId(),
                request.amount(), request.currency(), request.reference(), request.occurredAt());
        TransferResult result = posting.transfer(cmd, PostingService.Channel.API);
        Transaction txn = result.transaction();
        HttpStatus status = txn.status() == Transaction.Status.POSTED ? HttpStatus.CREATED : HttpStatus.UNPROCESSABLE_CONTENT;
        return ResponseEntity.status(status)
                .location(URI.create("/api/v1/transfers/" + txn.id()))
                .header(REPLAYED, Boolean.toString(result.replayed()))
                .body(txn);
    }

    @GetMapping("/{id}")
    Transaction get(@PathVariable UUID id) {
        return posting.get(id);
    }
}
