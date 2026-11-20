package io.github.abhayrkhot.ledger.account;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/accounts")
@Validated
class AccountController {

    private final AccountService accounts;

    AccountController(AccountService accounts) {
        this.accounts = accounts;
    }

    record OpenAccountRequest(
            @NotBlank @Size(max = 128) String ownerId,
            @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency,
            Account.Type type) {}

    @PostMapping
    ResponseEntity<Account> open(@Valid @RequestBody OpenAccountRequest request) {
        Account.Type type = request.type() == null ? Account.Type.CUSTOMER : request.type();
        Account account = accounts.open(request.ownerId(), type, request.currency());
        return ResponseEntity.created(URI.create("/api/v1/accounts/" + account.id())).body(account);
    }

    @GetMapping("/{id}")
    AccountSummary get(@PathVariable UUID id) {
        return accounts.summary(id);
    }

    @GetMapping("/{id}/entries")
    List<AccountSummary.Entry> entries(
            @PathVariable UUID id,
            @RequestParam(required = false) Long beforeId,
            @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
        return accounts.entries(id, beforeId, limit);
    }
}
