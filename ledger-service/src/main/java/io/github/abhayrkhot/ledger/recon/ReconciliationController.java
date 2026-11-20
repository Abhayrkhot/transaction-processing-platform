package io.github.abhayrkhot.ledger.recon;

import io.github.abhayrkhot.ledger.common.LedgerException;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
class ReconciliationController {

    private final ReconciliationService recon;

    ReconciliationController(ReconciliationService recon) {
        this.recon = recon;
    }

    /** Body: settlement CSV ({@value SettlementLine#HEADER}). Window applies to ledger occurred_at. */
    @PostMapping(path = "/api/v1/reconciliations", consumes = "text/csv")
    ReconciliationService.Report reconcile(
            @RequestParam Instant from, @RequestParam Instant to, HttpServletRequest request) throws IOException {
        if (!from.isBefore(to)) {
            throw LedgerException.badRequest("BAD_WINDOW", "from must be before to");
        }
        var lines = SettlementLine.parse(new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8));
        return recon.reconcile(lines, from, to);
    }

    @GetMapping("/api/v1/ledger/integrity")
    ReconciliationService.Integrity integrity() {
        return recon.integrity();
    }
}
