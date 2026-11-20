package io.github.abhayrkhot.ledger.recon;

import io.github.abhayrkhot.ledger.common.LedgerException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * One line of a settlement file from the payment network:
 * {@code reference,amount,currency,value_date} with amount in minor units.
 */
record SettlementLine(String reference, long amount, String currency, Instant valueDate) {

    static final String HEADER = "reference,amount,currency,value_date";

    static List<SettlementLine> parse(Reader input) throws IOException {
        List<SettlementLine> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(input)) {
            String header = reader.readLine();
            if (header == null || !header.trim().equalsIgnoreCase(HEADER)) {
                throw LedgerException.badRequest("BAD_SETTLEMENT_FILE", "expected header: " + HEADER);
            }
            String line;
            int number = 1;
            while ((line = reader.readLine()) != null) {
                number++;
                if (line.isBlank()) {
                    continue;
                }
                String[] f = line.split(",", -1);
                if (f.length != 4 || f[0].isBlank()) {
                    throw bad(number, "expected 4 fields");
                }
                try {
                    lines.add(new SettlementLine(f[0].trim(), Long.parseLong(f[1].trim()), f[2].trim(), Instant.parse(f[3].trim())));
                } catch (NumberFormatException | DateTimeParseException e) {
                    throw bad(number, e.getMessage());
                }
            }
        }
        return lines;
    }

    private static LedgerException bad(int number, String why) {
        return LedgerException.badRequest("BAD_SETTLEMENT_FILE", "line " + number + ": " + why);
    }
}
