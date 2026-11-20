package io.github.abhayrkhot.ledger.transfer;

/** Outcome of a transfer; {@code replayed} is true when the key had already been processed. */
public record TransferResult(Transaction transaction, boolean replayed) {}
