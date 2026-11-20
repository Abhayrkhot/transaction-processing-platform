package io.github.abhayrkhot.ledger.outbox;

import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {

    public record Message(long id, String topic, String key, String payload) {}

    private final JdbcClient jdbc;

    OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Must be called inside the transaction that produced the event. */
    public void append(String topic, String key, String payload) {
        jdbc.sql("INSERT INTO outbox (topic, message_key, payload) VALUES (:topic, :key, :payload)")
                .param("topic", topic)
                .param("key", key)
                .param("payload", payload)
                .update();
    }

    /**
     * Claims the oldest unpublished rows. SKIP LOCKED lets several relays run without
     * publishing the same row twice; rows inserted by transactions that are still open are
     * simply picked up by a later poll.
     */
    List<Message> claim(int limit) {
        return jdbc.sql("SELECT id, topic, message_key, payload FROM outbox ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED")
                .param("limit", limit)
                .query((rs, i) -> new Message(rs.getLong("id"), rs.getString("topic"), rs.getString("message_key"), rs.getString("payload")))
                .list();
    }

    void delete(List<Long> ids) {
        jdbc.sql("DELETE FROM outbox WHERE id IN (:ids)").param("ids", ids).update();
    }
}
