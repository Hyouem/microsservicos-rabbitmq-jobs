package com.hyouem.jobs.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyouem.jobs.JobMessage;
import com.hyouem.jobs.Queues;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class JobConsumerTest {
    private JdbcTemplate db;
    private RabbitTemplate rabbit;
    private Channel channel;
    private JobConsumer consumer;
    private ObjectMapper mapper;

    @BeforeEach
    void setup() {
        db = mock(JdbcTemplate.class);
        rabbit = mock(RabbitTemplate.class);
        channel = mock(Channel.class);
        mapper = new ObjectMapper();
        consumer = new JobConsumer(db, rabbit, mapper);
    }

    private Message message(String payload) throws Exception {
        var job = new JobMessage(UUID.randomUUID(), payload);
        var properties = new MessageProperties();
        properties.setDeliveryTag(42L);
        return new Message(mapper.writeValueAsBytes(job), properties);
    }

    @Test
    void completesSuccessfulJob() throws Exception {
        when(db.update(startsWith("update jobs set status='PROCESSING'"), any(UUID.class)))
            .thenReturn(1);
        when(db.queryForObject(anyString(), eq(Integer.class), any(UUID.class)))
            .thenReturn(1);

        consumer.consume(message("hello"), channel);

        verify(db).update(
            startsWith("update jobs set status='COMPLETED'"),
            any(UUID.class)
        );
        verify(channel).basicAck(42L, false);
        verifyNoInteractions(rabbit);
    }

    @Test
    void retriesFailedJobBeforeThirdAttempt() throws Exception {
        when(db.update(startsWith("update jobs set status='PROCESSING'"), any(UUID.class)))
            .thenReturn(1);
        when(db.queryForObject(anyString(), eq(Integer.class), any(UUID.class)))
            .thenReturn(2);

        consumer.consume(message("FAIL: retry"), channel);

        verify(rabbit).send(
            eq(Queues.RETRY_EXCHANGE),
            eq(Queues.ROUTING),
            any(Message.class)
        );
        verify(channel).basicAck(42L, false);
    }

    @Test
    void sendsThirdFailureToDeadLetterQueue() throws Exception {
        when(db.update(startsWith("update jobs set status='PROCESSING'"), any(UUID.class)))
            .thenReturn(1);
        when(db.queryForObject(anyString(), eq(Integer.class), any(UUID.class)))
            .thenReturn(3);

        consumer.consume(message("FAIL: permanent"), channel);

        verify(rabbit).send(
            eq(Queues.DLX),
            eq(Queues.ROUTING),
            any(Message.class)
        );
        verify(channel).basicAck(42L, false);
    }

    @Test
    void sendsInvalidJsonToDeadLetterQueue() throws Exception {
        var properties = new MessageProperties();
        properties.setDeliveryTag(42L);
        var invalid = new Message(
            "{invalid".getBytes(StandardCharsets.UTF_8),
            properties
        );

        consumer.consume(invalid, channel);

        verify(rabbit).send(
            eq(Queues.DLX),
            eq(Queues.ROUTING),
            same(invalid)
        );
        verify(channel).basicAck(42L, false);
        verifyNoInteractions(db);
    }

    @Test
    void acknowledgesJobThatCannotBeClaimed() throws Exception {
        when(db.update(startsWith("update jobs set status='PROCESSING'"), any(UUID.class)))
            .thenReturn(0);

        consumer.consume(message("hello"), channel);

        verify(channel).basicAck(42L, false);
        verify(db, never()).queryForObject(
            anyString(),
            eq(Integer.class),
            any(UUID.class)
        );
        verifyNoInteractions(rabbit);
    }
}
