package com.hyouem.jobs.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyouem.jobs.Queues;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class JobControllerTest {
    private JdbcTemplate db;
    private RabbitTemplate rabbit;
    private JobController controller;

    @BeforeEach
    void setup() {
        db = mock(JdbcTemplate.class);
        rabbit = mock(RabbitTemplate.class);
        controller = new JobController(db, rabbit, new ObjectMapper());
    }

    @Test
    void rejectsInvalidPayload() throws Exception {
        for (String payload : new String[]{null, "", " ", "x".repeat(1001)}) {
            var response = controller.create(new JobController.NewJob(payload));
            assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        }
        verifyNoInteractions(db, rabbit);
    }

    @Test
    void createsJobAndPublishesMessage() throws Exception {
        var response = controller.create(new JobController.NewJob("hello"));

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());

        @SuppressWarnings("unchecked")
        var body = (Map<String, Object>) response.getBody();

        assertNotNull(body);
        assertInstanceOf(UUID.class, body.get("id"));
        assertEquals("PENDING", body.get("status"));

        verify(db).update(
            startsWith("insert into jobs"),
            eq(body.get("id")),
            eq("hello")
        );

        verify(rabbit).convertAndSend(
            eq(Queues.EXCHANGE),
            eq(Queues.ROUTING),
            contains("hello")
        );
    }

    @Test
    void returnsExistingJob() {
        UUID id = UUID.randomUUID();
        Map<String, Object> job = Map.of("id", id, "status", "COMPLETED");

        when(db.queryForList(anyString(), eq(id))).thenReturn(List.of(job));

        var response = controller.get(id);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(job, response.getBody());
    }

    @Test
    void returnsNotFoundForUnknownJob() {
        UUID id = UUID.randomUUID();

        when(db.queryForList(anyString(), eq(id))).thenReturn(List.of());

        assertEquals(HttpStatus.NOT_FOUND, controller.get(id).getStatusCode());
    }

    @Test
    void marksJobFailedWhenPublishingFails() {
        doThrow(new IllegalStateException("broker unavailable"))
            .when(rabbit)
            .convertAndSend(
                eq(Queues.EXCHANGE),
                eq(Queues.ROUTING),
                anyString()
            );

        assertThrows(
            IllegalStateException.class,
            () -> controller.create(new JobController.NewJob("hello"))
        );

        verify(db).update(
            startsWith("update jobs set status='FAILED'"),
            eq("publish failed"),
            any(UUID.class)
        );
    }
}
