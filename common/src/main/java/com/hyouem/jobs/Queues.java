package com.hyouem.jobs;
public final class Queues {
 private Queues() {}
 public static final String EXCHANGE="jobs.exchange", ROUTING="jobs.process", QUEUE="jobs.queue";
 public static final String RETRY_EXCHANGE="jobs.retry.exchange", RETRY_QUEUE="jobs.retry.queue";
 public static final String DLX="jobs.dead.exchange", DLQ="jobs.dead.queue";
}
