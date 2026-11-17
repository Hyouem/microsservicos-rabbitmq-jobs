package com.hyouem.jobs;
import java.util.UUID;
public record JobMessage(UUID jobId, String payload) {}
