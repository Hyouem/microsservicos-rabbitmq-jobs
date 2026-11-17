package com.hyouem.jobs;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitTopologyConfig {

    @Bean
    Declarables topology() {
        var main = new DirectExchange(Queues.EXCHANGE, true, false);
        var retry = new DirectExchange(Queues.RETRY_EXCHANGE, true, false);
        var dead = new DirectExchange(Queues.DLX, true, false);

        var queue = QueueBuilder.durable(Queues.QUEUE).build();

        var retryQueue = QueueBuilder.durable(Queues.RETRY_QUEUE)
                .ttl(3000)
                .deadLetterExchange(Queues.EXCHANGE)
                .deadLetterRoutingKey(Queues.ROUTING)
                .build();

        var deadQueue = QueueBuilder.durable(Queues.DLQ).build();

        return new Declarables(
                main,
                retry,
                dead,
                queue,
                retryQueue,
                deadQueue,
                BindingBuilder.bind(queue).to(main).with(Queues.ROUTING),
                BindingBuilder.bind(retryQueue).to(retry).with(Queues.ROUTING),
                BindingBuilder.bind(deadQueue).to(dead).with(Queues.ROUTING)
        );
    }
}