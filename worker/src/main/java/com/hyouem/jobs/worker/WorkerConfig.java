package com.hyouem.jobs.worker;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
@Configuration
public class WorkerConfig {
 @Bean com.fasterxml.jackson.databind.ObjectMapper objectMapper(){return new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();}
 @Bean SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory cf){
  var f=new SimpleRabbitListenerContainerFactory();f.setConnectionFactory(cf);f.setAcknowledgeMode(AcknowledgeMode.MANUAL);f.setPrefetchCount(1);return f;
 }
}
