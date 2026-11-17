package com.hyouem.jobs.worker;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyouem.jobs.JobMessage;
import com.hyouem.jobs.Queues;
import com.rabbitmq.client.Channel;
import java.nio.charset.StandardCharsets;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
@Component
public class JobConsumer {
 private final JdbcTemplate db;private final RabbitTemplate rabbit;private final ObjectMapper json;
 public JobConsumer(JdbcTemplate db,RabbitTemplate rabbit,ObjectMapper json){this.db=db;this.rabbit=rabbit;this.json=json;}
 @RabbitListener(queues=Queues.QUEUE)
 public void consume(Message message,Channel channel) throws Exception {
  long tag=message.getMessageProperties().getDeliveryTag();
  JobMessage job;
  try{job=json.readValue(message.getBody(),JobMessage.class);}catch(Exception e){rabbit.send(Queues.DLX,Queues.ROUTING,message);channel.basicAck(tag,false);return;}
  try{
   int claimed=db.update("update jobs set status='PROCESSING',attempts=attempts+1,updated_at=now() where id=? and status in ('PENDING','FAILED') and attempts<3",job.jobId());
   if(claimed==0){channel.basicAck(tag,false);return;}
   int attempt=db.queryForObject("select attempts from jobs where id=?",Integer.class,job.jobId());
   try{
    if(job.payload().startsWith("FAIL:")) throw new IllegalStateException("Simulated processing failure");
    // A real external side effect must use the job ID as its idempotency key.
    db.update("update jobs set status='COMPLETED',error=null,updated_at=now() where id=?",job.jobId());
   }catch(Exception processing){
    db.update("update jobs set status='FAILED',error=?,updated_at=now() where id=?",processing.getMessage(),job.jobId());
    rabbit.send(attempt>=3?Queues.DLX:Queues.RETRY_EXCHANGE,Queues.ROUTING,message);
   }
   channel.basicAck(tag,false);
  }catch(Exception e){channel.basicNack(tag,false,true);throw e;}
 }
}
