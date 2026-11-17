package com.hyouem.jobs.producer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyouem.jobs.JobMessage;
import com.hyouem.jobs.Queues;
import java.util.Map;
import java.util.UUID;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
@RestController @RequestMapping("/api/jobs")
public class JobController {
 private final JdbcTemplate db; private final RabbitTemplate rabbit; private final ObjectMapper json;
 public JobController(JdbcTemplate db,RabbitTemplate rabbit,ObjectMapper json){this.db=db;this.rabbit=rabbit;this.json=json;}
 public record NewJob(String payload) {}
 @PostMapping public ResponseEntity<?> create(@RequestBody NewJob req) throws Exception {
  if(req.payload()==null || req.payload().isBlank() || req.payload().length()>1000) return ResponseEntity.badRequest().body(Map.of("error","payload must have 1-1000 characters"));
  UUID id=UUID.randomUUID();
  db.update("insert into jobs(id,payload,status,attempts) values(?,?,'PENDING',0)",id,req.payload());
  try {rabbit.convertAndSend(Queues.EXCHANGE,Queues.ROUTING,json.writeValueAsString(new JobMessage(id,req.payload())));}
  catch(Exception e){db.update("update jobs set status='FAILED',error=? where id=?", "publish failed",id);throw e;}
  return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("id",id,"status","PENDING"));
 }
 @GetMapping("/{id}") public ResponseEntity<?> get(@PathVariable("id") UUID id){
  var list=db.queryForList("select id,payload,status,attempts,error,created_at,updated_at from jobs where id=?",id);
  return list.isEmpty()?ResponseEntity.notFound().build():ResponseEntity.ok(list.get(0));
 }
}
