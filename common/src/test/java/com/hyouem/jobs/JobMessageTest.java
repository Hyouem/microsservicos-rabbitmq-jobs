package com.hyouem.jobs;
import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;
class JobMessageTest { @Test void preservesFields(){UUID id=UUID.randomUUID();var msg=new JobMessage(id,"ok");assertEquals(id,msg.jobId());assertEquals("ok",msg.payload());} }
