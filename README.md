## KafkaProducer :    



## KafkaConsumer :  
  - Multithreaded KafkaConsumer implementation.
  - KafkaConsumer is a Kafka client that consumes records from a Kafka cluster.  
  - In this implementation of KafkaConsumer we use the records from Kafka belonging to same partition id, save them in a map and 
    print the record offset, timestamp and value.  
  - Kafka scales the process of consumption of data from topic, by distributing topic partitions among a consumer group. A 
    consumer group is set of consumer instances sharing a common group identifier(groupId).  
    <p align="center"> 
      <img width="500" alt="screen shot 2018-02-09 at 11 56 16 pm" src="https://user-images.githubusercontent.com/15849566/36060007-ebc3943e-0df4-11e8-801c-6ff4e8d65619.png">
    </p>
  
<p align="center">                                           
©Apache Kafka documentation
 </p>        
   
   ### Compiling:    
   ```bash
     mvn compile  
   ```
      
   ### Execute:
   ```bash  
     mvn exec:java -Dexec.mainClass=com.kafka.consumer.tushar.ConsumerMain -Dexec.args=$TOPIC_NAME  
   ```
