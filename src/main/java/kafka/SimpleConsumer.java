

package kafka;

import java.util.Properties;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Date;
import java.sql.Timestamp;
import java.text.ParseException;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class SimpleConsumer {
	
	public static void main(String[] args) throws Exception {
		
		  String topicName = "test";
	      Properties props = new Properties();
	      
	      props.put("bootstrap.servers", "localhost:9092");
	      
	      props.put("group.id", "test");
	      
	      //-----------------------------------------------------------
	      // for normal flow
	      //-----------------------------------------------------------
	      	      
	      //props.put("enable.auto.commit", "true");
	      //props.put("auto.commit.interval.ms", "1000");
	      
	      //-----------------------------------------------------------
	      // for setting up the threshold
	      //-----------------------------------------------------------
	      
	      props.put("enable.auto.commit", "false");
	      props.put("max.poll.records","10");
	      
	      //------------------------------------------------------------
	      
	      
	      props.put("session.timeout.ms", "30000");
	      props.put("key.deserializer", 
	        "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	    
	   
	      
	      @SuppressWarnings("resource")
		org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
	      
	      //Kafka Consumer subscribes list of topics here.
	      consumer.subscribe(Arrays.asList(topicName));
	      
	      //print the topic name
	      System.out.println("Subscribed to topic " + topicName);
	      int i = 0;
	      	      
	      //consumerOffsetsForTimes(consumer);
	      
	      while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records)
	         {
	        	 
	        	 i++;	 
	        	 
	        	 Date date = new Date();
	        
	        	 //System.out.println(new Timestamp(date.getTime()));
	  	 
	        	//System.out.println("Batch Count - " + i);
	        	
	        	// print the offset,key and value for the consumer records.
	         System.out.printf("Time = %s,offset = %d, key = %s, value = %s\n", 
	        		 new Timestamp(date.getTime()),record.offset(), record.key(), record.value());
	        }
	         if(i>=5){
	        	System.out.println("Batch Count: " + i);
	        	consumer.commitSync();
	        	System.out.println("commiting the records");
	        	i=0;
	        }
	           
	      }
	            
	      
	   }
	
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
    public static void consumerOffsetsForTimes(KafkaConsumer<String, String> consumer) 
    {
    	
	  //WriteIntoFile writebuff = new WriteIntoFile();		
    	
      //Map<TopicPartition, Long> topicPartitionsWithTimestamps = new HashMap<>();
      
      TopicPartition topicPartition = new TopicPartition("test",0);
      
      
      String[] timestrings = {"02/10/2018 00:00:00","02/11/2018 00:00:00"};
      
      
      for (String timestring : timestrings ){
    	  
    	  search_timestamp_offset(consumer,topicPartition,timestring);
    	  
      }
      
      
      //converting time 
  
     /* 
      long epoch = 0L;
  	  try {
		epoch = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse("02/10/2018 00:00:00").getTime();
		
		//System.out.println("Epoch convertor: " + Long.toString(epoch));
		
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	*/
      // We are interested in the offset for data ingested 60 seconds ago
      //long timestamp = (System.currentTimeMillis() - 60000);
    
      
      
      /*
      
      long timestamp = get_epoch("02/10/2018 00:00:00");
      
      //System.out.println("Currrent time: " + timestamp);

      topicPartitionsWithTimestamps.put(topicPartition, timestamp);
      
      Map<TopicPartition, OffsetAndTimestamp> results = consumer.offsetsForTimes(topicPartitionsWithTimestamps);
      
      Collection<OffsetAndTimestamp> offsetAndTimestamp = results.values();
      
      /*
      for(TopicPartition result : results.keySet()){
    	  
    	 // System.out.println("Topic : " + result.topic());
    	   
      }
     
      */
      /*
      Iterator<OffsetAndTimestamp> itr = offsetAndTimestamp.iterator();
   
      while(itr.hasNext()){
    	  
    	  //System.out.println("Offset and TimeStamp" + itr.next().toString());
    	      	  
    	  OffsetAndTimestamp offset_timestamp = itr.next();
    	  
    	  //System.out.println("Offset: " + Long.toString(offset_timestamp.offset()));
    	  
    	  Long offsettimestamp = offset_timestamp.timestamp();
    	  
    	  //System.out.println("Offset: " + Long.toString(offset_timestamp.offset()));
    	  
    	  System.out.println("Timestamp: " + new Timestamp(offsettimestamp) + " Offset: " + Long.toString(offset_timestamp.offset()));
    	  
    	  //writebuff.write_in_file(new Timestamp(offsettimestamp) + "#" + Long.toString(offset_timestamp.offset()));
    	    
      }
      
      
      */
        	   	  
    //  System.out.println("Iterator size" + offsetAndTimestamp.size());
    	  
      }

    
    
    
    static void search_timestamp_offset(KafkaConsumer<String, String> consumer,TopicPartition topicPartition, String timestring){
    	  
    	
    	 Map<TopicPartition, Long> topicPartitionsWithTimestamps = new HashMap<>();
          
          long timestamp = get_epoch(timestring);
          
          //System.out.println("Currrent time: " + timestamp);

          topicPartitionsWithTimestamps.put(topicPartition, timestamp);
          
          Map<TopicPartition, OffsetAndTimestamp> results = consumer.offsetsForTimes(topicPartitionsWithTimestamps);
          
          Collection<OffsetAndTimestamp> offsetAndTimestamp = results.values();
          
          /*
          for(TopicPartition result : results.keySet()){
        	  
        	 // System.out.println("Topic : " + result.topic());
        	   
          }
         
          */
          
          Iterator<OffsetAndTimestamp> itr = offsetAndTimestamp.iterator();
       
          while(itr.hasNext()){
        	  
        	  //System.out.println("Offset and TimeStamp" + itr.next().toString());
        	      	  
        	  OffsetAndTimestamp offset_timestamp = itr.next();
       	  
        	  Long offsettimestamp = offset_timestamp.timestamp();
        	          	  
        	  System.out.println("Timestamp: " + new Timestamp(offsettimestamp) + " Offset: " + Long.toString(offset_timestamp.offset()));
        	  
        	  //writebuff.write_in_file(new Timestamp(offsettimestamp) + "#" + Long.toString(offset_timestamp.offset()));
        	    
          }
       
    	  
      }
    
    
      
    /*
      // Convenience method for single-partition lookup
      consumer.offsetsForTimes(topicPartition, timestamp, done -> {
        if(done.succeeded()) {
          OffsetAndTimestamp offsetAndTimestamp = done.result();
            System.out.println("Offset for topic="+topicPartition.getTopic()+
              ", partition="+topicPartition.getPartition()+"\n"+
              ", timestamp="+timestamp+", offset="+offsetAndTimestamp.getOffset()+
              ", offsetTimestamp="+offsetAndTimestamp.getTimestamp());

        }
      });
  }*/
	

     public static long get_epoch (String timestamp){
    	
        long epoch = 0L;
        
    	try {
  		epoch = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse(timestamp).getTime();
  		
  		//System.out.println("Epoch convertor: " + Long.toString(epoch));
  		
  		
  	} catch (ParseException e) {
  		// TODO Auto-generated catch block
  		e.printStackTrace();
  	}
    	return epoch;
    }
    
    
}
