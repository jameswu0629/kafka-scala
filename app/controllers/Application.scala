package controllers

import play.api._
import play.api.mvc._

import java.util.Properties
import java.util.Date
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.HashMap
import java.util.List
import java.util.Map

import scala.util.{Random => rnd}
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig


import kafka.api.FetchRequest
import kafka.api.FetchRequestBuilder
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.javaapi._
import kafka.javaapi.consumer.SimpleConsumer
import kafka.message.MessageAndOffset

class SimpleExample 
{
  private var m_replicaBrokers:List[String] = new ArrayList[String]
  private var numConsumerFetchError         = 0
  private var a_maxReads:Long               = 5
  private var consumer:SimpleConsumer       = null

  def SimpleExample() 
  {
    m_replicaBrokers = new ArrayList[String]
  }
  
  def run(a_topic:String, a_partition:Int, a_seedBrokers:List[String], a_port:Int) = 
  {
    val metadata:PartitionMetadata  = findLeader(a_seedBrokers, a_port, a_topic, a_partition)
    
    if (metadata == null)
    {
      throw new Exception("Can't fine metadata for Topic and Partition. Exiting")
    }
    
    if (metadata.leader == null)
    {
      throw new Exception("Can't find Leader for Topic and Partition. Exiting")
    }
    
    var leadBroker:String = metadata.leader.host
    val clientName:String = "Client_%s_%s".format(a_topic, a_partition)
    
    consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName)
    var readOffset:Long = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime, clientName)       
    
    var numErrors:Int = 0
        
    breakable
    { 
      while(a_maxReads > 0)
      {
        val fetchResponse:FetchResponse = fetchResponseFromBroker(leadBroker, a_topic, a_partition, a_port, clientName, readOffset)
        numConsumerFetchError = 0
        
        var numRead:Long = 0
        fetchResponse.messageSet(a_topic, a_partition).toList.foreach
        {
          messageAndOffset =>
            var currentOffset:Long = messageAndOffset.offset
            if (currentOffset < readOffset)
            {
              println("Found an old offset: %d Expecting: %d".format(currentOffset, readOffset))
            }
            else
            {
              readOffset = messageAndOffset.nextOffset
              var payload:ByteBuffer = messageAndOffset.message.payload
              
              var bytes:Array[Byte] = new Array[Byte](payload.limit)
              payload.get(bytes)
              println ("%s: %s".format(messageAndOffset.offset.toString, new String(bytes, "UTF-8")))
              
              numRead+=1
              a_maxReads-=1
            }
        }
        
        if (numRead == 0)
        {
          try 
          {
            Thread.sleep(1000)
          }
          catch
          {
            case ie:InterruptedException =>
              println (ie.printStackTrace)
          }
        }
        
      }   
      
      if (consumer != null) consumer.close
    }
  }

  def fetchResponseFromBroker(leadBroker:String, a_topic:String, a_partition:Int, a_port:Int, clientName:String, readOffset:Long): FetchResponse =
  {
    if (consumer == null)
    {
      consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName)
    }
     
    val req:FetchRequest = new FetchRequestBuilder()
      .clientId(clientName)
      .addFetch(a_topic, a_partition, readOffset, 100000)
      .build
                         
    val fetchResponse:FetchResponse = consumer.fetch(req)
  
    if (fetchResponse.hasError)
    {
      numConsumerFetchError+=1
      val code:Short = fetchResponse.errorCode(a_topic, a_partition)
      println("Error fetching data from the Broker:%s Reason: %d".format(leadBroker, code))
    
      if (numConsumerFetchError > 5) throw new Exception("numConsumerFetchError > 5")
    
      if (code == ErrorMapping.OffsetOutOfRangeCode)
      {
        // We asked for an invalid offset. For simple case ask for the last element to reset
        var anotherOffset = getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.LatestTime, clientName)
        fetchResponseFromBroker(leadBroker, a_topic, a_partition, a_port, clientName, anotherOffset)
      }
    
      consumer.close
      consumer = null
      var anotherBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port)
      fetchResponseFromBroker(anotherBroker, a_topic, a_partition, a_port, clientName, readOffset)
    }
    
    fetchResponse
  }
  
  def findNewLeader(a_oldLeader:String, a_topic:String, a_partition:Int, a_port:Int): String = 
  {
    var host = ""
    
    breakable 
    {
      // retry 3 times
      var x = 1
      for (x <- 1 to 3)
      {
         var goToSleep:Boolean = false
         val metadata:PartitionMetadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition)
       
         if (metadata == null)
         {
           goToSleep = true
         }
         else if (metadata.leader == null)
         {
           goToSleep = true
         }
         else if (a_oldLeader.equalsIgnoreCase(metadata.leader.host) && x == 0)
         {
           goToSleep = true
         }
         
         if (goToSleep)
         {
           try 
           {
             Thread.sleep(1000)
           }
           catch
           {
             case ie:InterruptedException =>
               println (ie.printStackTrace)
           }           
         }
         else
         {
           host = metadata.leader.host
           break
         }
      }   
    }
    
    if (host.isEmpty)
    {
      println("Unable to find new leader after Broker failure. Exiting")
      throw new Exception("Unable to find new leader after Broker failure. Exiting")
    }
    
    host
  }
  
  def getLastOffset(consumer:SimpleConsumer, topic:String, partition:Int, whichTime:Long, clientName:String) : Long = 
  {
    val topicAndPartition:TopicAndPartition = new TopicAndPartition(topic, partition)
    val requestInfo = new HashMap[TopicAndPartition, PartitionOffsetRequestInfo]
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1))

    val request:kafka.javaapi.OffsetRequest = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)
    val response:OffsetResponse             = consumer.getOffsetsBefore(request)

    response.hasError match
    {
      case true =>
        println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition))
        0
      case _ =>
        val offsets:Array[Long] = response.offsets(topic, partition)
        offsets(0)
    }
  }
  
  private def findLeader(a_seedBrokers:List[String], a_port:Int, a_topic:String, a_partition:Int) : PartitionMetadata = 
  {
    var returnMetaData:PartitionMetadata = null
    
    breakable 
    {
      a_seedBrokers.toList.foreach
      {
        seed =>
          var consumer:SimpleConsumer = null
          try
          {
            consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup")
            var topics:List[String] = new ArrayList[String]
            topics.add(a_topic)

            val req:TopicMetadataRequest                 = new TopicMetadataRequest(topics)
            val resp:kafka.javaapi.TopicMetadataResponse = consumer.send(req)
            
            var metaData:List[TopicMetadata]  = resp.topicsMetadata
            metaData.toList.foreach
            {
              item =>
                item.partitionsMetadata.toList.foreach
                {
                  part =>
                    if (part.partitionId == a_partition) 
                    {
                      returnMetaData = part
                      break
                    }
                }
            }

          }
          catch
          {
            case e:Exception =>
              println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic + ", " + a_partition + "] Reason: " + e)
            
          }
          finally
          {
            if (consumer != null) consumer.close
          }      
      }
    }

    if (returnMetaData != null) 
    {
      m_replicaBrokers.clear
      
      returnMetaData.replicas.toList.foreach
      {
        replica =>
          m_replicaBrokers.add(replica.host)
      }
    }
    
    returnMetaData
  }
  
}

object Application extends Controller {
  
  def simpleconsumer = Action {
    val example:SimpleExample = new SimpleExample()
    val maxReads:Long      = 10
    val topic:String       = "consumer-test"
    val partition          = 1
    val port               = 2192
    
    val seeds:List[String] = new ArrayList[String]
    seeds.add("??")
    
    try
    {
      example.run(topic, partition, seeds, port)
    }
    catch 
    {
      case e:Exception => 
        e.printStackTrace
    }
    
    Ok("Hello world")
  }
  
  def index = Action {
    
    var props:java.util.Properties = new java.util.Properties
    props.put("metadata.broker.list", "127.0.0.1:9092,127.0.0.1:9093")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    //props.put("partitioner.class", "example.producer.SimplePartitioner")
    props.put("request.required.acks", "1")
 
    val config = new ProducerConfig(props)
    val producer:Producer[String, String] = new Producer[String, String](config)
    
    var x = 0
    for(x <- 1 to 100)
    {
      val runtime = new Date().getTime
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + ",www.example.com," + ip
      val data:KeyedMessage[String, String] = new KeyedMessage[String, String]("my-replicated-topic", ip, msg)
      producer.send(data)
    }
    
    producer.close
    
    Ok(views.html.index("Your new application is ready."))
  }
  
}
