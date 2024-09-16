import { MessageType, OrderEvent, TOPIC_TYPE } from "../../types";
import { MessageBrokerType, MessageHandler, PublishType } from "./borker.type";

    

















     






 // Configuration Properties

 const CLIENT_ID  = process.env.CLIENT_ID || "order-service";
 const GROUP_ID = process.env.GROUP_ID ||  "order-service-group"; // Grouping the consumers together
 const BROKERS = [process.env.BROKER_1 || "localhost:9092", process.env.BROKER_2 || "localhost:9093"]


  const kafka = new Kafka({
      clientId: CLIENT_ID,
      brokers: BROKERS,
      logLevel: logLevel.info
  })

   let producer: Producer;

   let consumer: Consumer;


 const createTopic = async (topic: string[]) =>{
      const topics = topic.map((t) =>({
        topic: t,
        numPartitions: 2,
        replicationFactor: 1, // Based on the available number of brokers
        
  })) 

     const admin = kafka.admin();

     await admin.connect();
     const topicExists = await admin.listTopics();

  for( const t of topics){
       if(!topicExists.includes(t.topic)){
          await admin.createTopics({
          topics: [t]
          })
       }
  }

    admin.disconnect();
 }


 const connectProducer = async <T>(): Promise<T> =>{
      await createTopic(["OrderEvents"])

      if(producer){
          return producer as unknown as T;
      }

      producer = kafka.producer({
          createPartitioner: Partitioners.DefaultPartitioners
      })

       await producer.connect()
     return producer as unknown as T;
 }


 const disconnectProducer  = async (): Promise<void> =>{
      if( producer){
      await producer.disconnect();
      }
 }


 const publish = async (data: PublishType): Promise<boolean> =>{
    const producer = await connectProducer<Producer>();
   const  result = await  producer.send({
      topic: data.topic,
      messages: [{
      headers: data.headers,
      key: data.event,
      value: JSON.stringify(data.message)
      }]
   })

 //return !result;
   return result.lenggth > 0;

 }


 const connectConsumer = async <T>(): Promise<T> =>{
       if(consumer){
          return consumer as unknown as T;
       }
 }

 const disconnectConsumer = async (): Promise<void> =>{
    if(consumer){
    await consumer.disconnect();
    }
}


  const subscribe = async( messageHandler: MessageHandler, topic: TOPIC_TYPE,

  ): Promise<void>  =>{
      const consumer = await connectConsumer<Consumer>() // Producer and Consumer coming from Kafka
      await consumer.subscribe({ topic: topic, fromBegining: true});

       await consumer.run({
          eachMessage: async ({ topic, partition, message}) =>{
               if(topic !== "OrderEvents"){
                 return;
               }

               if(message.key && message.value){
                  const inputMessage: MessageType ={
                      headers: message.headers,
                      event: message.key.toString() as OrderEvent,
                      data: message.value ? JSON.stringify(message.value.toString()):  null
                  };

                  await messageHandler(inputMessage);
                  
                  await consumer.commitOffsets([{
                      topic, partition, offset: (Number(message.offset) + 1).toString()
                  }])
               }
          }
       })
  }
export const MessageBroker: MessageBrokerType = {

    connectProducer,
    disconnectProducer,
    publish,
    connectConsumer,
    disconnectConsumer,
    subscribe
}