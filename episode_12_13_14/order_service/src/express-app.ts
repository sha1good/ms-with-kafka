import express, { NextFunction, Request, Response } from "express";
import cors from "cors";
import cartRoutes from "./routes/cart.routes";
import orderRoutes from "./routes/order.routes";
import { httpLogger, HandleErrorWithLogger, MessageBroker } from "./utils";
import { Producer, Consumer } from "kafkajs";

 export const ExpressApp = async () =>{
  const app = express();
  app.use(cors());
  app.use(express.json());
  app.use(httpLogger);

  // First step is to connect to producer and consumer

  const producer = await MessageBroker.connectProducer<Producer>()
  producer.on("producer.connect", () =>{
       console.log("Producer Connected!")
  })
  

  
  const consumer = await MessageBroker.connectProducer<Consumer>()
  consumer.on("consumer.connect", () =>{
       console.log("consumer Connected!")
  })

  // Subscribe to the topic or Publish the Message
  await MessageBroker.subscribe((message) => {
   console.log("Consumer received the message")
   console.log("Message received the message", message)
  }, "OrderEvents")

  
  app.use(cartRoutes);
  app.use(orderRoutes);
  
  app.use("/", (req: Request, res: Response, _: NextFunction) => {
    return res.status(200).json({ message: "I am healthy!" });
  });
  
  app.use(HandleErrorWithLogger);

   return app;
 }
