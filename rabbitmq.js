// rabbitmq.js — dùng chung cho tất cả service
// Cùng pattern với các service khác: queue trực tiếp, không dùng exchange
const amqp = require("amqplib");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://192.168.1.6:5672";

let channel;

async function connect() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();
  console.log("[RabbitMQ] ✅ Connected to RabbitMQ");
  return channel;
}

async function publish(queue, message) {
  if (!channel) await connect();
  await channel.assertQueue(queue, { durable: true });
  channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), {
    persistent: true,
  });
  console.log(`[RabbitMQ] 📤 Published to [${queue}]:`, message);
}

async function consume(queue, handler) {
  if (!channel) await connect();
  await channel.assertQueue(queue, { durable: true });
  channel.consume(queue, (msg) => {
    if (msg !== null) {
      const content = JSON.parse(msg.content.toString());
      console.log(`[RabbitMQ] 📥 Consumed from [${queue}]:`, content);
      handler(content);
      channel.ack(msg);
    }
  });
}

module.exports = { connect, publish, consume };
