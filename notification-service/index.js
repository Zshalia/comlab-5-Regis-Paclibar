'use strict';

const amqp = require('amqplib');

const brokerUrl = process.env.BROKER_URL || 'amqp://guest:guest@localhost:5672';
const exchangeName = process.env.EVENT_EXCHANGE || 'shop.events';
const queueName = process.env.NOTIFICATION_QUEUE || 'notification_queue';
let connection;
let channel;
let reconnectTimer;
let stopping = false;

async function connect() {
  if (stopping) return;
  try {
    connection = await amqp.connect(brokerUrl);
    connection.on('error', (error) => console.error(`RabbitMQ connection error: ${error.message}`));
    connection.on('close', () => {
      connection = undefined;
      channel = undefined;
      scheduleReconnect();
    });
    channel = await connection.createChannel();
    await channel.assertExchange(exchangeName, 'topic', { durable: true });
    await channel.assertQueue(queueName, { durable: true });
    await channel.bindQueue(queueName, exchangeName, 'payment.success');
    await channel.prefetch(1);

    await channel.consume(queueName, (message) => {
      if (!message) return;
      try {
        const payment = JSON.parse(message.content.toString());
        console.log(`Payment confirmation: order=${payment.orderId || '(unknown)'}, status=${payment.status || 'success'}, amount=${payment.amount ?? 'n/a'}`);
        channel.ack(message);
      } catch (error) {
        console.error(`Could not process payment notification: ${error.message}`);
        channel.nack(message, false, false);
      }
    });
    console.log(`Notification service listening for payment.success on "${exchangeName}"`);
  } catch (error) {
    console.error(`Could not connect to RabbitMQ: ${error.message}`);
    if (connection) await connection.close().catch(() => {});
    connection = undefined;
    channel = undefined;
    scheduleReconnect();
  }
}

function scheduleReconnect() {
  if (stopping || reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = undefined;
    connect();
  }, 5000);
}

async function shutdown() {
  stopping = true;
  clearTimeout(reconnectTimer);
  if (channel) await channel.close().catch(() => {});
  if (connection) await connection.close().catch(() => {});
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

connect();
