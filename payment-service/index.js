'use strict';

const amqp = require('amqplib');

const brokerUrl = process.env.BROKER_URL || 'amqp://guest:guest@localhost:5672';
const exchangeName = process.env.EVENT_EXCHANGE || 'shop.events';
const queueName = process.env.PAYMENT_QUEUE || 'payment_queue';
const paymentApproved = process.env.PAYMENT_APPROVED !== 'false';
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
    channel = await connection.createConfirmChannel();
    await channel.assertExchange(exchangeName, 'topic', { durable: true });
    await channel.assertQueue(queueName, { durable: true });
    await channel.bindQueue(queueName, exchangeName, 'order.placed');
    await channel.prefetch(1);

    await channel.consume(queueName, (message) => {
      if (!message) return;
      processOrder(message).catch((error) => {
        console.error(`Payment processing failed: ${error.message}`);
        if (channel) channel.nack(message, false, true);
      });
    });
    console.log(`Payment service listening for order.placed on "${exchangeName}"`);
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

function publishConfirmed(routingKey, event) {
  return new Promise((resolve, reject) => {
    channel.publish(exchangeName, routingKey, Buffer.from(JSON.stringify(event)), { persistent: true }, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

async function processOrder(message) {
  const order = JSON.parse(message.content.toString());
  if (!order.orderId) throw new Error('Order event is missing orderId');

  const amount = Number(order.totalAmount);
  const approved = paymentApproved && Number.isFinite(amount) && amount > 0;
  const paymentEvent = {
    orderId: order.orderId,
    paymentId: `payment-${order.orderId}`,
    amount: Number.isFinite(amount) ? amount : null,
    status: approved ? 'success' : 'failed',
    reason: approved ? undefined : 'Payment was declined or amount was invalid',
    processedAt: new Date().toISOString()
  };

  await publishConfirmed(approved ? 'payment.success' : 'payment.failed', paymentEvent);
  console.log(`Payment ${paymentEvent.status} for order ${order.orderId}: ${paymentEvent.amount}`);
  channel.ack(message);
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
