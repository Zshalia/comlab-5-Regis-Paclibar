'use strict';

const http = require('node:http');
const { randomUUID } = require('node:crypto');
const amqp = require('amqplib');

const port = Number(process.env.PORT || 3000);
const brokerUrl = process.env.BROKER_URL || 'amqp://guest:guest@localhost:5672';
const exchangeName = process.env.EVENT_EXCHANGE || 'shop.events';
const inventoryQueue = process.env.INVENTORY_QUEUE || 'inventory_queue';
const pendingInventory = new Map();
let connection;
let channel;
let server;
let reconnectTimer;
let stopping = false;

const wait = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function connectBroker() {
  while (!stopping) {
    try {
      connection = await amqp.connect(brokerUrl);
      let established = false;
      connection.on('error', (error) => console.error(`RabbitMQ connection error: ${error.message}`));
      connection.on('close', () => {
        connection = undefined;
        channel = undefined;
        for (const [correlationId, pending] of pendingInventory) {
          clearTimeout(pending.timeout);
          pending.reject(new Error('RabbitMQ connection closed'));
          pendingInventory.delete(correlationId);
        }
        if (established && !stopping && !reconnectTimer) {
          reconnectTimer = setTimeout(() => {
            reconnectTimer = undefined;
            connectBroker();
          }, 5000);
        }
      });

      channel = await connection.createConfirmChannel();
      await channel.assertExchange(exchangeName, 'topic', { durable: true });
      await channel.assertQueue(inventoryQueue, { durable: true });
      const replyQueue = await channel.assertQueue('', { exclusive: true, autoDelete: true });
      pendingReplyQueue = replyQueue.queue;
      await channel.consume(pendingReplyQueue, (message) => {
        if (!message) return;
        const pending = pendingInventory.get(message.properties.correlationId);
        if (pending) {
          clearTimeout(pending.timeout);
          pendingInventory.delete(message.properties.correlationId);
          try {
            pending.resolve(JSON.parse(message.content.toString()));
          } catch (error) {
            pending.reject(error);
          }
        }
        channel.ack(message);
      });
      established = true;
      console.log(`Connected to RabbitMQ; events use exchange "${exchangeName}"`);
      return;
    } catch (error) {
      console.error(`RabbitMQ unavailable: ${error.message}; retrying in 5 seconds`);
      if (connection) await connection.close().catch(() => {});
      connection = undefined;
      channel = undefined;
      await wait(5000);
    }
  }
}

function publishConfirmed(exchange, routingKey, payload) {
  return new Promise((resolve, reject) => {
    channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(payload)), { persistent: true }, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

function reserveInventory(order) {
  return new Promise((resolve, reject) => {
    const correlationId = randomUUID();
    const timeout = setTimeout(() => {
      pendingInventory.delete(correlationId);
      reject(new Error('Inventory service did not respond in time'));
    }, Number(process.env.INVENTORY_TIMEOUT_MS || 10000));

    pendingInventory.set(correlationId, { resolve, reject, timeout });
    channel.sendToQueue(inventoryQueue, Buffer.from(JSON.stringify(order)), {
      correlationId,
      replyTo: pendingReplyQueue,
      persistent: true
    });
  });
}

let pendingReplyQueue;

function readJson(request) {
  return new Promise((resolve, reject) => {
    let body = '';
    request.on('data', (chunk) => {
      body += chunk;
      if (body.length > 1024 * 1024) {
        reject(Object.assign(new Error('Request body is too large'), { statusCode: 413 }));
        request.destroy();
      }
    });
    request.on('end', () => {
      try {
        resolve(JSON.parse(body || '{}'));
      } catch {
        reject(Object.assign(new Error('Request body must be valid JSON'), { statusCode: 400 }));
      }
    });
    request.on('error', reject);
  });
}

function validateOrder(body) {
  if (!Array.isArray(body.items) || body.items.length === 0) return 'items must be a non-empty array';
  for (const item of body.items) {
    if (!(item.sku || item.productId) || !Number.isInteger(Number(item.quantity)) || Number(item.quantity) <= 0) {
      return 'each item must include a sku and a positive integer quantity';
    }
  }
  if (!Number.isFinite(Number(body.totalAmount)) || Number(body.totalAmount) <= 0) {
    return 'totalAmount must be a positive number';
  }
  return null;
}

async function handleRequest(request, response) {
  const url = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
  if (request.method === 'GET' && url.pathname === '/health') {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ status: 'ok', brokerConnected: Boolean(channel) }));
    return;
  }
  if (request.method !== 'POST' || url.pathname !== '/orders') {
    response.writeHead(404, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ error: 'Not found' }));
    return;
  }

  try {
    const body = await readJson(request);
    const validationError = validateOrder(body);
    if (validationError) {
      response.writeHead(400, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ error: validationError }));
      return;
    }

    if (!channel || !pendingReplyQueue) throw Object.assign(new Error('Order service is not connected to RabbitMQ'), { statusCode: 503 });
    const order = {
      orderId: randomUUID(),
      items: body.items.map((item) => ({ sku: String(item.sku || item.productId), quantity: Number(item.quantity) })),
      totalAmount: Number(body.totalAmount),
      customer: body.customer || null,
      createdAt: new Date().toISOString()
    };

    const inventory = await reserveInventory(order);
    if (!inventory.success) {
      response.writeHead(409, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ error: inventory.reason || 'Inventory reservation failed', orderId: order.orderId }));
      return;
    }

    order.inventory = inventory.items;
    await publishConfirmed(exchangeName, 'order.placed', order);
    response.writeHead(201, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ status: 'placed', order }));
  } catch (error) {
    console.error(`Order request failed: ${error.message}`);
    response.writeHead(error.statusCode || 502, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ error: error.message }));
  }
}

async function start() {
  await connectBroker();
  server = http.createServer((request, response) => {
    handleRequest(request, response).catch((error) => {
      console.error(`Unhandled request error: ${error.message}`);
      if (!response.headersSent) response.writeHead(500, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ error: 'Internal server error' }));
    });
  });
  server.listen(port, '0.0.0.0', () => console.log(`Order service listening on port ${port}`));
}

async function shutdown() {
  stopping = true;
  clearTimeout(reconnectTimer);
  if (server) server.close();
  for (const pending of pendingInventory.values()) {
    clearTimeout(pending.timeout);
    pending.reject(new Error('Order service is shutting down'));
  }
  pendingInventory.clear();
  if (channel) await channel.close().catch(() => {});
  if (connection) await connection.close().catch(() => {});
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

start().catch((error) => {
  console.error(`Order service failed to start: ${error.message}`);
  process.exit(1);
});
