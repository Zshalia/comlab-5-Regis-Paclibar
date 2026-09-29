'use strict';

const amqp = require('amqplib');

const brokerUrl = process.env.BROKER_URL || 'amqp://guest:guest@localhost:5672';
const queueName = process.env.INVENTORY_QUEUE || 'inventory_queue';
const initialStock = process.env.INITIAL_STOCK || '{"SKU-001":100,"SKU-002":100}';

let stock;
try {
	stock = JSON.parse(initialStock);
	if (!stock || Array.isArray(stock) || typeof stock !== 'object') {
		throw new Error('INITIAL_STOCK must be a JSON object');
	}
} catch (error) {
	console.error(`Invalid INITIAL_STOCK: ${error.message}`);
	process.exit(1);
}

let connection;
let channel;
let reconnectTimer;
let stopping = false;

function reserveItems(order) {
	if (!order || !Array.isArray(order.items) || order.items.length === 0) {
		return { success: false, reason: 'Order must contain a non-empty items array' };
	}

	const requested = new Map();
	for (const item of order.items) {
		const sku = String(item.sku || item.productId || '');
		const quantity = Number(item.quantity);
		if (!sku || !Number.isInteger(quantity) || quantity <= 0) {
			return { success: false, reason: 'Each item needs a sku and positive integer quantity' };
		}
		requested.set(sku, (requested.get(sku) || 0) + quantity);
	}

	for (const [sku, quantity] of requested) {
		if (!Number.isInteger(stock[sku]) || stock[sku] < quantity) {
			return { success: false, reason: `Insufficient stock for ${sku}` };
		}
	}

	for (const [sku, quantity] of requested) {
		stock[sku] -= quantity;
	}

	return {
		success: true,
		items: Array.from(requested, ([sku, quantity]) => ({ sku, quantity, remaining: stock[sku] }))
	};
}

async function connect() {
	if (stopping) return;

	try {
		connection = await amqp.connect(brokerUrl);
		connection.on('error', (error) => console.error(`RabbitMQ connection error: ${error.message}`));
		connection.on('close', () => {
			channel = undefined;
			connection = undefined;
			if (!stopping && !reconnectTimer) {
				console.error('RabbitMQ connection closed; retrying in 5 seconds');
				reconnectTimer = setTimeout(() => {
					reconnectTimer = undefined;
					connect();
				}, 5000);
			}
		});

		channel = await connection.createChannel();
		await channel.assertQueue(queueName, { durable: true });
		await channel.prefetch(1);
		await channel.consume(queueName, async (message) => {
			if (!message) return;

			try {
				const order = JSON.parse(message.content.toString());
				const result = reserveItems(order);
				const response = {
					orderId: order.orderId || order.id || null,
					...result
				};

				if (message.properties.replyTo) {
					channel.sendToQueue(
						message.properties.replyTo,
						Buffer.from(JSON.stringify(response)),
						{ correlationId: message.properties.correlationId, persistent: true }
					);
				}

				console.log(`${response.success ? 'Reserved' : 'Rejected'} inventory for order ${response.orderId || '(unknown)'}`, response);
				channel.ack(message);
			} catch (error) {
				console.error(`Could not process inventory message: ${error.message}`);
				channel.nack(message, false, false);
			}
		});

		console.log(`Inventory service listening on queue "${queueName}"`);
	} catch (error) {
		console.error(`Could not connect to RabbitMQ: ${error.message}; retrying in 5 seconds`);
		if (connection) await connection.close().catch(() => {});
		connection = undefined;
		reconnectTimer = setTimeout(() => {
			reconnectTimer = undefined;
			connect();
		}, 5000);
	}
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
