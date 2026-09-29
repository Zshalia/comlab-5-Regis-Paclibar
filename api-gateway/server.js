'use strict';

const http = require('node:http');

const port = Number(process.env.PORT || 8080);
const upstream = new URL(process.env.ORDER_SERVICE_URL || 'http://localhost:3000');
const allowedOrigin = process.env.CORS_ORIGIN || 'http://localhost:3000';

const server = http.createServer((request, response) => {
	response.setHeader('Access-Control-Allow-Origin', allowedOrigin);
	response.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
	response.setHeader('Access-Control-Allow-Headers', 'Content-Type');

	if (request.method === 'OPTIONS') {
		response.writeHead(204);
		response.end();
		return;
	}

	if (!['/orders', '/health'].includes(new URL(request.url, 'http://gateway').pathname)) {
		response.writeHead(404, { 'content-type': 'application/json' });
		response.end(JSON.stringify({ error: 'Not found' }));
		return;
	}

	const proxyRequest = http.request({
		hostname: upstream.hostname,
		port: upstream.port || 80,
		path: request.url,
		method: request.method,
		headers: { ...request.headers, host: upstream.host }
	}, (proxyResponse) => {
		response.writeHead(proxyResponse.statusCode || 502, proxyResponse.headers);
		proxyResponse.pipe(response);
	});

	proxyRequest.on('error', (error) => {
		console.error(`Order service proxy error: ${error.message}`);
		if (!response.headersSent) response.writeHead(502, { 'content-type': 'application/json' });
		response.end(JSON.stringify({ error: 'Order service is unavailable' }));
	});
	request.pipe(proxyRequest);
});

server.listen(port, '0.0.0.0', () => console.log(`API gateway listening on port ${port}`));
