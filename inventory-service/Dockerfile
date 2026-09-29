FROM node:20-alpine

ENV NODE_ENV=production

WORKDIR /app

COPY package*.json ./
RUN if [ -s package.json ]; then npm install --omit=dev; fi

COPY . .

EXPOSE 3001

USER node

CMD ["node", "index.js"]
