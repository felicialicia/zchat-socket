FROM node:20-slim

WORKDIR /app

COPY package.json ./
RUN npm install

COPY index.js ./

ENV PORT=3003
EXPOSE 3003

CMD ["node", "index.js"]
