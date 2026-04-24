# Pixel Agents Bridge — watches Claude JSONL transcripts and broadcasts over WS.
FROM node:22-alpine
WORKDIR /app
COPY bridge/package.json bridge/package-lock.json* ./bridge/
WORKDIR /app/bridge
RUN npm install --no-audit --no-fund --omit=dev && \
    npm install tsx typescript --no-save --no-audit --no-fund
COPY bridge/src ./src
COPY bridge/tsconfig.json ./
ENV NODE_ENV=production
EXPOSE 8787
CMD ["npx", "tsx", "src/server.ts"]
