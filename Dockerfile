# Pixel Agents Web — dockerized build of the VS Code extension's webview as a
# standalone browser app. Served by Caddy at multica.not-a-camp.com/pixel.
#
# Stage 1 (this file): static build, serves mock data.
# Stage 2 (future): sidecar Node bridge that streams Multica WS events.

FROM node:22-alpine AS build
WORKDIR /src

# Build context is the whole repo so that webview-ui's imports of
# ../shared/assets/* resolve (see vite.config.ts).
COPY shared ./shared
COPY webview-ui ./webview-ui
WORKDIR /src/webview-ui
RUN npm ci --no-audit --no-fund
RUN npm run build

FROM caddy:2-alpine AS runtime
# Vite outputs to ../dist/webview relative to webview-ui/
COPY --from=build /src/dist/webview /srv
COPY Caddyfile /etc/caddy/Caddyfile
EXPOSE 3000
