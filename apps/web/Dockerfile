FROM node:24-bookworm-slim AS builder
WORKDIR /app

COPY apps/web/package.json ./package.json
RUN npm install

COPY apps/web ./
RUN npm run build

FROM nginx:1.29-alpine AS runtime

RUN apk add --no-cache gettext

COPY apps/web/nginx/default.conf /etc/nginx/conf.d/default.conf
COPY apps/web/nginx/entrypoint.sh /entrypoint.sh
COPY --from=builder /app/dist /usr/share/nginx/html
COPY apps/web/public/config.template.js /usr/share/nginx/html/config.template.js

RUN chmod +x /entrypoint.sh

EXPOSE 8080

CMD ["/entrypoint.sh"]
