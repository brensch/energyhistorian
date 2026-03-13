#!/bin/sh
set -eu

: "${API_BASE_URL:=http://localhost:8090}"
: "${WORKOS_CLIENT_ID:=}"
: "${WORKOS_API_HOSTNAME:=}"
: "${ENABLE_DEV_AUTH:=false}"
: "${DEV_USER_ID:=dev-user}"
: "${DEV_USER_EMAIL:=dev@example.com}"
: "${DEV_USER_NAME:=Developer}"
: "${DEV_ORG_ID:=dev-org}"

envsubst \
  '$API_BASE_URL $WORKOS_CLIENT_ID $WORKOS_API_HOSTNAME $ENABLE_DEV_AUTH $DEV_USER_ID $DEV_USER_EMAIL $DEV_USER_NAME $DEV_ORG_ID' \
  < /usr/share/nginx/html/config.template.js \
  > /usr/share/nginx/html/config.js

exec nginx -g 'daemon off;'
