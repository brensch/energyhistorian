# web

Chat-style SPA for the `ai-api` service.

## Stack

- React + Vite
- WorkOS AuthKit for browser auth
- Plotly for client-side charts

## Why the graph is rendered in the browser

The backend already returns:

- chart intent
- query preview rows
- the SQL and answer context

That is a better API boundary than returning pre-rendered Plotly HTML or image blobs:

- the backend stays presentation-agnostic
- the browser keeps full chart interactivity
- the same API can support web, mobile, and embedded clients
- chart rendering cost stays off the API service

So the API should return structured chart payloads, not a finished graph artifact. The frontend turns that payload into Plotly traces.

## Local dev

```bash
cd apps/web
npm install
npm run dev
```

For local dev, `public/config.js` points at `http://localhost:8090` and defaults `enableDevAuth` to `false` so browser auth goes through WorkOS. Re-enable dev auth only if you explicitly want to bypass login during UI work.

## Container

The Docker image builds the SPA and serves it with nginx on port `8080`.

Runtime env vars:

- `API_BASE_URL`
- `WORKOS_CLIENT_ID`
- `WORKOS_API_HOSTNAME`
- `ENABLE_DEV_AUTH`
- `DEV_USER_ID`
- `DEV_USER_EMAIL`
- `DEV_USER_NAME`
- `DEV_ORG_ID`
