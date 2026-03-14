const runtimeConfig = window.__APP_CONFIG__;
const env = import.meta.env;

function pickConfigValue(...values: Array<string | undefined>) {
  return values.find((value) => typeof value === 'string' && value.length > 0) ?? '';
}

function parseBoolean(value: boolean | string | undefined, fallback: boolean) {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'string') {
    return value === 'true';
  }
  return fallback;
}

export const appConfig = {
  apiBaseUrl: pickConfigValue(runtimeConfig?.apiBaseUrl, env.VITE_API_BASE_URL) || 'http://localhost:8090',
  workosClientId: pickConfigValue(runtimeConfig?.workosClientId, env.VITE_WORKOS_CLIENT_ID),
  workosApiHostname: pickConfigValue(
    runtimeConfig?.workosApiHostname,
    env.VITE_WORKOS_API_HOSTNAME,
  ),
  enableDevAuth: parseBoolean(
    runtimeConfig?.enableDevAuth ?? env.VITE_ENABLE_DEV_AUTH,
    false,
  ),
  devUserId: pickConfigValue(runtimeConfig?.devUserId, env.VITE_DEV_USER_ID) || 'dev-user',
  devUserEmail:
    pickConfigValue(runtimeConfig?.devUserEmail, env.VITE_DEV_USER_EMAIL) || 'dev@example.com',
  devUserName:
    pickConfigValue(runtimeConfig?.devUserName, env.VITE_DEV_USER_NAME) || 'Developer',
  devOrgId: pickConfigValue(runtimeConfig?.devOrgId, env.VITE_DEV_ORG_ID) || 'dev-org',
};
