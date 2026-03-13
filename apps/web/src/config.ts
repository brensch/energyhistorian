const runtimeConfig = window.__APP_CONFIG__;

export const appConfig = {
  apiBaseUrl: runtimeConfig?.apiBaseUrl ?? 'http://localhost:8090',
  workosClientId: runtimeConfig?.workosClientId ?? '',
  workosApiHostname: runtimeConfig?.workosApiHostname ?? '',
  enableDevAuth: runtimeConfig?.enableDevAuth ?? false,
  devUserId: runtimeConfig?.devUserId ?? 'dev-user',
  devUserEmail: runtimeConfig?.devUserEmail ?? 'dev@example.com',
  devUserName: runtimeConfig?.devUserName ?? 'Developer',
  devOrgId: runtimeConfig?.devOrgId ?? 'dev-org',
};
