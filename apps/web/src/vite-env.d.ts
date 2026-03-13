/// <reference types="vite/client" />

interface AppConfig {
  apiBaseUrl: string;
  workosClientId: string;
  workosApiHostname: string;
  enableDevAuth: boolean;
  devUserId: string;
  devUserEmail: string;
  devUserName: string;
  devOrgId: string;
}

interface Window {
  __APP_CONFIG__?: AppConfig;
}
