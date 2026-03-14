/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_WORKOS_CLIENT_ID?: string;
  readonly VITE_WORKOS_API_HOSTNAME?: string;
  readonly VITE_ENABLE_DEV_AUTH?: string;
  readonly VITE_DEV_USER_ID?: string;
  readonly VITE_DEV_USER_EMAIL?: string;
  readonly VITE_DEV_USER_NAME?: string;
  readonly VITE_DEV_ORG_ID?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

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
