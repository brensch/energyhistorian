import {
  AuthKitProvider,
  useAuth as useWorkosAuth,
} from '@workos-inc/authkit-react';
import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren,
} from 'react';

import { appConfig } from './config';
import type { AuthUser } from './types';

export interface AuthSession {
  ready: boolean;
  isAuthenticated: boolean;
  user: AuthUser | null;
  accessToken: string | null;
  authError: string | null;
  signIn: () => void;
  signOut: () => void;
  headers: Record<string, string>;
}

const DEV_USER: AuthUser = {
  id: appConfig.devUserId,
  email: appConfig.devUserEmail,
  name: appConfig.devUserName,
  org_id: appConfig.devOrgId,
  role: 'admin',
  permissions: [],
  session_id: 'dev-session',
  is_admin: true,
};

const AuthContext = createContext<AuthSession | null>(null);

function AuthContextProvider({
  children,
  value,
}: PropsWithChildren<{ value: AuthSession }>) {
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

function WorkosBoundary({ children }: PropsWithChildren) {
  const { user, signIn, signOut, isLoading, getAccessToken } = useWorkosAuth() as {
    user: {
      id: string;
      email?: string;
      firstName?: string;
      lastName?: string;
      organizationId?: string;
    } | null;
    signIn: () => Promise<void>;
    signOut: () => Promise<void>;
    isLoading: boolean;
    getAccessToken: () => Promise<string | null>;
  };
  const [accessToken, setAccessToken] = useState<string | null>(null);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }
    if (window.location.pathname === '/login' && !isLoading && !user) {
      void signIn();
    }
  }, [isLoading, signIn, user]);

  useEffect(() => {
    let cancelled = false;
    async function syncToken() {
      if (!user) {
        setAccessToken(null);
        return;
      }
      const token = await getAccessToken();
      if (!cancelled) {
        setAccessToken(token);
      }
    }
    void syncToken();
    return () => {
      cancelled = true;
    };
  }, [getAccessToken, user]);

  const session = useMemo<AuthSession>(
    () => ({
      ready: !isLoading,
      isAuthenticated: Boolean(user),
      user: user
        ? {
            id: user.id,
            email: user.email ?? '',
            name:
              [user.firstName, user.lastName].filter(Boolean).join(' ') ||
              user.email ||
              'Unknown user',
            org_id: user.organizationId ?? '',
            role: '',
            permissions: [],
            session_id: '',
            is_admin: false,
          }
        : null,
      accessToken,
      authError: null,
      signIn: () => {
        void signIn();
      },
      signOut: () => {
        void signOut();
      },
      headers: accessToken
        ? { Authorization: `Bearer ${accessToken}` }
        : ({} as Record<string, string>),
    }),
    [accessToken, isLoading, signIn, signOut, user],
  );

  return <AuthContextProvider value={session}>{children}</AuthContextProvider>;
}

function DevBoundary({ children }: PropsWithChildren) {
  const session = useMemo<AuthSession>(
    () => ({
      ready: true,
      isAuthenticated: true,
      user: DEV_USER,
      accessToken: null,
      authError: null,
      signIn: () => undefined,
      signOut: () => undefined,
      headers: {
        'X-Dev-User-Id': DEV_USER.id,
        'X-Dev-User-Email': DEV_USER.email,
        'X-Dev-User-Name': DEV_USER.name,
        'X-Dev-Org-Id': DEV_USER.org_id,
        'X-Dev-Role': DEV_USER.role,
      },
    }),
    [],
  );

  return <AuthContextProvider value={session}>{children}</AuthContextProvider>;
}

export function AppAuthProvider({ children }: PropsWithChildren) {
  if (appConfig.enableDevAuth) {
    return <DevBoundary>{children}</DevBoundary>;
  }

  if (!appConfig.workosClientId) {
    const session: AuthSession = {
      ready: true,
      isAuthenticated: false,
      user: null,
      accessToken: null,
      authError:
        'WorkOS client ID is missing. Set VITE_WORKOS_CLIENT_ID for Vite dev or configure workosClientId in public/config.js.',
      signIn: () => undefined,
      signOut: () => undefined,
      headers: {},
    };
    return <AuthContextProvider value={session}>{children}</AuthContextProvider>;
  }

  return (
    <AuthKitProvider
      clientId={appConfig.workosClientId}
      apiHostname={appConfig.workosApiHostname || undefined}
      redirectUri={`${window.location.origin}/callback`}
    >
      <WorkosBoundary>{children}</WorkosBoundary>
    </AuthKitProvider>
  );
}

export function useAppAuth() {
  const session = useContext(AuthContext);
  if (!session) {
    throw new Error('useAppAuth must be used within AppAuthProvider');
  }
  return session;
}
