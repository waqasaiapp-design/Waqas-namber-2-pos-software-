import express from 'express';
import type { Request, Response, NextFunction, Application } from 'express';
import path from 'path';
import fs from 'fs';
import os from 'os';
import crypto from 'crypto';
import net from 'net';
import tls from 'tls';
import { DEFAULT_STAFF_PERMISSIONS } from './src/types/pos.ts';
import type {
  Product,
  SaleTransaction,
  PurchaseTransaction,
  Customer,
  CustomerLedgerEntry,
  Supplier,
  SaleReturn,
  HeldBill,
  StoreBranding,
  DeviceInfo,
  PairingSession,
  SyncEvent,
  UserProfile,
  ShopAccount,
  SubscriptionPaymentRecord,
  SubscriptionRenewalRecord,
  AuthUser,
  UserRole,
  AuditLogEntry,
  DefaultSubscriptionPricing,
  StockTransaction,
  StockTransactionType,
  PurchaseReturn,
  ExpenseEntry,
  StaffMember,
  StaffModulePermissions,
  SupplierLedgerEntry,
  PrinterSettings,
  CategoryInfo,
  DailyHisabRow,
  ReceiptSettings,
} from './src/types/pos.ts';
import {
  SCREENSHOT_CATEGORIES,
  INITIAL_SUPPLIERS,
  SEED_CATALOG_PRODUCTS,
} from './src/data/defaultProducts.ts';
import {
  applyPurchaseDeleteAllToState,
  applyPurchaseDeleteToState,
  applyPurchaseSaveToState,
  formatStockAuditTimestamp,
} from './src/utils/purchaseHelpers.ts';
import {
  STOCK_ALREADY_ZERO_MESSAGE,
  buildInsufficientStockMessage,
  findLiveProduct,
  reconcileNegativeStockFromHistory,
  roundStockQty,
  validatePurchaseReturnStock,
  validateSaleItemsStock,
} from './src/utils/stockValidation.ts';
import { APP_VERSION, APP_BUILD_ID, APP_BUILD_DATE, AUTH_POLICY_VERSION } from './src/version.ts';

export const isServerlessRuntime = Boolean(
  process.env.VERCEL ||
    process.env.VERCEL_ENV ||
    process.env.AWS_LAMBDA_FUNCTION_NAME ||
    process.env.WAQAS_SERVERLESS === '1'
);

const SEED_DATA_DIR = path.resolve(process.cwd(), 'data');

function isDirectoryWritable(dirPath: string): boolean {
  try {
    fs.mkdirSync(dirPath, { recursive: true });
    const probeFile = path.join(dirPath, `.write_probe_${process.pid}_${Date.now()}`);
    fs.writeFileSync(probeFile, 'ok');
    fs.unlinkSync(probeFile);
    return true;
  } catch {
    return false;
  }
}

function copySeedDirectoryIfMissing(srcDir: string, destDir: string) {
  if (!fs.existsSync(srcDir)) return;
  try {
    fs.mkdirSync(destDir, { recursive: true });
    const entries = fs.readdirSync(srcDir, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.name === 'backups' || entry.name.startsWith('.write_probe_')) continue;
      const srcPath = path.join(srcDir, entry.name);
      const destPath = path.join(destDir, entry.name);
      if (entry.isDirectory()) {
        copySeedDirectoryIfMissing(srcPath, destPath);
      } else if (entry.isFile() && !fs.existsSync(destPath)) {
        fs.copyFileSync(srcPath, destPath);
      }
    }
  } catch (err) {
    console.warn('[Server] Seed directory sync warning:', err);
  }
}

function resolveDataDir(): { dataDir: string; storageMode: 'local-fs' | 'serverless-tmp' } {
  if (process.env.WAQAS_DATA_DIR) {
    const customDir = path.resolve(process.env.WAQAS_DATA_DIR);
    if (isDirectoryWritable(customDir)) {
      return { dataDir: customDir, storageMode: isServerlessRuntime ? 'serverless-tmp' : 'local-fs' };
    }
  }

  if (!isServerlessRuntime && isDirectoryWritable(SEED_DATA_DIR)) {
    return { dataDir: SEED_DATA_DIR, storageMode: 'local-fs' };
  }

  const tmpDataDir = path.join(os.tmpdir(), 'waqas-pos-data');
  fs.mkdirSync(tmpDataDir, { recursive: true });
  copySeedDirectoryIfMissing(SEED_DATA_DIR, tmpDataDir);
  return { dataDir: tmpDataDir, storageMode: 'serverless-tmp' };
}

const resolvedStorage = resolveDataDir();
export const DATA_DIR = resolvedStorage.dataDir;
export const SHOPS_DIR = path.join(DATA_DIR, 'shops');
fs.mkdirSync(SHOPS_DIR, { recursive: true });

const STORE_DB_FILE = path.join(DATA_DIR, 'store_db.json');
const DEVICES_DB_FILE = path.join(DATA_DIR, 'devices_db.json');
const SUBSCRIPTIONS_DB_FILE = path.join(DATA_DIR, 'subscriptions_db.json');
const AUTH_DB_FILE = path.join(DATA_DIR, 'auth_db.json');
const AUDIT_LOGS_FILE = path.join(DATA_DIR, 'audit_logs.json');
const PAIRING_SESSIONS_FILE = path.join(DATA_DIR, 'pairing_sessions.json');
const SUPER_ADMIN_POS_SHOP_ID = 'SHOP-SA-POS';

// -------------------------------------------------------------
// OPTIONAL SHARED KV STORE (Vercel KV / Upstash Redis REST API)
// Enables multi-container persistence on Vercel Serverless Functions
// -------------------------------------------------------------
function getKvConfig(): { url: string; token: string; enabled: boolean } {
  const url = (
    process.env.KV_REST_API_URL ||
    process.env.UPSTASH_REDIS_REST_URL ||
    ''
  ).replace(/\/+$/, '');
  const token =
    process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || '';
  return { url, token, enabled: Boolean(url && token) };
}

const initialKvCfg = getKvConfig();
const isSharedKvEnabled = initialKvCfg.enabled;

interface KvGetResult<T> {
  ok: boolean;
  found: boolean;
  value: T | null;
}

async function kvGetResult<T>(key: string): Promise<KvGetResult<T>> {
  const kv = getKvConfig();
  if (!kv.enabled) return { ok: false, found: false, value: null };
  try {
    const res = await fetch(`${kv.url}/get/${encodeURIComponent(key)}`, {
      method: 'GET',
      headers: { Authorization: `Bearer ${kv.token}` },
    });
    if (!res.ok) return { ok: false, found: false, value: null };
    const body = (await res.json()) as { result?: unknown };
    if (body?.result == null) return { ok: true, found: false, value: null };
    if (typeof body.result === 'string') {
      return { ok: true, found: true, value: JSON.parse(body.result) as T };
    }
    return { ok: true, found: true, value: body.result as T };
  } catch {
    return { ok: false, found: false, value: null };
  }
}

async function kvGetJson<T>(key: string): Promise<T | null> {
  const res = await kvGetResult<T>(key);
  return res.found ? res.value : null;
}

const pendingKvWrites = new Set<Promise<void>>();

export async function flushPendingKvWrites(): Promise<void> {
  if (pendingKvWrites.size === 0) return;
  await Promise.allSettled(Array.from(pendingKvWrites));
}

function trackKvWrite(task: Promise<void>): Promise<void> {
  pendingKvWrites.add(task);
  task.finally(() => {
    pendingKvWrites.delete(task);
  });
  return task;
}

async function kvSetJson(key: string, value: unknown, ttlSeconds?: number): Promise<void> {
  const kv = getKvConfig();
  if (!kv.enabled) return;
  const run = (async () => {
    try {
      const serialized = JSON.stringify(value);
      const endpoint = ttlSeconds
        ? `${kv.url}/set/${encodeURIComponent(key)}?EX=${Math.max(1, Math.floor(ttlSeconds))}`
        : `${kv.url}/set/${encodeURIComponent(key)}`;
      await fetch(endpoint, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${kv.token}`,
          'Content-Type': 'text/plain; charset=utf-8',
        },
        body: serialized,
      });
    } catch {}
  })();
  return trackKvWrite(run);
}

async function kvSetNxJson(key: string, value: unknown): Promise<void> {
  const kv = getKvConfig();
  if (!kv.enabled) return;
  const run = (async () => {
    try {
      const serialized = JSON.stringify(value);
      const res = await fetch(`${kv.url}/set/${encodeURIComponent(key)}?NX=true`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${kv.token}`,
          'Content-Type': 'text/plain; charset=utf-8',
        },
        body: serialized,
      });
      if (!res.ok) {
        await fetch(`${kv.url}/set/${encodeURIComponent(key)}`, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${kv.token}`,
            'Content-Type': 'text/plain; charset=utf-8',
          },
          body: serialized,
        });
      }
    } catch {}
  })();
  return trackKvWrite(run);
}

async function kvDel(key: string): Promise<void> {
  const kv = getKvConfig();
  if (!kv.enabled) return;
  const run = (async () => {
    try {
      await fetch(`${kv.url}/del/${encodeURIComponent(key)}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${kv.token}` },
      });
    } catch {}
  })();
  return trackKvWrite(run);
}

// -------------------------------------------------------------
// HMAC-SIGNED SESSION TOKENS (Survives across serverless cold/warm containers)
// -------------------------------------------------------------
const EPHEMERAL_SESSION_SECRET = crypto.randomBytes(32).toString('hex');
let sharedKvSessionSecret: string | null = null;

function getExplicitEnvSessionSecret(): string {
  return (
    process.env.SESSION_SECRET ||
    process.env.WAQAS_SESSION_SECRET ||
    process.env.AUTH_SECRET ||
    ''
  ).trim();
}

function hasConfiguredSessionSecret(): boolean {
  return Boolean(getExplicitEnvSessionSecret() || (isSharedKvEnabled && sharedKvSessionSecret));
}

function getSessionSigningSecret(): string {
  return (
    getExplicitEnvSessionSecret() ||
    sharedKvSessionSecret ||
    EPHEMERAL_SESSION_SECRET
  );
}

function base64UrlEncode(input: string | Buffer): string {
  return Buffer.from(input)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function base64UrlDecode(input: string): string {
  const normalized = input.replace(/-/g, '+').replace(/_/g, '/');
  const padLen = (4 - (normalized.length % 4)) % 4;
  return Buffer.from(normalized + '='.repeat(padLen), 'base64').toString('utf8');
}

function createSignedSessionToken(prefix: 'SA' | 'SO' | 'STF', session: Omit<AuthSessionData, 'token'>): string {
  const payloadObj = {
    jti: crypto.randomBytes(12).toString('hex'),
    uid: session.userId,
    role: session.role,
    shopId: session.shopId,
    staffId: session.staffId,
    email: session.email,
    gmail: session.gmail,
    username: session.username,
    fullName: session.fullName,
    iat: session.createdAt,
    exp: session.expiresAt,
    pv: session.policyVersion || AUTH_POLICY_VERSION,
  };
  const payloadB64 = base64UrlEncode(JSON.stringify(payloadObj));
  const sig = base64UrlEncode(
    crypto.createHmac('sha256', getSessionSigningSecret()).update(`${prefix}.${payloadB64}`).digest()
  );
  return `${prefix}.${payloadB64}.${sig}`;
}

function verifySignedSessionToken(token: string): (AuthSessionData & { jti?: string }) | null {
  const parts = token.split('.');
  if (parts.length !== 3) return null;
  const [prefix, payloadB64, sigB64] = parts;
  if (prefix !== 'SA' && prefix !== 'SO' && prefix !== 'STF') return null;

  try {
    const expectedSig = base64UrlEncode(
      crypto.createHmac('sha256', getSessionSigningSecret()).update(`${prefix}.${payloadB64}`).digest()
    );
    const a = Buffer.from(sigB64);
    const b = Buffer.from(expectedSig);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
      return null;
    }
    const parsed = JSON.parse(base64UrlDecode(payloadB64));
    if (!parsed || !parsed.uid || !parsed.role) return null;
    if (parsed.pv !== AUTH_POLICY_VERSION) return null;
    if (!parsed.exp || parsed.exp <= Date.now()) return null;

    if (parsed.jti && authDb.revokedTokens?.[parsed.jti]) {
      return null;
    }
    const revokedBefore = authDb.revokedBefore?.[parsed.uid] || 0;
    if (parsed.iat && parsed.iat < revokedBefore) {
      return null;
    }
    if (parsed.shopId) {
      const shopRevokedBefore = authDb.revokedBefore?.[`shop:${parsed.shopId}`] || 0;
      if (parsed.iat && parsed.iat < shopRevokedBefore) {
        return null;
      }
    }

    return {
      token,
      jti: parsed.jti,
      userId: parsed.uid,
      email: parsed.email,
      gmail: parsed.gmail,
      username: parsed.username,
      fullName: parsed.fullName,
      role: parsed.role,
      shopId: parsed.shopId,
      staffId: parsed.staffId,
      createdAt: parsed.iat || Date.now(),
      expiresAt: parsed.exp,
      policyVersion: parsed.pv,
    };
  } catch {
    return null;
  }
}

function revokeTokenInDb(token: string, fallbackExp?: number) {
  const parts = token.split('.');
  if (parts.length === 3) {
    try {
      const parsed = JSON.parse(base64UrlDecode(parts[1]));
      if (parsed?.jti) {
        authDb.revokedTokens = authDb.revokedTokens || {};
        authDb.revokedTokens[parsed.jti] = parsed.exp || fallbackExp || Date.now() + 30 * 24 * 60 * 60 * 1000;
      }
    } catch {}
  }
}

export interface StoredSuperAdmin {
  id: string;
  gmail: string;
  googleSub?: string;
  mobile?: string;
  fullName: string;
  status: 'active';
  verifiedGmail: boolean;
  verifiedMobile?: boolean;
  passwordHash: string;
  passwordSalt: string;
  createdAt: string;
  lastLoginAt?: string;
}

export interface PendingOAuthState {
  purpose: 'login' | 'provision';
  redirectUri: string;
  createdAt: number;
  expiresAt: number;
}

export interface PendingGoogleAssertion {
  gmail: string;
  googleSub: string;
  fullName: string;
  purpose: 'login' | 'provision';
  createdAt: number;
  expiresAt: number;
  used: boolean;
  passwordRetries: number;
}

export interface PendingOtpSession {
  purpose: 'setup' | 'recovery';
  gmail?: string;
  mobile?: string;
  fullName?: string;
  passwordSaltTemp?: string;
  passwordHashTemp?: string;
  emailOtpHash?: string;
  smsOtpHash?: string;
  channel?: 'gmail' | 'mobile';
  expiresAt: number;
  attempts: number;
  createdAt: number;
}

export interface StoredUser {
  id: string;
  email?: string;
  username?: string;
  secondaryEmails?: string[];
  fullName: string;
  phone?: string;
  whatsapp?: string;
  role: UserRole;
  shopId?: string;
  passwordHash: string;
  passwordSalt: string;
  mustChangePasswordOnFirstLogin?: boolean;
  createdAt: string;
}

export interface AuthSessionData {
  token: string;
  userId: string;
  email?: string;
  gmail?: string;
  username?: string;
  fullName?: string;
  role: UserRole;
  shopId?: string;
  staffId?: string;
  permissions?: StaffModulePermissions;
  createdAt: number;
  expiresAt: number;
  ip?: string;
  policyVersion?: string;
}

export interface AuthDb {
  superAdmin: StoredSuperAdmin | null;
  superAdminSetupLocked?: boolean;
  users: StoredUser[];
  sessions: Record<string, AuthSessionData>;
  revokedTokens?: Record<string, number>;
  revokedBefore?: Record<string, number>;
  pendingOtps: Record<string, PendingOtpSession>;
  failedAttempts: Record<string, { count: number; lastAttempt: number }>;
}

function normalizeMobile(input: string): string {
  const digits = String(input || '').replace(/[^0-9]/g, '');
  if (!digits) return '';
  if (digits.startsWith('0092') && digits.length >= 12) {
    return '0' + digits.slice(4);
  }
  if (digits.startsWith('92') && digits.length === 12) {
    return '0' + digits.slice(2);
  }
  if (digits.length === 10 && digits.startsWith('3')) {
    return '0' + digits;
  }
  return digits;
}

function hashPassword(password: string, salt: string): string {
  return 'scrypt:' + crypto.scryptSync(password, salt, 64, { N: 16384, r: 8, p: 1 }).toString('hex');
}

function verifyPasswordRecord(password: string, salt: string, storedHash: string): boolean {
  if (!password || !salt || !storedHash) return false;
  if (storedHash.startsWith('scrypt:')) {
    const expectedHex = storedHash.slice(7);
    const derived = crypto.scryptSync(password, salt, 64, { N: 16384, r: 8, p: 1 });
    const expectedBuf = Buffer.from(expectedHex, 'hex');
    if (derived.length !== expectedBuf.length) return false;
    return crypto.timingSafeEqual(derived, expectedBuf);
  }
  const calc100k = crypto.pbkdf2Sync(password, salt, 100000, 32, 'sha256').toString('hex');
  if (calc100k === storedHash) return true;
  const calc1k = crypto.pbkdf2Sync(password, salt, 1000, 32, 'sha256').toString('hex');
  return calc1k === storedHash;
}

function verifyPassword(password: string, user: StoredUser): boolean {
  if (!user || !user.passwordSalt || !user.passwordHash) return false;
  const ok = verifyPasswordRecord(password, user.passwordSalt, user.passwordHash);
  if (ok && !user.passwordHash.startsWith('scrypt:')) {
    user.passwordHash = hashPassword(password, user.passwordSalt);
    persistAuthDb();
  }
  return ok;
}

function loadInitialAuditLogs(): AuditLogEntry[] {
  if (fs.existsSync(AUDIT_LOGS_FILE)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(AUDIT_LOGS_FILE, 'utf-8'));
      if (Array.isArray(parsed)) {
        return parsed.map((entry: any) => ({
          ...entry,
          actorRole: entry.actorRole === 'MASTER_ADMIN' ? 'SUPER_ADMIN' : entry.actorRole,
        }));
      }
    } catch (e) {
      console.warn('Failed to parse audit_logs.json, initializing', e);
    }
  }
  const initialLogs: AuditLogEntry[] = [
    {
      id: `AUD-${Date.now()}-001`,
      timestamp: new Date().toISOString(),
      action: 'SYSTEM_INITIALIZED',
      actorEmail: 'system@waqaspos',
      actorRole: 'SUPER_ADMIN',
      details: 'WAQAS POS SOFTWARE centralized multi-shop authentication initialized.',
    },
  ];
  try {
    fs.writeFileSync(AUDIT_LOGS_FILE, JSON.stringify(initialLogs, null, 2));
  } catch (err) {
    console.error('Error saving audit_logs.json', err);
  }
  return initialLogs;
}

let auditLogs: AuditLogEntry[] = loadInitialAuditLogs();

function persistAuditLogs() {
  const sliced = auditLogs.slice(0, 500);
  try {
    fs.writeFileSync(AUDIT_LOGS_FILE, JSON.stringify(sliced, null, 2));
  } catch (e) {
    console.error('Error persisting audit_logs.json', e);
  }
  if (isSharedKvEnabled) {
    kvSetJson('waqas:audit_logs', sliced).catch(() => {});
  }
}

function recordAuditLog(
  action: string,
  actorEmail: string,
  actorRole: UserRole,
  details: string,
  shopId?: string,
  ipAddress?: string
) {
  const entry: AuditLogEntry = {
    id: `AUD-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`,
    timestamp: new Date().toISOString(),
    action,
    actorEmail,
    actorRole,
    details,
    shopId,
    ipAddress,
  };
  auditLogs.unshift(entry);
  persistAuditLogs();
}

const hasEnvSuperAdminOverride = Boolean(
  process.env.SUPER_ADMIN_PASSWORD_SALT && process.env.SUPER_ADMIN_PASSWORD_HASH
);
const FIXED_SUPER_ADMIN_EMAIL = String(process.env.SUPER_ADMIN_EMAIL || 'muhammedsaad255251@gmail.com')
  .trim()
  .toLowerCase();
const FIXED_SUPER_ADMIN_NAME = String(process.env.SUPER_ADMIN_NAME || 'Muhammad Waqas').trim();
const FIXED_SUPER_ADMIN_SALT =
  process.env.SUPER_ADMIN_PASSWORD_SALT || 'b14561a263ef0e8ff0ea689fd55dac48';
const FIXED_SUPER_ADMIN_HASH =
  process.env.SUPER_ADMIN_PASSWORD_HASH ||
  'scrypt:3d8886d2b0130d72bcc7df325617b5f9b65148ed0e5823b8f5e041da8d63e77856a89d05a586f61508df19c110e92f378cb109ca541d0f87b3be8487a4593dfb';

function createFixedSuperAdminRecord(): StoredSuperAdmin {
  return {
    id: 'SUPER-ADMIN-01',
    gmail: FIXED_SUPER_ADMIN_EMAIL,
    fullName: FIXED_SUPER_ADMIN_NAME,
    status: 'active',
    verifiedGmail: true,
    passwordSalt: FIXED_SUPER_ADMIN_SALT,
    passwordHash: FIXED_SUPER_ADMIN_HASH,
    createdAt: '2026-01-01T00:00:00.000Z',
  };
}

function normalizeLoadedAuthDb(raw: any): AuthDb {
  const users: StoredUser[] = Array.isArray(raw?.users)
    ? raw.users
        .filter((u: any) => u && u.role !== 'MASTER_ADMIN')
        .map((u: any) => {
          if (u.id === 'USR-SHOP-001' && !u.username) {
            return { ...u, username: 'waqasmart' };
          }
          return u;
        })
    : [];
  const sessions: Record<string, AuthSessionData> = {};
  if (raw?.sessions && typeof raw.sessions === 'object') {
    for (const [tok, sess] of Object.entries<any>(raw.sessions)) {
      if (sess && sess.role && sess.role !== 'MASTER_ADMIN' && sess.policyVersion === AUTH_POLICY_VERSION) {
        sessions[tok] = sess;
      }
    }
  }
  const existingSa: StoredSuperAdmin | null =
    !hasEnvSuperAdminOverride &&
    raw?.superAdmin &&
    typeof raw.superAdmin.gmail === 'string' &&
    raw.superAdmin.gmail.trim().toLowerCase() === FIXED_SUPER_ADMIN_EMAIL &&
    raw.superAdmin.passwordHash &&
    raw.superAdmin.passwordSalt
      ? {
          ...raw.superAdmin,
          gmail: FIXED_SUPER_ADMIN_EMAIL,
          status: 'active',
          verifiedGmail: true,
        }
      : null;

  return {
    superAdmin: existingSa || createFixedSuperAdminRecord(),
    superAdminSetupLocked: true,
    users,
    sessions,
    revokedTokens: raw?.revokedTokens && typeof raw.revokedTokens === 'object' ? raw.revokedTokens : {},
    revokedBefore: raw?.revokedBefore && typeof raw.revokedBefore === 'object' ? raw.revokedBefore : {},
    pendingOtps: {},
    failedAttempts: raw?.failedAttempts && typeof raw.failedAttempts === 'object' ? raw.failedAttempts : {},
  };
}

function loadInitialAuthDb(): AuthDb {
  if (fs.existsSync(AUTH_DB_FILE)) {
    try {
      const raw: any = JSON.parse(fs.readFileSync(AUTH_DB_FILE, 'utf-8'));
      const loaded = normalizeLoadedAuthDb(raw);
      try {
        fs.writeFileSync(AUTH_DB_FILE, JSON.stringify(loaded, null, 2));
      } catch {}
      return loaded;
    } catch (e) {
      console.warn('Failed to parse auth_db.json, initializing', e);
    }
  }

  const shopOwnerSalt = crypto.randomBytes(16).toString('hex');
  const shopOwnerHash = hashPassword('ShopOwner@123', shopOwnerSalt);

  const initialAuth: AuthDb = {
    superAdmin: createFixedSuperAdminRecord(),
    superAdminSetupLocked: true,
    users: [
      {
        id: 'USR-SHOP-001',
        email: 'waqasmart.pos@gmail.com',
        username: 'waqasmart',
        secondaryEmails: [],
        fullName: 'Muhammad Waqas',
        phone: '03429634133',
        whatsapp: '03228570782',
        role: 'SHOP_OWNER',
        shopId: 'SHOP-WM-001',
        passwordSalt: shopOwnerSalt,
        passwordHash: shopOwnerHash,
        mustChangePasswordOnFirstLogin: false,
        createdAt: '2026-01-01T00:00:00.000Z',
      },
    ],
    sessions: {},
    revokedTokens: {},
    revokedBefore: {},
    pendingOtps: {},
    failedAttempts: {},
  };

  try {
    fs.writeFileSync(AUTH_DB_FILE, JSON.stringify(initialAuth, null, 2));
  } catch (err) {
    console.error('Error creating default auth_db.json', err);
  }

  return initialAuth;
}

let authDb: AuthDb = loadInitialAuthDb();

function pruneExpiredAuthState() {
  const now = Date.now();
  if (authDb.sessions && typeof authDb.sessions === 'object') {
    for (const [tok, sess] of Object.entries(authDb.sessions)) {
      if (!sess || sess.expiresAt <= now || sess.policyVersion !== AUTH_POLICY_VERSION) {
        delete authDb.sessions[tok];
      }
    }
  }
  if (authDb.revokedTokens && typeof authDb.revokedTokens === 'object') {
    for (const [jti, exp] of Object.entries(authDb.revokedTokens)) {
      if (typeof exp !== 'number' || exp <= now) {
        delete authDb.revokedTokens[jti];
      }
    }
  }
  if (authDb.failedAttempts && typeof authDb.failedAttempts === 'object') {
    for (const [key, rec] of Object.entries(authDb.failedAttempts)) {
      if (rec && rec.lastAttempt < now - 60 * 60 * 1000) {
        delete authDb.failedAttempts[key];
      }
    }
  }
}

function persistAuthDb() {
  pruneExpiredAuthState();
  try {
    const tmpFile = `${AUTH_DB_FILE}.tmp`;
    fs.writeFileSync(tmpFile, JSON.stringify(authDb, null, 2));
    fs.renameSync(tmpFile, AUTH_DB_FILE);
  } catch (e) {
    console.error('Error persisting auth_db.json', e);
  }
  if (isSharedKvEnabled) {
    kvSetJson('waqas:auth_db', authDb).catch(() => {});
  }
}

interface SubscriptionsData {
  shops: ShopAccount[];
  payments: SubscriptionPaymentRecord[];
  renewals: SubscriptionRenewalRecord[];
  defaultPricing?: DefaultSubscriptionPricing;
}

function loadInitialSubscriptions(): SubscriptionsData {
  if (fs.existsSync(SUBSCRIPTIONS_DB_FILE)) {
    try {
      return JSON.parse(fs.readFileSync(SUBSCRIPTIONS_DB_FILE, 'utf-8'));
    } catch (e) {
      console.warn('Failed to parse subscriptions_db.json, using defaults', e);
    }
  }

  const defaultSubs: SubscriptionsData = {
    shops: [
      {
        id: 'SHOP-WM-001',
        shopName: 'WAQAS MART (Main Branch)',
        ownerName: 'Muhammad Waqas',
        email: 'waqasmart.pos@gmail.com',
        phone: '03429634133',
        whatsapp: '03228570782',
        address: 'Main Commercial Market, Pakistan',
        status: 'active',
        plan: '12_months',
        startDate: '2026-01-01',
        expiryDate: '2027-01-01',
        pricing: {
          oneMonth: 1500,
          threeMonths: 4000,
          sixMonths: 7500,
          twelveMonths: 14000,
          customPricePerMonth: 1500,
          lastUpdated: '2026-10-24',
        },
        initialPayment: 14000,
        remainingBalance: 0,
        totalPaid: 14000,
        totalIncome: 14000,
        renewalsCount: 1,
        mustChangePasswordOnFirstLogin: false,
        createdAt: '2026-01-01',
      },
    ],
    payments: [
      {
        id: 'PAY-1001',
        receiptNumber: 'RCP-9821',
        shopId: 'SHOP-WM-001',
        shopName: 'WAQAS MART (Main Branch)',
        ownerName: 'Muhammad Waqas',
        ownerEmail: 'waqasmart.pos@gmail.com',
        plan: '12_months',
        durationLabel: '12 Months (1 Year)',
        totalAmount: 14000,
        discount: 0,
        finalAmount: 14000,
        amountPaid: 14000,
        remainingBalance: 0,
        paymentMethod: 'Bank Transfer',
        paymentDate: '2026-01-01',
        transactionReference: 'TRX-PKR-00192',
        confirmedBy: 'Super Admin',
        status: 'Paid',
        createdAt: '2026-01-01T10:00:00Z',
      },
    ],
    renewals: [
      {
        id: 'REN-1001',
        shopId: 'SHOP-WM-001',
        shopName: 'WAQAS MART (Main Branch)',
        plan: '12_months',
        durationDays: 365,
        previousExpiry: '2026-01-01',
        newExpiry: '2027-01-01',
        paymentReceiptNumber: 'RCP-9821',
        amount: 14000,
        renewedAt: '2026-01-01',
        confirmedBy: 'Super Admin',
      },
    ],
  };

  try {
    fs.writeFileSync(SUBSCRIPTIONS_DB_FILE, JSON.stringify(defaultSubs, null, 2));
  } catch (err) {
    console.error('Error creating default subscriptions_db.json', err);
  }

  return defaultSubs;
}

let subscriptionsDb: SubscriptionsData = loadInitialSubscriptions();

function persistSubscriptions() {
  try {
    const tmpFile = `${SUBSCRIPTIONS_DB_FILE}.tmp`;
    fs.writeFileSync(tmpFile, JSON.stringify(subscriptionsDb, null, 2));
    fs.renameSync(tmpFile, SUBSCRIPTIONS_DB_FILE);
  } catch (e) {
    console.error('Error persisting subscriptions_db.json', e);
  }
  if (isSharedKvEnabled) {
    kvSetJson('waqas:subscriptions_db', subscriptionsDb).catch(() => {});
  }
}

export interface StoreData {
  products: Product[];
  transactions: SaleTransaction[];
  purchases: PurchaseTransaction[];
  customers: Customer[];
  customerLedgers: CustomerLedgerEntry[];
  saleReturns: SaleReturn[];
  suppliers: Supplier[];
  heldBills: HeldBill[];
  branding: StoreBranding;
  userProfile: UserProfile;
  martOwner?: any;
  stockHistory?: StockTransaction[];
  purchaseReturns?: PurchaseReturn[];
  supplierLedgers?: SupplierLedgerEntry[];
  expenses?: ExpenseEntry[];
  staffList?: StaffMember[];
  printerSettings?: PrinterSettings;
  categories?: CategoryInfo[];
  dailyHisab?: Record<string, Record<string, Partial<DailyHisabRow>>>;
  receiptSettings?: ReceiptSettings;
  syncVersion?: number;
  lastUpdated?: string;
}

function normalizeStaffPermissions(raw?: Partial<StaffModulePermissions>): StaffModulePermissions {
  const r = raw || {};
  return {
    newSale: typeof r.newSale === 'boolean' ? r.newSale : DEFAULT_STAFF_PERMISSIONS.newSale,
    oldSale: typeof r.oldSale === 'boolean' ? r.oldSale : DEFAULT_STAFF_PERMISSIONS.oldSale,
    newPurchase: typeof r.newPurchase === 'boolean' ? r.newPurchase : DEFAULT_STAFF_PERMISSIONS.newPurchase,
    oldPurchase: typeof r.oldPurchase === 'boolean' ? r.oldPurchase : DEFAULT_STAFF_PERMISSIONS.oldPurchase,
    stock: typeof r.stock === 'boolean' ? r.stock : DEFAULT_STAFF_PERMISSIONS.stock,
    products: typeof r.products === 'boolean' ? r.products : DEFAULT_STAFF_PERMISSIONS.products,
    customers: typeof r.customers === 'boolean' ? r.customers : DEFAULT_STAFF_PERMISSIONS.customers,
    suppliers: typeof r.suppliers === 'boolean' ? r.suppliers : DEFAULT_STAFF_PERMISSIONS.suppliers,
    expenses: typeof r.expenses === 'boolean' ? r.expenses : DEFAULT_STAFF_PERMISSIONS.expenses,
    reports: typeof r.reports === 'boolean' ? r.reports : Boolean(r.canAccessReports),
    printing: typeof r.printing === 'boolean' ? r.printing : DEFAULT_STAFF_PERMISSIONS.printing,
    settings: typeof r.settings === 'boolean' ? r.settings : Boolean(r.canAccessSettings),
    canEditPrice: Boolean(r.canEditPrice),
    canGiveDiscount: Boolean(r.canGiveDiscount),
    canDeleteItems: Boolean(r.canDeleteItems),
    canAccessReports: typeof r.reports === 'boolean' ? r.reports : Boolean(r.canAccessReports),
    canAccessSettings: typeof r.settings === 'boolean' ? r.settings : Boolean(r.canAccessSettings),
    canAccessLedgers: typeof r.canAccessLedgers === 'boolean' ? r.canAccessLedgers : Boolean(r.customers || r.suppliers),
    canViewCostAndProfit: Boolean(r.canViewCostAndProfit),
  };
}

function normalizeStoreData(raw: Partial<StoreData>, fallbackShopName = 'WAQAS MART'): StoreData {
  const staffNormalized: StaffMember[] = Array.isArray(raw.staffList)
    ? raw.staffList.map((st: any) => ({
        ...st,
        permissions: normalizeStaffPermissions(st?.permissions),
      }))
    : [];

  return {
    products: Array.isArray(raw.products) ? raw.products : [],
    transactions: Array.isArray(raw.transactions) ? raw.transactions : [],
    purchases: Array.isArray(raw.purchases) ? raw.purchases : [],
    customers: Array.isArray(raw.customers) ? raw.customers : [],
    customerLedgers: Array.isArray(raw.customerLedgers) ? raw.customerLedgers : [],
    saleReturns: Array.isArray(raw.saleReturns) ? raw.saleReturns : [],
    suppliers: Array.isArray(raw.suppliers) && raw.suppliers.length > 0 ? raw.suppliers : INITIAL_SUPPLIERS,
    heldBills: Array.isArray(raw.heldBills) ? raw.heldBills : [],
    stockHistory: Array.isArray(raw.stockHistory) ? raw.stockHistory : [],
    purchaseReturns: Array.isArray(raw.purchaseReturns) ? raw.purchaseReturns : [],
    supplierLedgers: Array.isArray(raw.supplierLedgers) ? raw.supplierLedgers : [],
    expenses: Array.isArray(raw.expenses) ? raw.expenses : [],
    staffList: staffNormalized,
    printerSettings: raw.printerSettings,
    categories: Array.isArray(raw.categories) && raw.categories.length > 0 ? raw.categories : SCREENSHOT_CATEGORIES,
    dailyHisab: raw.dailyHisab && typeof raw.dailyHisab === 'object' ? raw.dailyHisab : {},
    receiptSettings: raw.receiptSettings,
    martOwner: raw.martOwner,
    syncVersion: typeof raw.syncVersion === 'number' ? raw.syncVersion : 1,
    lastUpdated: raw.lastUpdated || new Date().toISOString(),
    branding: raw.branding || {
      storeName: fallbackShopName,
      primaryName: fallbackShopName.split(' ')[0] || 'WAQAS',
      accentName: fallbackShopName.split(' ').slice(1).join(' ') || 'MART',
      subtitle: 'Supermarket Management',
      primaryColor: '#00a859',
      sidebarPromoTitle: 'Small Steps Big Dreams',
      sidebarPromoBadge: fallbackShopName,
      bottomBannerTitle: 'Fresh Products Everyday',
      bottomBannerSubtitle: 'Best Quality • Lowest Prices',
      terminalDeviceId: 'WM-001',
    },
    userProfile: raw.userProfile || {
      id: 'USR-SHOP-OWNER',
      displayName: 'Shop Owner',
      email: '',
      phone: '',
      role: 'Shop Owner',
      createdAt: new Date().toLocaleDateString('en-GB'),
    },
  };
}

function getShopStoreFile(shopId: string): string {
  const cleanId = (shopId || 'SHOP-WM-001').replace(/[^a-zA-Z0-9_-]/g, '');
  return path.join(SHOPS_DIR, `${cleanId}.json`);
}

const shopStoreCache = new Map<string, StoreData>();

function getShopStore(shopId: string): StoreData {
  const targetId = shopId || 'SHOP-WM-001';
  const cached = shopStoreCache.get(targetId);
  if (cached) return cached;

  const shopMeta = subscriptionsDb.shops.find((s) => s.id === targetId);
  const shopName =
    shopMeta?.shopName ||
    (targetId === SUPER_ADMIN_POS_SHOP_ID
      ? 'MY POS APP'
      : targetId === 'SHOP-WM-001'
      ? 'WAQAS MART'
      : 'NEW MART');
  const file = getShopStoreFile(targetId);

  if (fs.existsSync(file)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(file, 'utf-8'));
      const normalized = normalizeStoreData(parsed, shopName);
      shopStoreCache.set(targetId, normalized);
      return normalized;
    } catch (e) {
      console.warn(`Failed to parse shop store for ${targetId}, using fallback`, e);
    }
  }

  if (targetId === 'SHOP-WM-001' && fs.existsSync(STORE_DB_FILE)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(STORE_DB_FILE, 'utf-8'));
      const normalized = normalizeStoreData(parsed, shopName);
      shopStoreCache.set(targetId, normalized);
      fs.writeFileSync(file, JSON.stringify(normalized, null, 2));
      return normalized;
    } catch {}
  }

  const fresh = normalizeStoreData({}, shopName);
  if (targetId === SUPER_ADMIN_POS_SHOP_ID) {
    fresh.userProfile = {
      id: authDb.superAdmin?.id || 'SUPER_ADMIN_ROOT',
      displayName: authDb.superAdmin?.fullName || 'Muhammad Saad (Super Admin)',
      email: authDb.superAdmin?.gmail || '',
      phone: authDb.superAdmin?.mobile || '03228570782',
      role: 'SUPER_ADMIN',
      createdAt: new Date().toLocaleDateString('en-GB'),
    };
    fresh.martOwner = {
      martName: 'MY POS APP',
      badge: 'SUPER ADMIN POS',
      ownerName: authDb.superAdmin?.fullName || 'Muhammad Saad',
      designation: 'Super Admin Personal POS',
      mobileNumber: authDb.superAdmin?.mobile || '03228570782',
      whatsappNumber: authDb.superAdmin?.mobile || '03228570782',
      address: 'Super Admin Personal Terminal',
      photoUrl: '/5c454cea45cc8c39a24b57a83fbbc5cf_0.webp',
    };
  } else if (shopMeta) {
    fresh.userProfile = {
      id: `USR-${targetId}`,
      displayName: shopMeta.ownerName || shopName,
      email: shopMeta.email || '',
      phone: shopMeta.phone || '',
      role: 'Shop Owner',
      createdAt: shopMeta.createdAt || new Date().toLocaleDateString('en-GB'),
    };
  }
  shopStoreCache.set(targetId, fresh);
  try {
    fs.writeFileSync(file, JSON.stringify(fresh, null, 2));
    if (targetId === 'SHOP-WM-001') {
      fs.writeFileSync(STORE_DB_FILE, JSON.stringify(fresh, null, 2));
    }
  } catch {}
  return fresh;
}

function persistShopStore(shopId: string, bumpVersion = true): StoreData {
  const targetId = shopId || 'SHOP-WM-001';
  const store = getShopStore(targetId);
  if (bumpVersion) {
    store.syncVersion = (Number(store.syncVersion) || 0) + 1;
    store.lastUpdated = new Date().toISOString();
  }
  const serialized = JSON.stringify(store, null, 2);
  try {
    fs.writeFileSync(getShopStoreFile(targetId), serialized);
    if (targetId === 'SHOP-WM-001') {
      fs.writeFileSync(STORE_DB_FILE, serialized);
    }
  } catch (err) {
    console.error(`Error saving shop store data for ${targetId}:`, err);
  }
  if (isSharedKvEnabled) {
    kvSetJson(`waqas:shop:${targetId}`, store).catch(() => {});
  }
  return store;
}

function setShopStore(shopId: string, data: StoreData, bumpVersion = true): StoreData {
  const targetId = shopId || 'SHOP-WM-001';
  const shopMeta = subscriptionsDb.shops.find((s) => s.id === targetId);
  const fallbackName = targetId === SUPER_ADMIN_POS_SHOP_ID ? 'MY POS APP' : shopMeta?.shopName || 'WAQAS MART';
  const normalized = normalizeStoreData(data, fallbackName);
  shopStoreCache.set(targetId, normalized);
  return persistShopStore(targetId, bumpVersion);
}

function sanitizeStoreForClient(store: StoreData, role: UserRole, permissions?: StaffModulePermissions): StoreData {
  const cloned: StoreData = {
    ...store,
    staffList: (store.staffList || []).map((st: any) => ({
      ...st,
      passwordHash: undefined,
      passwordSalt: undefined,
    })),
  };

  if (role === 'STAFF' && !permissions?.canViewCostAndProfit) {
    cloned.products = (store.products || []).map((p) => ({ ...p, costPrice: 0 }));
    cloned.transactions = (store.transactions || []).map((tx) => ({
      ...tx,
      items: (tx.items || []).map((it) => ({
        ...it,
        product: it.product ? { ...it.product, costPrice: 0 } : it.product,
      })),
    }));
    if (!permissions?.newPurchase && !permissions?.oldPurchase && !permissions?.reports) {
      cloned.purchases = [];
      cloned.purchaseReturns = [];
    }
    if (!permissions?.expenses && !permissions?.reports) {
      cloned.expenses = [];
    }
  }

  return cloned;
}

function loadInitialDevices(): DeviceInfo[] {
  if (fs.existsSync(DEVICES_DB_FILE)) {
    try {
      const content = fs.readFileSync(DEVICES_DB_FILE, 'utf-8');
      const parsed = JSON.parse(content);
      if (Array.isArray(parsed)) {
        return parsed.map((d: any) => ({
          ...d,
          shopId: d.shopId || 'SHOP-WM-001',
        }));
      }
    } catch (e) {
      console.warn('Failed to parse devices_db.json, initializing', e);
    }
  }

  const initialDevices: DeviceInfo[] = [
    {
      deviceId: 'DEV-PRIMARY-TERMINAL',
      sessionToken: 'TOKEN-PRIMARY-TERMINAL-ROOT',
      shopId: 'SHOP-WM-001',
      deviceName: 'Main Store Windows Terminal',
      deviceType: 'Windows PC',
      browser: 'Chrome / Desktop',
      os: 'Windows 11 / 10',
      isCurrentDevice: false,
      firstRegistered: new Date().toLocaleDateString('en-GB'),
      lastActive: new Date().toLocaleTimeString('en-PK', { hour: '2-digit', minute: '2-digit' }),
      status: 'active',
      connectionState: 'online',
    },
  ];

  try {
    fs.writeFileSync(DEVICES_DB_FILE, JSON.stringify(initialDevices, null, 2));
  } catch (err) {
    console.error('Error saving default devices', err);
  }

  return initialDevices;
}

getShopStore('SHOP-WM-001');
let registeredDevices: DeviceInfo[] = loadInitialDevices();
const pairingSessions = new Map<string, PairingSession>();
const removedDeviceIds = new Set<string>();

function loadPairingSessionsFromDisk() {
  if (!fs.existsSync(PAIRING_SESSIONS_FILE)) return;
  try {
    const raw = JSON.parse(fs.readFileSync(PAIRING_SESSIONS_FILE, 'utf-8'));
    if (Array.isArray(raw)) {
      const now = Date.now();
      for (const s of raw) {
        if (s && s.pairingToken && (s.expiresAt > now || s.status === 'approved')) {
          pairingSessions.set(s.pairingToken, s);
        }
      }
    }
  } catch {}
}

function persistPairingSessions() {
  try {
    const now = Date.now();
    const list = Array.from(pairingSessions.values()).filter(
      (s) => s.expiresAt > now - 60000 || s.status === 'approved'
    );
    fs.writeFileSync(PAIRING_SESSIONS_FILE, JSON.stringify(list, null, 2));
  } catch {}
}

loadPairingSessionsFromDisk();

async function savePairingSessionShared(session: PairingSession): Promise<void> {
  pairingSessions.set(session.pairingToken, session);
  persistPairingSessions();
  if (isSharedKvEnabled) {
    await Promise.all([
      kvSetJson(`waqas:pairing:${session.pairingToken}`, session, 360),
      kvSetJson(`waqas:pairing_code:${session.manualCode.toUpperCase()}`, session.pairingToken, 360),
    ]);
  }
}

async function deletePairingSessionShared(session: PairingSession): Promise<void> {
  pairingSessions.delete(session.pairingToken);
  persistPairingSessions();
  if (isSharedKvEnabled) {
    await Promise.all([
      kvDel(`waqas:pairing:${session.pairingToken}`),
      kvDel(`waqas:pairing_code:${session.manualCode.toUpperCase()}`),
    ]);
  }
}

async function findPairingSessionShared(tokenOrCode: string): Promise<PairingSession | undefined> {
  const clean = tokenOrCode.trim();
  if (!clean) return undefined;

  loadPairingSessionsFromDisk();

  let session = pairingSessions.get(clean);
  if (!session) {
    for (const s of pairingSessions.values()) {
      if (s.manualCode.toUpperCase() === clean.toUpperCase()) {
        session = s;
        break;
      }
    }
  }

  if (!session && isSharedKvEnabled) {
    const direct = await kvGetJson<PairingSession>(`waqas:pairing:${clean}`);
    if (direct) {
      pairingSessions.set(direct.pairingToken, direct);
      return direct;
    }
    const mappedToken = await kvGetJson<string>(`waqas:pairing_code:${clean.toUpperCase()}`);
    if (mappedToken) {
      const byCode = await kvGetJson<PairingSession>(`waqas:pairing:${mappedToken}`);
      if (byCode) {
        pairingSessions.set(byCode.pairingToken, byCode);
        return byCode;
      }
    }
  } else if (session && isSharedKvEnabled) {
    const fresh = await kvGetJson<PairingSession>(`waqas:pairing:${session.pairingToken}`);
    if (fresh) {
      pairingSessions.set(fresh.pairingToken, fresh);
      return fresh;
    }
  }

  return session;
}

function verifyDeviceAccess(
  deviceId?: string,
  sessionToken?: string
): { allowed: boolean; statusCode: number; error?: string; message?: string; device?: DeviceInfo } {
  if (!deviceId) {
    return { allowed: true, statusCode: 200 };
  }
  if (removedDeviceIds.has(deviceId)) {
    return {
      allowed: false,
      statusCode: 403,
      error: 'DEVICE_REMOVED',
      message: 'This device was removed by the administrator. Re-pair via QR code to reconnect.',
    };
  }
  const dev = registeredDevices.find((d) => d.deviceId === deviceId);
  if (!dev) {
    if (sessionToken) {
      return {
        allowed: false,
        statusCode: 403,
        error: 'DEVICE_UNKNOWN',
        message: 'Device session is no longer registered. Please pair again.',
      };
    }
    return { allowed: true, statusCode: 200 };
  }
  if (dev.status === 'revoked') {
    return {
      allowed: false,
      statusCode: 403,
      error: 'DEVICE_REVOKED',
      message: 'Device access has been revoked by administrator.',
      device: dev,
    };
  }
  if (dev.sessionToken && sessionToken && dev.sessionToken !== sessionToken) {
    return {
      allowed: false,
      statusCode: 403,
      error: 'INVALID_DEVICE_TOKEN',
      message: 'Invalid or expired device authorization token.',
      device: dev,
    };
  }
  return { allowed: true, statusCode: 200, device: dev };
}

function recordShopStockTx(
  shopData: StoreData,
  product: Product,
  previousStock: number,
  quantityChange: number,
  newStock: number,
  transactionType: StockTransactionType,
  invoiceNumber: string,
  userName?: string,
  notes?: string
) {
  const entry: StockTransaction = {
    id: `STK-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`,
    productId: product.id,
    productName: product.name,
    barcode: product.barcode,
    previousStock,
    quantityChange,
    newStock,
    transactionType,
    invoiceNumber,
    dateTime: new Date().toLocaleString('en-PK', {
      day: 'numeric',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: true,
    }),
    userName,
    notes,
  };
  shopData.stockHistory = [entry, ...(shopData.stockHistory || [])].slice(0, 5000);
}

function persistDevices() {
  try {
    fs.writeFileSync(DEVICES_DB_FILE, JSON.stringify(registeredDevices, null, 2));
  } catch (e) {
    console.error('Error persisting devices_db.json', e);
  }
  if (isSharedKvEnabled) {
    kvSetJson('waqas:devices_db', {
      devices: registeredDevices,
      removed: Array.from(removedDeviceIds),
    }).catch(() => {});
  }
}

let lastKvHydrationAt = 0;
let inFlightKvHydration: Promise<void> | null = null;

async function ensureSharedKvSessionSecret(): Promise<void> {
  if (!isSharedKvEnabled || getExplicitEnvSessionSecret()) return;
  if (sharedKvSessionSecret) return;

  const existing = await kvGetResult<string>('waqas:session_secret');
  if (existing.ok && existing.found && typeof existing.value === 'string' && existing.value.length >= 32) {
    sharedKvSessionSecret = existing.value;
    return;
  }
  if (existing.ok && !existing.found) {
    const generated = crypto.randomBytes(32).toString('hex');
    await kvSetNxJson('waqas:session_secret', generated);
    const confirmed = await kvGetResult<string>('waqas:session_secret');
    if (confirmed.ok && confirmed.found && typeof confirmed.value === 'string' && confirmed.value.length >= 32) {
      sharedKvSessionSecret = confirmed.value;
    } else {
      sharedKvSessionSecret = generated;
    }
  }
}

async function hydrateSharedKvIfConfigured(force = false): Promise<void> {
  if (!isSharedKvEnabled) return;
  const now = Date.now();
  if (!force && now - lastKvHydrationAt < 1500) return;
  if (inFlightKvHydration) {
    await inFlightKvHydration;
    if (!force) return;
  }

  inFlightKvHydration = (async () => {
    lastKvHydrationAt = Date.now();
    try {
      await ensureSharedKvSessionSecret();
      const [kvAuthRes, kvSubsRes, kvDevsRes] = await Promise.all([
        kvGetResult<AuthDb>('waqas:auth_db'),
        kvGetResult<SubscriptionsData>('waqas:subscriptions_db'),
        kvGetResult<{ devices?: DeviceInfo[]; removed?: string[] }>('waqas:devices_db'),
      ]);

      if (kvAuthRes.ok && kvAuthRes.found && kvAuthRes.value) {
        authDb = normalizeLoadedAuthDb(kvAuthRes.value);
      } else if (kvAuthRes.ok && !kvAuthRes.found) {
        await kvSetJson('waqas:auth_db', authDb);
      }

      if (kvSubsRes.ok && kvSubsRes.found && kvSubsRes.value && Array.isArray(kvSubsRes.value.shops)) {
        subscriptionsDb = kvSubsRes.value;
      } else if (kvSubsRes.ok && !kvSubsRes.found) {
        await kvSetJson('waqas:subscriptions_db', subscriptionsDb);
      }

      if (kvDevsRes.ok && kvDevsRes.found && kvDevsRes.value && Array.isArray(kvDevsRes.value.devices)) {
        registeredDevices = kvDevsRes.value.devices;
        removedDeviceIds.clear();
        for (const rId of kvDevsRes.value.removed || []) removedDeviceIds.add(rId);
      } else if (kvDevsRes.ok && !kvDevsRes.found) {
        await kvSetJson('waqas:devices_db', {
          devices: registeredDevices,
          removed: Array.from(removedDeviceIds),
        });
      }
    } catch {}
  })();

  try {
    await inFlightKvHydration;
  } finally {
    inFlightKvHydration = null;
  }
}

async function ensureShopStoreHydratedFromKv(shopId: string): Promise<StoreData> {
  if (isSharedKvEnabled) {
    const kvStore = await kvGetJson<StoreData>(`waqas:shop:${shopId}`);
    if (kvStore) {
      const shopMeta = subscriptionsDb.shops.find((s) => s.id === shopId);
      const fallbackName =
        shopId === SUPER_ADMIN_POS_SHOP_ID ? 'MY POS APP' : shopMeta?.shopName || 'WAQAS MART';
      const normalized = normalizeStoreData(kvStore, fallbackName);
      shopStoreCache.set(shopId, normalized);
      return normalized;
    }
  }
  return getShopStore(shopId);
}

interface SseClient {
  id: string;
  res: Response;
  deviceId: string;
  shopId: string;
  role: UserRole;
  canViewCostAndProfit: boolean;
  ip: string;
  connectedAt: number;
}
const sseClients = new Map<string, SseClient>();

function sanitizeSseEventForClient(event: SyncEvent, client: SseClient): SyncEvent {
  if (client.role !== 'STAFF' || client.canViewCostAndProfit) {
    return event;
  }
  const p = event.payload;
  if (!p) return event;

  if (event.type === 'store:sync' && typeof p === 'object') {
    return {
      ...event,
      payload: sanitizeStoreForClient(p as StoreData, 'STAFF', { ...DEFAULT_STAFF_PERMISSIONS, canViewCostAndProfit: false }),
    };
  }
  if (Array.isArray(p) && event.type === 'stock:updated') {
    return {
      ...event,
      payload: p.map((item: any) => ({ ...item, costPrice: 0 })),
    };
  }
  if (typeof p === 'object' && Array.isArray(p.updatedProducts)) {
    return {
      ...event,
      payload: {
        ...p,
        updatedProducts: p.updatedProducts.map((item: any) => ({ ...item, costPrice: 0 })),
      },
    };
  }
  return event;
}

function broadcastSyncEventToShop(shopId: string, event: SyncEvent) {
  const targetShopId = shopId || 'SHOP-WM-001';
  sseClients.forEach((client) => {
    if (client.shopId !== targetShopId) return;
    try {
      const safeEvent = sanitizeSseEventForClient(event, client);
      client.res.write(`data: ${JSON.stringify(safeEvent)}\n\n`);
    } catch {
      sseClients.delete(client.id);
    }
  });
}

function disconnectShopClients(
  shopId: string,
  eventType: 'device:revoked' | 'auth:shop_restricted' = 'device:revoked',
  message?: string
) {
  for (const [cid, client] of sseClients.entries()) {
    if (client.shopId === shopId) {
      try {
        client.res.write(
          `data: ${JSON.stringify({
            type: eventType,
            timestamp: Date.now(),
            payload: { shopId, message },
          })}\n\n`
        );
        client.res.end();
      } catch {}
      sseClients.delete(cid);
    }
  }
}

export function closeAllSseClients() {
  sseClients.forEach((client) => {
    try {
      if (!client.res.writableEnded && !client.res.destroyed) {
        client.res.end();
      }
    } catch {}
  });
  sseClients.clear();
}

let maintenanceStarted = false;
export function startBackgroundMaintenanceTimers() {
  if (maintenanceStarted || isServerlessRuntime) return;
  maintenanceStarted = true;

  const cleanupTimer = setInterval(() => {
    const now = Date.now();
    let pairingChanged = false;
    for (const [token, session] of pairingSessions.entries()) {
      if (session.expiresAt <= now) {
        session.status = 'expired';
        pairingSessions.delete(token);
        pairingChanged = true;
      }
    }
    if (pairingChanged) persistPairingSessions();

    let otpDirty = false;
    for (const [k, otp] of Object.entries(authDb.pendingOtps || {})) {
      if (otp.expiresAt <= now) {
        delete authDb.pendingOtps[k];
        otpDirty = true;
      }
    }
    if (otpDirty) persistAuthDb();
  }, 60 * 1000);
  cleanupTimer.unref?.();

  const pingTimer = setInterval(() => {
    const ping = JSON.stringify({
      type: 'ping',
      timestamp: Date.now(),
    });
    sseClients.forEach((client) => {
      if (client.res.writableEnded || client.res.destroyed) {
        sseClients.delete(client.id);
        return;
      }
      try {
        client.res.write(`data: ${ping}\n\n`);
      } catch {
        sseClients.delete(client.id);
      }
    });
  }, 20 * 1000);
  pingTimer.unref?.();
}

let cachedApp: Application | null = null;

export function getRuntimeInfo() {
  return {
    serverless: isServerlessRuntime,
    storageMode: isSharedKvEnabled ? 'vercel-kv' : resolvedStorage.storageMode,
    sharedAcrossInstances: isSharedKvEnabled || !isServerlessRuntime,
    hasSessionSecret: hasConfiguredSessionSecret(),
    dataDir: DATA_DIR,
  };
}

function asyncHandler(fn: (req: Request, res: Response, next: NextFunction) => Promise<any>) {
  return (req: Request, res: Response, next: NextFunction) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

function isSignedTokenShape(token: string): boolean {
  const parts = token.split('.');
  return parts.length === 3 && (parts[0] === 'SA' || parts[0] === 'SO' || parts[0] === 'STF');
}

function extractSessionTokenFromReq(req: Request): string {
  const candidates: string[] = [];
  const authHeader = req.headers.authorization;
  if (typeof authHeader === 'string' && authHeader.startsWith('Bearer ')) {
    candidates.push(authHeader.substring(7).trim());
  }
  const headerTok = req.headers['x-session-token'];
  if (typeof headerTok === 'string') {
    candidates.push(headerTok.trim());
  } else if (Array.isArray(headerTok) && headerTok[0]) {
    candidates.push(String(headerTok[0]).trim());
  }
  if (req.query?.token && typeof req.query.token === 'string') {
    candidates.push(req.query.token.trim());
  }
  if (req.body && typeof req.body === 'object' && req.body.sessionToken) {
    candidates.push(String(req.body.sessionToken).trim());
  }

  for (const raw of candidates) {
    if (raw && raw !== 'null' && raw !== 'undefined') {
      return raw;
    }
  }
  return '';
}

export function createApiApp(): Application {
  if (cachedApp) return cachedApp;

  const app = express();
  app.set('trust proxy', 1);

  // 1. CORS & Preflight Handling (No wildcard '*' on authenticated requests)
  app.use((req: Request, res: Response, next: NextFunction) => {
    const origin = req.headers.origin;
    if (origin) {
      res.header('Access-Control-Allow-Origin', origin);
      res.header('Access-Control-Allow-Credentials', 'true');
      res.header('Vary', 'Origin');
    }
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH');
    res.header(
      'Access-Control-Allow-Headers',
      'Origin, X-Requested-With, Content-Type, Accept, Authorization, x-device-id, x-device-token, x-session-token, x-shop-id'
    );
    if (req.method === 'OPTIONS') {
      return res.status(204).end();
    }
    next();
  });

  // Guard pre-parsed request body (Vercel Node.js runtime sometimes parses req.body before Express)
  app.use((req: Request, _res: Response, next: NextFunction) => {
    if (
      req.body !== undefined &&
      (req.readableEnded || (req as any)._readableState?.ended || (req as any).complete)
    ) {
      (req as any)._body = true;
    }
    next();
  });

  app.use(express.json({ limit: '25mb' }));

  // Hydrate shared KV state on serverless invocations when configured
  if (isSharedKvEnabled) {
    app.use('/api', async (req: Request, _res: Response, next: NextFunction) => {
      if (req.path === '/health' || req.path === '/ping') return next();
      try {
        await hydrateSharedKvIfConfigured();
        next();
      } catch (err) {
        next(err);
      }
    });
  }

  // Health & ping routes
  app.get('/api/health', asyncHandler(async (_req: Request, res: Response) => {
    if (isSharedKvEnabled && !sharedKvSessionSecret) {
      await ensureSharedKvSessionSecret();
    }
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.json({
      status: 'healthy',
      app: 'WAQAS POS SOFTWARE',
      version: APP_VERSION,
      buildId: APP_BUILD_ID,
      buildDate: APP_BUILD_DATE,
      serverTime: new Date().toISOString(),
      devicesConnected: registeredDevices.length,
      runtime: getRuntimeInfo(),
    });
  }));

  app.get('/api/ping', (_req: Request, res: Response) => {
    res.json({ pong: true, time: Date.now(), serverless: isServerlessRuntime });
  });

  app.get(['/download', '/download-app', '/api/download', '/waqas-mart-pos.zip'], (_req: Request, res: Response) => {
    return res.status(404).json({
      success: false,
      error: 'ZIP_DOWNLOAD_REMOVED',
      message: 'Offline ZIP package download has been removed in this version.',
    });
  });

  app.use((req: Request, _res: Response, next: NextFunction) => {
    if (req.path.startsWith('/api') && !req.path.includes('/events') && !req.path.includes('/ping')) {
      console.log(`[API] ${req.method} ${req.path}`);
    }
    next();
  });

  // -------------------------------------------------------------
  // 1. CENTRALIZED AUTHENTICATION (SUPER ADMIN, SHOP OWNER, STAFF)
  // -------------------------------------------------------------
  function getSessionFromToken(token: string): AuthSessionData | null {
    if (!token) return null;

    const storedSession = authDb.sessions[token];
    if (storedSession) {
      if (storedSession.expiresAt <= Date.now() || storedSession.policyVersion !== AUTH_POLICY_VERSION) {
        delete authDb.sessions[token];
        persistAuthDb();
        return null;
      }
      return storedSession;
    }

    // Fallback: verify HMAC-signed stateless token (for multi-instance Vercel serverless compatibility)
    const verified = verifySignedSessionToken(token);
    if (verified) {
      return verified;
    }
    return null;
  }

  function getSessionFromReq(req: Request): AuthSessionData | null {
    return getSessionFromToken(extractSessionTokenFromReq(req));
  }

  async function getSessionFromReqAsync(req: Request): Promise<{
    session: AuthSessionData | null;
    token: string;
    ephemeralContainerMismatch: boolean;
  }> {
    const token = extractSessionTokenFromReq(req);
    if (!token) {
      return { session: null, token: '', ephemeralContainerMismatch: false };
    }

    let session = getSessionFromToken(token);
    if (!session && isSharedKvEnabled) {
      await hydrateSharedKvIfConfigured(true);
      session = getSessionFromToken(token);
    }

    const ephemeralContainerMismatch = Boolean(
      !session &&
        isSignedTokenShape(token) &&
        isServerlessRuntime &&
        !hasConfiguredSessionSecret() &&
        !isSharedKvEnabled
    );

    return { session, token, ephemeralContainerMismatch };
  }

  function checkAndAutoExpireSubscriptions() {
    const todayStr = new Date().toISOString().split('T')[0];
    let updated = false;

    subscriptionsDb.shops.forEach((shop) => {
      if (shop.status === 'blocked' || shop.status === 'suspended' || shop.status === 'inactive') {
        return;
      }
      if (shop.expiryDate < todayStr) {
        if (shop.status !== 'expired') {
          shop.status = 'expired';
          updated = true;
          recordAuditLog(
            'SUBSCRIPTION_EXPIRED',
            'system',
            'SUPER_ADMIN',
            `Shop ${shop.shopName} (${shop.id}) expired on ${shop.expiryDate}`,
            shop.id
          );
        }
      } else {
        const expTime = new Date(shop.expiryDate).getTime();
        const diffDays = Math.ceil((expTime - Date.now()) / (1000 * 60 * 60 * 24));
        if (diffDays <= 7 && shop.status === 'active') {
          shop.status = 'expiring_soon';
          updated = true;
        }
      }
    });

    if (updated) {
      persistSubscriptions();
    }
  }

  checkAndAutoExpireSubscriptions();

  function sanitizeAuthUser(session: AuthSessionData): AuthUser {
    if (session.role === 'SUPER_ADMIN' && authDb.superAdmin) {
      return {
        id: authDb.superAdmin.id,
        gmail: authDb.superAdmin.gmail,
        email: authDb.superAdmin.gmail,
        mobile: authDb.superAdmin.mobile,
        phone: authDb.superAdmin.mobile,
        fullName: authDb.superAdmin.fullName,
        role: 'SUPER_ADMIN',
        verifiedGmail: authDb.superAdmin.verifiedGmail,
        verifiedMobile: authDb.superAdmin.verifiedMobile,
        createdAt: authDb.superAdmin.createdAt,
      };
    }
    const shopMeta = session.shopId ? subscriptionsDb.shops.find((s) => s.id === session.shopId) : undefined;
    if (session.role === 'STAFF' && session.shopId && session.staffId) {
      const store = getShopStore(session.shopId);
      const staff = (store.staffList || []).find((s) => s.id === session.staffId);
      return {
        id: session.userId,
        username: staff?.username || session.username,
        fullName: staff?.name || session.fullName || 'Staff Member',
        phone: staff?.phone,
        role: 'STAFF',
        shopId: session.shopId,
        shopName: shopMeta?.shopName,
        staffId: session.staffId,
        permissions: normalizeStaffPermissions(staff?.permissions || session.permissions),
        createdAt: staff?.createdAt || new Date().toISOString(),
      };
    }
    const owner = authDb.users.find((u) => u.id === session.userId);
    return {
      id: owner?.id || session.userId,
      email: owner?.email || session.email,
      username: owner?.username || session.username,
      fullName: owner?.fullName || shopMeta?.ownerName || 'Shop Owner',
      phone: owner?.phone || shopMeta?.phone,
      whatsapp: owner?.whatsapp || shopMeta?.whatsapp,
      role: session.role,
      shopId: session.shopId,
      shopName: shopMeta?.shopName,
      mustChangePasswordOnFirstLogin: owner?.mustChangePasswordOnFirstLogin,
      createdAt: owner?.createdAt || new Date().toISOString(),
    };
  }

  interface ResolvedActor {
    ok: boolean;
    statusCode: number;
    error?: string;
    message?: string;
    role: UserRole;
    shopId: string;
    actorEmail: string;
    staffId?: string;
    permissions?: StaffModulePermissions;
    device?: DeviceInfo;
    session?: AuthSessionData;
  }

  async function resolveShopActorAsync(req: Request): Promise<ResolvedActor> {
    checkAndAutoExpireSubscriptions();
    const { session, ephemeralContainerMismatch } = await getSessionFromReqAsync(req);
    const deviceId =
      (req.headers['x-device-id'] as string) ||
      (req.query.deviceId as string) ||
      (req.body?.sourceDeviceId as string) ||
      '';
    const deviceToken =
      (req.headers['x-device-token'] as string) ||
      (req.query.deviceToken as string) ||
      '';

    if (deviceId) {
      let devCheck = verifyDeviceAccess(deviceId, deviceToken || undefined);
      if (
        !devCheck.allowed &&
        isSharedKvEnabled &&
        (devCheck.error === 'DEVICE_UNKNOWN' || devCheck.error === 'INVALID_DEVICE_TOKEN')
      ) {
        await hydrateSharedKvIfConfigured(true);
        devCheck = verifyDeviceAccess(deviceId, deviceToken || undefined);
      }
      if (!devCheck.allowed) {
        const isSuperAdminSession = session?.role === 'SUPER_ADMIN';
        const isStaleTokenWithActiveSession =
          Boolean(session) &&
          (devCheck.error === 'DEVICE_UNKNOWN' || devCheck.error === 'INVALID_DEVICE_TOKEN');
        if (!isSuperAdminSession && !isStaleTokenWithActiveSession) {
          return {
            ok: false,
            statusCode: devCheck.statusCode,
            error: devCheck.error,
            message: devCheck.message,
            role: 'STAFF',
            shopId: '',
            actorEmail: deviceId,
          };
        }
      }
    }

    if (session) {
      if (session.role === 'SUPER_ADMIN') {
        if (!authDb.superAdmin) {
          return {
            ok: false,
            statusCode: 401,
            error: 'SUPER_ADMIN_NOT_CONFIGURED',
            message: 'Super Admin session is invalid.',
            role: 'SUPER_ADMIN',
            shopId: '',
            actorEmail: 'unknown',
          };
        }
        const requestedShopId =
          (req.query.shopId as string) ||
          (req.headers['x-shop-id'] as string) ||
          (req.body?.shopId as string) ||
          SUPER_ADMIN_POS_SHOP_ID;
        return {
          ok: true,
          statusCode: 200,
          role: 'SUPER_ADMIN',
          shopId: requestedShopId,
          actorEmail: authDb.superAdmin.gmail,
          session,
        };
      }

      const shopId = session.shopId || '';
      let shop = subscriptionsDb.shops.find((s) => s.id === shopId);
      if (!shop && isSharedKvEnabled) {
        await hydrateSharedKvIfConfigured(true);
        shop = subscriptionsDb.shops.find((s) => s.id === shopId);
      }
      if (!shop) {
        return {
          ok: false,
          statusCode: 403,
          error: 'SHOP_NOT_FOUND',
          message: 'Shop account no longer exists.',
          role: session.role,
          shopId,
          actorEmail: session.username || session.email || session.userId,
        };
      }

      if (shop.status === 'inactive' || shop.status === 'suspended' || shop.status === 'blocked' || shop.status === 'expired') {
        return {
          ok: false,
          statusCode: 403,
          error: 'SHOP_ACCOUNT_RESTRICTED',
          message: `Shop "${shop.shopName}" is ${shop.status.toUpperCase()}. Contact Muhammad Waqas (03228570782).`,
          role: session.role,
          shopId,
          actorEmail: session.username || session.email || session.userId,
        };
      }

      const explicitShop = (req.query.shopId as string) || (req.headers['x-shop-id'] as string);
      if (explicitShop && explicitShop !== shopId) {
        return {
          ok: false,
          statusCode: 403,
          error: 'CROSS_SHOP_ACCESS_DENIED',
          message: 'Access to another shop database is prohibited.',
          role: session.role,
          shopId,
          actorEmail: session.username || session.email || session.userId,
        };
      }

      let livePermissions = session.permissions;
      if (session.role === 'STAFF' && session.staffId) {
        const shopStore = await ensureShopStoreHydratedFromKv(shopId);
        const staff = (shopStore.staffList || []).find((st) => st.id === session.staffId);
        if (!staff || staff.accountStatus !== 'active') {
          return {
            ok: false,
            statusCode: 403,
            error: 'STAFF_ACCOUNT_RESTRICTED',
            message: 'Staff account is inactive or suspended.',
            role: 'STAFF',
            shopId,
            actorEmail: session.username || session.userId,
          };
        }
        livePermissions = normalizeStaffPermissions(staff.permissions);
      }

      const dev = deviceId ? registeredDevices.find((d) => d.deviceId === deviceId) : undefined;
      return {
        ok: true,
        statusCode: 200,
        role: session.role,
        shopId,
        actorEmail: session.username || session.email || session.userId,
        staffId: session.staffId,
        permissions: livePermissions,
        device: dev,
        session,
      };
    }

    if (deviceId && deviceToken) {
      let dev = registeredDevices.find((d) => d.deviceId === deviceId && d.sessionToken === deviceToken);
      if (!dev && isSharedKvEnabled) {
        await hydrateSharedKvIfConfigured(true);
        dev = registeredDevices.find((d) => d.deviceId === deviceId && d.sessionToken === deviceToken);
      }
      if (dev && dev.status === 'active') {
        const shopId = dev.shopId || 'SHOP-WM-001';
        const shop = subscriptionsDb.shops.find((s) => s.id === shopId);
        if (shop && (shop.status === 'inactive' || shop.status === 'suspended' || shop.status === 'blocked' || shop.status === 'expired')) {
          return {
            ok: false,
            statusCode: 403,
            error: 'SHOP_ACCOUNT_RESTRICTED',
            message: `Shop "${shop.shopName}" is ${shop.status.toUpperCase()}. Terminal access locked.`,
            role: 'SHOP_OWNER',
            shopId,
            actorEmail: dev.deviceId,
          };
        }
        const explicitShop = (req.query.shopId as string) || (req.headers['x-shop-id'] as string);
        if (explicitShop && explicitShop !== shopId) {
          return {
            ok: false,
            statusCode: 403,
            error: 'CROSS_SHOP_ACCESS_DENIED',
            message: 'Terminal cannot access another shop database.',
            role: 'SHOP_OWNER',
            shopId,
            actorEmail: dev.deviceId,
          };
        }
        return {
          ok: true,
          statusCode: 200,
          role: 'SHOP_OWNER',
          shopId,
          actorEmail: dev.deviceId,
          device: dev,
        };
      }
    }

    if (ephemeralContainerMismatch) {
      console.warn('[Auth] Session token rejected on serverless instance without shared SESSION_SECRET or KV store.');
      return {
        ok: false,
        statusCode: 401,
        error: 'SERVERLESS_SESSION_SECRET_MISSING',
        message:
          'Session verification failed across Vercel serverless instances because SESSION_SECRET (or Vercel KV) is not configured in Vercel Environment Variables. Add SESSION_SECRET in Vercel Project Settings and redeploy.',
        role: 'STAFF',
        shopId: '',
        actorEmail: 'anonymous',
      };
    }

    return {
      ok: false,
      statusCode: 401,
      error: 'AUTH_REQUIRED',
      message: 'Authentication required. Please log in with your Super Admin, Shop Owner, or Staff credentials.',
      role: 'STAFF',
      shopId: '',
      actorEmail: 'anonymous',
    };
  }

  function requireSuperAdmin(req: Request, res: Response, next: NextFunction) {
    getSessionFromReqAsync(req)
      .then(({ session, ephemeralContainerMismatch }) => {
        if (!session) {
          if (ephemeralContainerMismatch) {
            console.warn('[Auth] Super Admin session rejected on serverless instance without shared SESSION_SECRET or KV store.');
            return res.status(401).json({
              success: false,
              error: 'SERVERLESS_SESSION_SECRET_MISSING',
              message:
                'Super Admin session failed across Vercel serverless instances because SESSION_SECRET (or Vercel KV) is not configured in Vercel Environment Variables. Add SESSION_SECRET in Vercel Project Settings and redeploy.',
            });
          }
          return res.status(401).json({ success: false, error: 'AUTH_REQUIRED', message: 'Super Admin authentication required.' });
        }
        if (session.role !== 'SUPER_ADMIN' || !authDb.superAdmin) {
          return res.status(403).json({ success: false, error: 'SUPER_ADMIN_ONLY', message: 'Access restricted to Primary Super Admin.' });
        }
        (req as any).superAdminSession = session;
        next();
      })
      .catch(next);
  }

  function checkRateLimit(key: string): { blocked: boolean; retrySeconds?: number } {
    const rec = authDb.failedAttempts[key];
    if (!rec) return { blocked: false };
    const elapsed = Date.now() - rec.lastAttempt;
    if (elapsed > 15 * 60 * 1000) {
      delete authDb.failedAttempts[key];
      return { blocked: false };
    }
    if (rec.count >= 10) {
      return { blocked: true, retrySeconds: Math.ceil((15 * 60 * 1000 - elapsed) / 1000) };
    }
    return { blocked: false };
  }

  function recordFailedAttempt(key: string) {
    const prev = authDb.failedAttempts[key];
    const withinWindow = prev && Date.now() - prev.lastAttempt < 15 * 60 * 1000;
    authDb.failedAttempts[key] = {
      count: withinWindow ? prev.count + 1 : 1,
      lastAttempt: Date.now(),
    };
    persistAuthDb();
  }

  function clearFailedAttempt(key: string) {
    if (authDb.failedAttempts[key]) {
      delete authDb.failedAttempts[key];
      persistAuthDb();
    }
  }

  app.get('/api/auth/bootstrap-status', (_req: Request, res: Response) => {
    return res.json({
      success: true,
      superAdminConfigured: true,
      superAdminSetupLocked: true,
      registrationDisabled: true,
      googleOAuthEnabled: false,
    });
  });

  app.post('/api/auth/google/verify-id-token', (_req: Request, res: Response) => {
    return res.status(403).json({
      success: false,
      error: 'GOOGLE_OAUTH_DISABLED',
      message: 'Google OAuth sign-in is disabled. Use the fixed Super Admin Gmail and password.',
    });
  });

  app.get('/api/auth/google/url', (_req: Request, res: Response) => {
    return res.status(403).json({
      success: false,
      error: 'GOOGLE_OAUTH_DISABLED',
      message: 'Google OAuth sign-in is disabled. Use the fixed Super Admin Gmail and password.',
    });
  });

  app.get(['/api/auth/google/callback', '/api/auth/google/callback/'], (_req: Request, res: Response) => {
    return res.status(403).json({
      success: false,
      error: 'GOOGLE_OAUTH_DISABLED',
      message: 'Google OAuth callback is disabled.',
    });
  });

  app.post('/api/auth/super-admin/validate-provision-token', (_req: Request, res: Response) => {
    return res.status(403).json({
      success: false,
      error: 'SUPER_ADMIN_REGISTRATION_DISABLED',
      message: 'Super Admin registration and provisioning are permanently disabled.',
    });
  });

  app.post('/api/auth/super-admin/provision', (_req: Request, res: Response) => {
    return res.status(403).json({
      success: false,
      error: 'SUPER_ADMIN_REGISTRATION_DISABLED',
      message: 'Super Admin registration and provisioning are permanently disabled.',
    });
  });

  app.post('/api/auth/super-admin/send-setup-otp', (_req: Request, res: Response) => {
    return res.status(403).json({
      success: false,
      error: 'PUBLIC_REGISTRATION_DISABLED',
      message: 'Public Super Admin registration is permanently disabled.',
    });
  });

  app.post('/api/auth/super-admin/complete-setup', (_req: Request, res: Response) => {
    return res.status(403).json({
      success: false,
      error: 'PUBLIC_REGISTRATION_DISABLED',
      message: 'Public Super Admin registration is permanently disabled.',
    });
  });

  app.post('/api/auth/super-admin/recover/request', (_req: Request, res: Response) => {
    return res.status(403).json({
      success: false,
      error: 'OTP_RECOVERY_DISABLED',
      message: 'OTP recovery is disabled.',
    });
  });

  app.post('/api/auth/super-admin/recover/verify', (_req: Request, res: Response) => {
    return res.status(403).json({
      success: false,
      error: 'OTP_RECOVERY_DISABLED',
      message: 'OTP recovery is disabled.',
    });
  });

  // Fixed Super Admin Login
  app.post('/api/auth/super-admin/login', asyncHandler(async (req: Request, res: Response) => {
    let sa = authDb.superAdmin;
    if (!sa) {
      return res.status(500).json({
        success: false,
        error: 'SUPER_ADMIN_NOT_CONFIGURED',
        message: 'Super Admin account is unavailable on the server.',
      });
    }

    const rateKey = `super_admin:${req.ip || 'local'}`;
    const limitCheck = checkRateLimit(rateKey);
    if (limitCheck.blocked) {
      return res.status(429).json({
        success: false,
        error: 'TOO_MANY_ATTEMPTS',
        message: `Too many failed login attempts. Try again in ${limitCheck.retrySeconds} seconds.`,
      });
    }

    const { gmail, email, password } = req.body || {};
    const cleanGmail = String(gmail || email || '').trim().toLowerCase();
    const cleanPassword = String(password || '').trim();

    if (!cleanGmail || !cleanPassword) {
      return res.status(400).json({
        success: false,
        error: 'CREDENTIALS_REQUIRED',
        message: 'Super Admin Gmail and Password are required.',
      });
    }

    if (cleanGmail !== FIXED_SUPER_ADMIN_EMAIL || cleanGmail !== sa.gmail.trim().toLowerCase()) {
      recordFailedAttempt(rateKey);
      recordAuditLog(
        'SUPER_ADMIN_WRONG_GMAIL',
        cleanGmail,
        'SUPER_ADMIN',
        `Rejected unauthorized Gmail "${cleanGmail}" at Super Admin login`,
        undefined,
        req.ip
      );
      return res.status(403).json({
        success: false,
        error: 'UNAUTHORIZED_GMAIL',
        message: 'Access denied. Only the registered Super Admin Gmail is authorized.',
      });
    }

    let passwordMatches = verifyPasswordRecord(cleanPassword, sa.passwordSalt, sa.passwordHash);
    if (!passwordMatches && isSharedKvEnabled) {
      await hydrateSharedKvIfConfigured(true);
      sa = authDb.superAdmin;
      if (sa) {
        passwordMatches = verifyPasswordRecord(cleanPassword, sa.passwordSalt, sa.passwordHash);
      }
    }

    if (!passwordMatches || !sa) {
      recordFailedAttempt(rateKey);
      recordAuditLog(
        'SUPER_ADMIN_LOGIN_FAILED',
        sa?.gmail || cleanGmail,
        'SUPER_ADMIN',
        `Invalid Super Admin password attempt from ${req.ip || 'unknown'}`,
        undefined,
        req.ip
      );
      return res.status(401).json({
        success: false,
        error: 'INVALID_SUPER_ADMIN_PASSWORD',
        message: 'Incorrect Super Admin password.',
      });
    }

    if (isSharedKvEnabled && !sharedKvSessionSecret) {
      await ensureSharedKvSessionSecret();
    }

    clearFailedAttempt(rateKey);
    sa.lastLoginAt = new Date().toISOString();

    const now = Date.now();
    const baseSess = {
      userId: sa.id,
      email: sa.gmail,
      gmail: sa.gmail,
      fullName: sa.fullName,
      role: 'SUPER_ADMIN' as UserRole,
      createdAt: now,
      expiresAt: now + 24 * 60 * 60 * 1000,
      ip: req.ip,
      policyVersion: AUTH_POLICY_VERSION,
    };
    const token = createSignedSessionToken('SA', baseSess);
    const sess: AuthSessionData = { ...baseSess, token };
    authDb.sessions[token] = sess;
    persistAuthDb();

    recordAuditLog(
      'SUPER_ADMIN_LOGIN_SUCCESS',
      sa.gmail,
      'SUPER_ADMIN',
      'Super Admin authenticated via fixed Gmail + password credentials',
      undefined,
      req.ip
    );

    return res.json({
      success: true,
      token,
      expiresAt: new Date(sess.expiresAt).toISOString(),
      policyVersion: AUTH_POLICY_VERSION,
      user: sanitizeAuthUser(sess),
    });
  }));

  // Mart Login Endpoint
  app.post('/api/auth/login', asyncHandler(async (req: Request, res: Response) => {
    checkAndAutoExpireSubscriptions();
    const { portal = 'shop', identifier, password } = req.body || {};
    const cleanIdent = String(identifier || '').trim();
    const cleanPassword = String(password || '').trim();

    if (portal === 'super_admin') {
      return res.status(400).json({
        success: false,
        error: 'USE_SUPER_ADMIN_LOGIN',
        message: 'Super Admin must sign in through the Super Admin Login portal using authorized Gmail and Password.',
      });
    }

    if (!cleanIdent || !cleanPassword) {
      return res.status(400).json({
        success: false,
        error: 'CREDENTIALS_REQUIRED',
        message: 'Shop ID / Shop Username and Shop Password are required.',
      });
    }

    const rateKey = `${portal}:${cleanIdent.toLowerCase()}`;
    const limitCheck = checkRateLimit(rateKey);
    if (limitCheck.blocked) {
      return res.status(429).json({
        success: false,
        error: 'TOO_MANY_ATTEMPTS',
        message: `Too many failed attempts. Try again in ${limitCheck.retrySeconds} seconds.`,
      });
    }

    const lowerIdent = cleanIdent.toLowerCase();
    const normPhoneIdent = normalizeMobile(cleanIdent);

    const findOwnerAndShop = () => {
      const foundOwner = authDb.users.find(
        (u) =>
          u.role === 'SHOP_OWNER' &&
          (u.shopId?.trim().toLowerCase() === lowerIdent ||
            u.username?.trim().toLowerCase() === lowerIdent ||
            u.email?.trim().toLowerCase() === lowerIdent ||
            (normPhoneIdent && normalizeMobile(u.phone || '') === normPhoneIdent))
      );
      const foundShop = subscriptionsDb.shops.find(
        (s) =>
          s.id.trim().toLowerCase() === lowerIdent ||
          (s.ownerUsername && s.ownerUsername.trim().toLowerCase() === lowerIdent) ||
          (s.email && s.email.trim().toLowerCase() === lowerIdent)
      );
      return { foundOwner, foundShop };
    };

    let { foundOwner: ownerUser, foundShop: matchedShop } = findOwnerAndShop();
    if (!ownerUser && !matchedShop && isSharedKvEnabled) {
      await hydrateSharedKvIfConfigured(true);
      ({ foundOwner: ownerUser, foundShop: matchedShop } = findOwnerAndShop());
    }

    if (!ownerUser && matchedShop) {
      ownerUser = authDb.users.find((u) => u.role === 'SHOP_OWNER' && u.shopId === matchedShop.id);
      if (!ownerUser) {
        recordFailedAttempt(rateKey);
        return res.status(403).json({
          success: false,
          error: 'OWNER_LOGIN_NOT_CONFIGURED',
          shopId: matchedShop.id,
          shopName: matchedShop.shopName,
          message: `Mart "${matchedShop.shopName}" (${matchedShop.id}) does not have an owner password set yet. Ask Super Admin to open Mart Accounts and set/reset the owner password.`,
        });
      }
    }

    if (ownerUser) {
      let shop = subscriptionsDb.shops.find((s) => s.id === ownerUser!.shopId);
      if (!shop) {
        return res.status(404).json({
          success: false,
          error: 'SHOP_NOT_FOUND',
          message: 'Associated mart account was not found in database.',
        });
      }

      let ownerPasswordOk = verifyPassword(cleanPassword, ownerUser);
      if (!ownerPasswordOk && isSharedKvEnabled) {
        await hydrateSharedKvIfConfigured(true);
        const refreshed = findOwnerAndShop();
        if (refreshed.foundOwner) {
          ownerUser = refreshed.foundOwner;
          shop = subscriptionsDb.shops.find((s) => s.id === ownerUser!.shopId) || shop;
          ownerPasswordOk = verifyPassword(cleanPassword, ownerUser);
        }
      }

      if (!ownerPasswordOk) {
        recordFailedAttempt(rateKey);
        recordAuditLog(
          'SHOP_LOGIN_FAILED',
          cleanIdent,
          'SHOP_OWNER',
          `Invalid password for Shop Owner "${cleanIdent}" (${shop.id})`,
          shop.id,
          req.ip
        );
        return res.status(401).json({
          success: false,
          error: 'INVALID_PASSWORD',
          message: 'Incorrect Shop Password. Please check your password and try again.',
        });
      }

      if (shop.status === 'inactive' || shop.status === 'suspended' || shop.status === 'blocked' || shop.status === 'expired') {
        return res.status(403).json({
          success: false,
          error: 'SHOP_ACCOUNT_RESTRICTED',
          status: shop.status,
          shopName: shop.shopName,
          expiryDate: shop.expiryDate,
          whatsapp: '03228570782',
          message: `This mart account ("${shop.shopName}") is currently ${shop.status.toUpperCase()}. Contact Super Admin (03228570782) to restore access.`,
        });
      }

      if (isSharedKvEnabled && !sharedKvSessionSecret) {
        await ensureSharedKvSessionSecret();
      }

      clearFailedAttempt(rateKey);
      const now = Date.now();
      const baseSess = {
        userId: ownerUser.id,
        email: ownerUser.email,
        username: ownerUser.username,
        fullName: ownerUser.fullName,
        role: 'SHOP_OWNER' as UserRole,
        shopId: shop.id,
        createdAt: now,
        expiresAt: now + 30 * 24 * 60 * 60 * 1000,
        ip: req.ip,
        policyVersion: AUTH_POLICY_VERSION,
      };
      const token = createSignedSessionToken('SO', baseSess);
      const sess: AuthSessionData = { ...baseSess, token };
      authDb.sessions[token] = sess;
      persistAuthDb();

      recordAuditLog(
        'SHOP_OWNER_LOGIN',
        ownerUser.username || ownerUser.email || ownerUser.id,
        'SHOP_OWNER',
        `Shop Owner signed into ${shop.shopName} (${shop.id})`,
        shop.id,
        req.ip
      );

      return res.json({
        success: true,
        token,
        expiresAt: new Date(sess.expiresAt).toISOString(),
        policyVersion: AUTH_POLICY_VERSION,
        user: sanitizeAuthUser(sess),
        shop: { ...shop, ownerUsername: ownerUser.username || shop.ownerUsername },
      });
    }

    for (const shop of subscriptionsDb.shops) {
      const shopStore = await ensureShopStoreHydratedFromKv(shop.id);
      const staff = (shopStore.staffList || []).find(
        (st) =>
          st.username &&
          st.username.trim().toLowerCase() === lowerIdent &&
          (st as any).passwordHash &&
          (st as any).passwordSalt
      ) as (StaffMember & { passwordSalt?: string }) | undefined;

      if (staff) {
        if (!verifyPasswordRecord(cleanPassword, staff.passwordSalt || '', staff.passwordHash || '')) {
          recordFailedAttempt(rateKey);
          return res.status(401).json({
            success: false,
            error: 'INVALID_PASSWORD',
            message: 'Incorrect Staff Password. Please verify your password and try again.',
          });
        }
        if (staff.accountStatus !== 'active') {
          return res.status(403).json({
            success: false,
            error: 'STAFF_ACCOUNT_RESTRICTED',
            message: `Staff account "${staff.name}" is ${staff.accountStatus}. Contact your Shop Owner.`,
          });
        }
        if (shop.status === 'inactive' || shop.status === 'suspended' || shop.status === 'blocked' || shop.status === 'expired') {
          return res.status(403).json({
            success: false,
            error: 'SHOP_ACCOUNT_RESTRICTED',
            status: shop.status,
            shopName: shop.shopName,
            whatsapp: '03228570782',
            message: `This mart account ("${shop.shopName}") is currently ${shop.status.toUpperCase()}. Contact Super Admin (03228570782).`,
          });
        }

        if (isSharedKvEnabled && !sharedKvSessionSecret) {
          await ensureSharedKvSessionSecret();
        }

        clearFailedAttempt(rateKey);
        const now = Date.now();
        const perms = normalizeStaffPermissions(staff.permissions);
        const baseSess = {
          userId: staff.id,
          username: staff.username,
          fullName: staff.name,
          role: 'STAFF' as UserRole,
          shopId: shop.id,
          staffId: staff.id,
          permissions: perms,
          createdAt: now,
          expiresAt: now + 14 * 24 * 60 * 60 * 1000,
          ip: req.ip,
          policyVersion: AUTH_POLICY_VERSION,
        };
        const token = createSignedSessionToken('STF', baseSess);
        const sess: AuthSessionData = { ...baseSess, token };
        authDb.sessions[token] = sess;
        persistAuthDb();

        recordAuditLog(
          'STAFF_LOGIN',
          staff.username,
          'STAFF',
          `Staff "${staff.name}" (${staff.role}) signed into ${shop.shopName}`,
          shop.id,
          req.ip
        );

        return res.json({
          success: true,
          token,
          expiresAt: new Date(sess.expiresAt).toISOString(),
          policyVersion: AUTH_POLICY_VERSION,
          user: sanitizeAuthUser(sess),
          shop,
        });
      }
    }

    recordFailedAttempt(rateKey);
    return res.status(401).json({
      success: false,
      error: 'ACCOUNT_NOT_FOUND',
      message: 'No Mart or Staff account found matching that Shop ID or Shop Username.',
    });
  }));

  app.get('/api/auth/me', asyncHandler(async (req: Request, res: Response) => {
    const { session, ephemeralContainerMismatch } = await getSessionFromReqAsync(req);
    if (!session) {
      if (ephemeralContainerMismatch) {
        return res.status(401).json({
          success: false,
          error: 'SERVERLESS_SESSION_SECRET_MISSING',
          message:
            'Session could not be verified across Vercel serverless instances because SESSION_SECRET (or Vercel KV) is missing.',
        });
      }
      return res.status(401).json({ success: false, error: 'SESSION_INVALID', message: 'Not authenticated.' });
    }
    if (session.role === 'SUPER_ADMIN') {
      if (!authDb.superAdmin) {
        return res.status(401).json({ success: false, message: 'Super Admin not configured.' });
      }
      return res.json({
        success: true,
        policyVersion: AUTH_POLICY_VERSION,
        user: sanitizeAuthUser(session),
        expiresAt: new Date(session.expiresAt).toISOString(),
      });
    }

    checkAndAutoExpireSubscriptions();
    let shop = subscriptionsDb.shops.find((s) => s.id === session.shopId);
    if (!shop && isSharedKvEnabled) {
      await hydrateSharedKvIfConfigured(true);
      shop = subscriptionsDb.shops.find((s) => s.id === session.shopId);
    }
    if (!shop || shop.status === 'inactive' || shop.status === 'suspended' || shop.status === 'blocked' || shop.status === 'expired') {
      return res.status(403).json({
        success: false,
        error: 'SHOP_ACCOUNT_RESTRICTED',
        status: shop?.status || 'inactive',
        message: shop
          ? `Shop "${shop.shopName}" is ${shop.status.toUpperCase()}. Contact Super Admin (03228570782).`
          : 'Shop account removed.',
      });
    }

    if (session.role === 'STAFF' && session.staffId) {
      const store = await ensureShopStoreHydratedFromKv(shop.id);
      const staff = (store.staffList || []).find((st) => st.id === session.staffId);
      if (!staff || staff.accountStatus !== 'active') {
        return res.status(403).json({ success: false, error: 'STAFF_ACCOUNT_RESTRICTED', message: 'Staff account is inactive.' });
      }
    }

    return res.json({
      success: true,
      policyVersion: AUTH_POLICY_VERSION,
      user: sanitizeAuthUser(session),
      shop,
      expiresAt: new Date(session.expiresAt).toISOString(),
    });
  }));

  app.post('/api/auth/logout', asyncHandler(async (req: Request, res: Response) => {
    const token = extractSessionTokenFromReq(req);
    if (token) {
      if (isSharedKvEnabled) {
        await hydrateSharedKvIfConfigured(true);
      }
      if (authDb.sessions[token]) {
        delete authDb.sessions[token];
      }
      const verified = verifySignedSessionToken(token);
      if (verified?.jti) {
        authDb.revokedTokens = authDb.revokedTokens || {};
        authDb.revokedTokens[verified.jti] = verified.expiresAt;
      }
      persistAuthDb();
    }
    return res.json({ success: true, message: 'Signed out cleanly.' });
  }));

  app.post('/api/auth/change-password', asyncHandler(async (req: Request, res: Response) => {
    const { session } = await getSessionFromReqAsync(req);
    if (!session) {
      return res.status(401).json({ success: false, message: 'Authentication required.' });
    }
    const { currentPassword, newPassword } = req.body || {};
    const cleanCurr = String(currentPassword || '').trim();
    const cleanNew = String(newPassword || '').trim();
    if (!cleanCurr || !cleanNew || cleanNew.length < 6) {
      return res.status(400).json({ success: false, message: 'Current password and new password (min 6 chars) are required.' });
    }

    if (session.role === 'SUPER_ADMIN' && authDb.superAdmin) {
      if (cleanNew.length < 8) {
        return res.status(400).json({ success: false, message: 'Super Admin password must be at least 8 characters.' });
      }
      if (!verifyPasswordRecord(cleanCurr, authDb.superAdmin.passwordSalt, authDb.superAdmin.passwordHash)) {
        return res.status(401).json({ success: false, message: 'Current password is incorrect.' });
      }
      const salt = crypto.randomBytes(16).toString('hex');
      authDb.superAdmin.passwordSalt = salt;
      authDb.superAdmin.passwordHash = hashPassword(cleanNew, salt);
      persistAuthDb();
      recordAuditLog('SUPER_ADMIN_PASSWORD_CHANGED', authDb.superAdmin.gmail, 'SUPER_ADMIN', 'Super Admin changed password.', undefined, req.ip);
      return res.json({ success: true, message: 'Super Admin password updated.' });
    }

    const user = authDb.users.find((u) => u.id === session.userId);
    if (!user) {
      return res.status(404).json({ success: false, message: 'Account not found.' });
    }
    if (!verifyPassword(cleanCurr, user)) {
      return res.status(401).json({ success: false, message: 'Current password is incorrect.' });
    }
    const salt = crypto.randomBytes(16).toString('hex');
    user.passwordSalt = salt;
    user.passwordHash = hashPassword(cleanNew, salt);
    user.mustChangePasswordOnFirstLogin = false;
    persistAuthDb();

    recordAuditLog('PASSWORD_CHANGED', user.username || user.email || user.id, session.role, 'Account password updated.', user.shopId, req.ip);
    return res.json({ success: true, message: 'Password updated successfully.' });
  }));

  // -------------------------------------------------------------
  // 2. DEVICE PAIRING & QR CODE MANAGEMENT (PER-SHOP SCOPED)
  // -------------------------------------------------------------
  app.post('/api/devices/pairing-session', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const shopStore = await ensureShopStoreHydratedFromKv(actor.shopId);
    const pairingToken = crypto.randomBytes(16).toString('hex');
    const manualCode = `WM-${Math.floor(1000 + Math.random() * 9000)}`;
    const createdAt = Date.now();
    const expiresAt = createdAt + 5 * 60 * 1000;

    const session: PairingSession = {
      pairingToken,
      manualCode,
      shopId: actor.shopId,
      createdAt,
      expiresAt,
      status: 'pending_scan',
    };

    await savePairingSessionShared(session);

    return res.json({
      success: true,
      pairingToken,
      manualCode,
      shopId: actor.shopId,
      expiresAt,
      storeName: shopStore.branding.storeName || 'WAQAS MART',
      subtitle: shopStore.branding.subtitle || 'Supermarket Management',
    });
  }));

  app.get('/api/devices/pairing-status/:token', async (req: Request, res: Response) => {
    const { token } = req.params;
    const session = await findPairingSessionShared(token);

    if (!session) {
      return res.status(404).json({
        success: false,
        error: 'PAIRING_SESSION_NOT_FOUND',
        message: 'Pairing session not found or expired.',
      });
    }

    if (Date.now() > session.expiresAt && session.status !== 'approved') {
      session.status = 'expired';
      await savePairingSessionShared(session);
      return res.json({ success: true, session });
    }

    let authPayload = null;
    if (session.status === 'approved' && session.approvedDeviceId) {
      const targetShopId = session.shopId || 'SHOP-WM-001';
      let approvedDev = registeredDevices.find((d) => d.deviceId === session.approvedDeviceId);
      if (!approvedDev && isSharedKvEnabled) {
        await hydrateSharedKvIfConfigured(true);
        approvedDev = registeredDevices.find((d) => d.deviceId === session.approvedDeviceId);
      }
      const shopStore = await ensureShopStoreHydratedFromKv(targetShopId);
      const shop = subscriptionsDb.shops.find((s) => s.id === targetShopId);
      authPayload = {
        sessionToken: approvedDev?.sessionToken,
        device: approvedDev,
        shopId: targetShopId,
        shop,
        user: shopStore.userProfile,
        storeState: sanitizeStoreForClient(shopStore, 'SHOP_OWNER'),
      };
    }

    return res.json({
      success: true,
      session,
      auth: authPayload,
    });
  });

  app.post('/api/devices/pair', async (req: Request, res: Response) => {
    const { tokenOrCode, device } = req.body || {};
    const cleanTokenOrCode = typeof tokenOrCode === 'string' ? tokenOrCode.trim() : '';
    if (!cleanTokenOrCode || !device?.deviceId) {
      return res.status(400).json({ success: false, message: 'Invalid pairing request payload.' });
    }

    const session = await findPairingSessionShared(cleanTokenOrCode);
    if (!session) {
      return res.status(404).json({ success: false, message: 'Invalid or expired QR code.' });
    }
    if (Date.now() > session.expiresAt) {
      session.status = 'expired';
      await savePairingSessionShared(session);
      return res.status(410).json({ success: false, message: 'This QR pairing code has expired.' });
    }
    if (session.status === 'rejected') {
      return res.status(403).json({ success: false, message: 'Pairing was rejected by the shop owner.' });
    }
    if (session.status === 'approved' || session.status === 'cancelled' || session.status === 'expired') {
      return res.status(409).json({
        success: false,
        error: 'PAIRING_CODE_ALREADY_USED',
        message: 'This QR pairing code has already been used or closed. Generate a new QR code from the owner device.',
      });
    }
    if (
      session.status === 'pending_approval' &&
      session.candidateDevice?.deviceId &&
      session.candidateDevice.deviceId !== device.deviceId
    ) {
      return res.status(409).json({
        success: false,
        error: 'PAIRING_SESSION_LOCKED',
        message: 'This QR code is already pending approval for another device.',
      });
    }

    session.candidateDevice = {
      deviceId: device.deviceId,
      deviceName: device.deviceName || 'New Terminal',
      deviceType: device.deviceType || 'Android Mobile',
      browser: device.browser || 'Browser',
      os: device.os || 'OS',
    };
    session.status = 'pending_approval';
    await savePairingSessionShared(session);

    const targetShopId = session.shopId || 'SHOP-WM-001';
    recordAuditLog(
      'DEVICE_PAIR_REQUEST',
      device.deviceId,
      'STAFF',
      `Device pairing requested by "${session.candidateDevice.deviceName}" (${session.candidateDevice.deviceType})`,
      targetShopId,
      req.ip
    );

    broadcastSyncEventToShop(targetShopId, {
      type: 'device:pairing_request',
      timestamp: Date.now(),
      payload: {
        pairingToken: session.pairingToken,
        candidateDevice: session.candidateDevice,
      },
    });

    return res.json({
      success: true,
      message: 'Pairing request submitted. Waiting for shop owner approval.',
      session,
    });
  });

  app.post('/api/devices/approve-pairing', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }

    const { pairingToken, approve } = req.body || {};
    const session = pairingToken ? await findPairingSessionShared(String(pairingToken)) : undefined;
    if (!session) {
      return res.status(404).json({ success: false, message: 'Pairing session not found.' });
    }

    const sessionShopId = session.shopId || 'SHOP-WM-001';
    if (actor.role !== 'SUPER_ADMIN' && actor.shopId !== sessionShopId) {
      return res.status(403).json({ success: false, message: 'Cannot approve pairing session belonging to another shop.' });
    }
    if (session.status === 'approved') {
      return res.status(409).json({ success: false, message: 'This pairing code has already been approved and consumed.' });
    }

    if (!approve) {
      session.status = 'rejected';
      await savePairingSessionShared(session);
      recordAuditLog(
        'DEVICE_PAIR_REJECTED',
        actor.actorEmail,
        actor.role,
        `Rejected pairing request for ${session.candidateDevice?.deviceName || session.pairingToken}`,
        sessionShopId,
        req.ip
      );
      broadcastSyncEventToShop(sessionShopId, {
        type: 'device:updated',
        timestamp: Date.now(),
        payload: { pairingToken, status: 'rejected' },
      });
      return res.json({ success: true, message: 'Device pairing rejected.' });
    }

    if (!session.candidateDevice) {
      return res.status(400).json({ success: false, message: 'No candidate device scanned this code.' });
    }

    const sessionToken = `ST-${crypto.randomBytes(16).toString('hex')}`;
    const newDev: DeviceInfo = {
      deviceId: session.candidateDevice.deviceId,
      sessionToken,
      shopId: sessionShopId,
      deviceName: session.candidateDevice.deviceName,
      deviceType: session.candidateDevice.deviceType,
      browser: session.candidateDevice.browser,
      os: session.candidateDevice.os,
      isCurrentDevice: false,
      firstRegistered: new Date().toLocaleDateString('en-GB'),
      lastActive: new Date().toLocaleTimeString('en-PK', { hour: '2-digit', minute: '2-digit' }),
      status: 'active',
      connectionState: 'online',
      ipAddress: req.ip,
    };

    removedDeviceIds.delete(newDev.deviceId);
    registeredDevices = registeredDevices.filter((d) => d.deviceId !== newDev.deviceId);
    registeredDevices.push(newDev);
    persistDevices();

    session.status = 'approved';
    session.approvedDeviceId = newDev.deviceId;
    await savePairingSessionShared(session);

    recordAuditLog(
      'DEVICE_PAIR_APPROVED',
      actor.actorEmail,
      actor.role,
      `Approved device "${newDev.deviceName}" (${newDev.deviceId})`,
      sessionShopId,
      req.ip
    );

    const shopDevices = registeredDevices.filter((d) => d.shopId === sessionShopId).map((d) => ({ ...d, sessionToken: undefined }));
    broadcastSyncEventToShop(sessionShopId, {
      type: 'device:approved',
      timestamp: Date.now(),
      payload: { pairingToken, device: { ...newDev, sessionToken: undefined } },
    });
    broadcastSyncEventToShop(sessionShopId, {
      type: 'device:updated',
      timestamp: Date.now(),
      payload: shopDevices,
    });

    return res.json({
      success: true,
      message: 'Device successfully authorized!',
      device: newDev,
    });
  }));

  app.post('/api/devices/register-primary', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }

    const dev = req.body?.device;
    if (!dev?.deviceId) {
      return res.status(400).json({ success: false, message: 'Device ID is required.' });
    }
    if (removedDeviceIds.has(dev.deviceId)) {
      return res.status(403).json({
        success: false,
        error: 'DEVICE_REMOVED',
        message: 'This device was removed by administrator. Re-pair via QR code.',
      });
    }

    const existing = registeredDevices.find((d) => d.deviceId === dev.deviceId);
    if (existing) {
      if (existing.status === 'revoked') {
        return res.status(403).json({
          success: false,
          error: 'DEVICE_REVOKED',
          message: 'This device is revoked.',
        });
      }
      if (!existing.sessionToken) {
        existing.sessionToken = `ST-${crypto.randomBytes(16).toString('hex')}`;
      }
      existing.shopId = actor.shopId;
      existing.lastActive = new Date().toLocaleTimeString('en-PK', { hour: '2-digit', minute: '2-digit' });
      existing.connectionState = 'online';
      persistDevices();
      return res.json({ success: true, device: existing, sessionToken: existing.sessionToken });
    }

    const sessionToken = `ST-${crypto.randomBytes(16).toString('hex')}`;
    const primaryDev: DeviceInfo = {
      deviceId: dev.deviceId,
      sessionToken,
      shopId: actor.shopId,
      deviceName: dev.deviceName || 'Primary Terminal',
      deviceType: dev.deviceType || 'Web Browser',
      browser: dev.browser || 'Browser',
      os: dev.os || 'OS',
      isCurrentDevice: false,
      firstRegistered: new Date().toLocaleDateString('en-GB'),
      lastActive: new Date().toLocaleTimeString('en-PK', { hour: '2-digit', minute: '2-digit' }),
      status: 'active',
      connectionState: 'online',
      ipAddress: req.ip,
    };
    registeredDevices.push(primaryDev);
    persistDevices();
    recordAuditLog(
      'PRIMARY_DEVICE_REGISTERED',
      actor.actorEmail,
      actor.role,
      `Registered terminal "${primaryDev.deviceName}" (${primaryDev.deviceId})`,
      actor.shopId,
      req.ip
    );
    broadcastSyncEventToShop(actor.shopId, {
      type: 'device:updated',
      timestamp: Date.now(),
      payload: registeredDevices.filter((d) => d.shopId === actor.shopId).map((d) => ({ ...d, sessionToken: undefined })),
    });
    return res.json({ success: true, device: primaryDev, sessionToken });
  }));

  app.post('/api/devices/cancel-pairing', asyncHandler(async (req: Request, res: Response) => {
    const { pairingToken } = req.body || {};
    if (pairingToken) {
      const s = await findPairingSessionShared(String(pairingToken));
      if (s) {
        s.status = 'cancelled';
        await deletePairingSessionShared(s);
      }
    }
    return res.json({ success: true, message: 'Pairing cancelled.' });
  }));

  // -------------------------------------------------------------
  // 3. DEVICE MANAGEMENT ENDPOINTS (SHOP-SCOPED OR SUPER ADMIN)
  // -------------------------------------------------------------
  app.get('/api/devices', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const visible = registeredDevices.filter((d) => (d.shopId || 'SHOP-WM-001') === actor.shopId);

    const list = visible.map((d) => {
      const hasLiveSse = Array.from(sseClients.values()).some((c) => c.deviceId === d.deviceId);
      const sId = d.shopId || 'SHOP-WM-001';
      const shop = subscriptionsDb.shops.find((s) => s.id === sId);
      return {
        ...d,
        shopName: sId === SUPER_ADMIN_POS_SHOP_ID ? 'SUPER ADMIN · MY POS APP' : shop?.shopName || 'WAQAS MART',
        sessionToken: undefined,
        connectionState: hasLiveSse ? ('online' as const) : ('offline' as const),
      };
    });
    return res.json({ success: true, devices: list });
  }));

  app.post('/api/devices/:id/rename', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const { id } = req.params;
    const { name } = req.body || {};
    const target = registeredDevices.find((d) => d.deviceId === id);
    if (!target) {
      return res.status(404).json({ success: false, message: 'Device not found.' });
    }
    if (actor.role !== 'SUPER_ADMIN' && (target.shopId || 'SHOP-WM-001') !== actor.shopId) {
      return res.status(403).json({ success: false, message: 'Cannot rename device belonging to another shop.' });
    }
    target.deviceName = name || target.deviceName;
    persistDevices();
    const targetShopId = target.shopId || 'SHOP-WM-001';
    recordAuditLog('DEVICE_RENAMED', actor.actorEmail, actor.role, `Renamed device ${id} to "${target.deviceName}"`, targetShopId, req.ip);

    broadcastSyncEventToShop(targetShopId, {
      type: 'device:updated',
      timestamp: Date.now(),
      payload: registeredDevices.filter((d) => d.shopId === targetShopId).map((d) => ({ ...d, sessionToken: undefined })),
    });

    return res.json({ success: true, device: { ...target, sessionToken: undefined } });
  }));

  app.post('/api/devices/:id/revoke', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const { id } = req.params;
    const target = registeredDevices.find((d) => d.deviceId === id);
    if (!target) {
      return res.status(404).json({ success: false, message: 'Device not found.' });
    }
    if (actor.role !== 'SUPER_ADMIN' && (target.shopId || 'SHOP-WM-001') !== actor.shopId) {
      return res.status(403).json({ success: false, message: 'Cannot revoke device belonging to another shop.' });
    }

    target.status = 'revoked';
    target.connectionState = 'offline';
    persistDevices();

    const targetShopId = target.shopId || 'SHOP-WM-001';
    recordAuditLog('DEVICE_REVOKED', actor.actorEmail, actor.role, `Revoked access for device "${target.deviceName}" (${id})`, targetShopId, req.ip);

    broadcastSyncEventToShop(targetShopId, {
      type: 'device:revoked',
      sourceDeviceId: id,
      timestamp: Date.now(),
      payload: { deviceId: id },
    });

    for (const [cid, client] of sseClients.entries()) {
      if (client.deviceId === id) {
        try { client.res.end(); } catch {}
        sseClients.delete(cid);
      }
    }

    broadcastSyncEventToShop(targetShopId, {
      type: 'device:updated',
      timestamp: Date.now(),
      payload: registeredDevices.filter((d) => d.shopId === targetShopId).map((d) => ({ ...d, sessionToken: undefined })),
    });

    return res.json({ success: true, message: 'Device access revoked successfully.' });
  }));

  app.post('/api/devices/:id/restore', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const { id } = req.params;
    const target = registeredDevices.find((d) => d.deviceId === id);
    if (!target) {
      return res.status(404).json({ success: false, message: 'Device not found.' });
    }
    if (actor.role !== 'SUPER_ADMIN' && (target.shopId || 'SHOP-WM-001') !== actor.shopId) {
      return res.status(403).json({ success: false, message: 'Cannot restore device belonging to another shop.' });
    }
    target.status = 'active';
    removedDeviceIds.delete(id);
    persistDevices();
    const targetShopId = target.shopId || 'SHOP-WM-001';
    recordAuditLog('DEVICE_RESTORED', actor.actorEmail, actor.role, `Restored access for device "${target.deviceName}" (${id})`, targetShopId, req.ip);
    broadcastSyncEventToShop(targetShopId, {
      type: 'device:updated',
      timestamp: Date.now(),
      payload: registeredDevices.filter((d) => d.shopId === targetShopId).map((d) => ({ ...d, sessionToken: undefined })),
    });
    return res.json({ success: true, message: 'Device access restored.', device: { ...target, sessionToken: undefined } });
  }));

  app.get('/api/devices/check-status', asyncHandler(async (req: Request, res: Response) => {
    const deviceId =
      (req.headers['x-device-id'] as string) ||
      (req.query.deviceId as string) ||
      '';
    const deviceToken =
      (req.headers['x-device-token'] as string) ||
      (req.query.deviceToken as string) ||
      '';

    if (!deviceId || !deviceToken) {
      return res.status(401).json({
        success: false,
        allowed: false,
        error: 'AUTH_REQUIRED',
        message: 'Device credentials missing. Please scan a new QR code.',
      });
    }

    let devCheck = verifyDeviceAccess(deviceId, deviceToken);
    if (
      !devCheck.allowed &&
      isSharedKvEnabled &&
      (devCheck.error === 'DEVICE_UNKNOWN' || devCheck.error === 'INVALID_DEVICE_TOKEN')
    ) {
      await hydrateSharedKvIfConfigured(true);
      devCheck = verifyDeviceAccess(deviceId, deviceToken);
    }
    if (!devCheck.allowed || !devCheck.device) {
      return res.status(devCheck.statusCode || 403).json({
        success: false,
        allowed: false,
        error: devCheck.error || 'DEVICE_REVOKED',
        message: devCheck.message || 'Device access is still restricted.',
      });
    }

    const shopId = devCheck.device.shopId || 'SHOP-WM-001';
    const shop = subscriptionsDb.shops.find((s) => s.id === shopId);
    if (shop && (shop.status === 'inactive' || shop.status === 'suspended' || shop.status === 'blocked' || shop.status === 'expired')) {
      return res.status(403).json({
        success: false,
        allowed: false,
        error: 'SHOP_ACCOUNT_RESTRICTED',
        message: `Shop "${shop.shopName}" is ${shop.status.toUpperCase()}. Terminal access locked.`,
      });
    }

    return res.json({
      success: true,
      allowed: true,
      device: { ...devCheck.device, sessionToken: undefined },
    });
  }));

  app.delete('/api/devices/:id', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const { id } = req.params;
    const target = registeredDevices.find((d) => d.deviceId === id);
    if (!target) {
      return res.status(404).json({ success: false, message: 'Device not found.' });
    }
    if (actor.role !== 'SUPER_ADMIN' && (target.shopId || 'SHOP-WM-001') !== actor.shopId) {
      return res.status(403).json({ success: false, message: 'Cannot remove device belonging to another shop.' });
    }

    const targetShopId = target.shopId || 'SHOP-WM-001';
    registeredDevices = registeredDevices.filter((d) => d.deviceId !== id);
    removedDeviceIds.add(id);
    persistDevices();

    recordAuditLog('DEVICE_REMOVED', actor.actorEmail, actor.role, `Permanently removed device "${target.deviceName}" (${id})`, targetShopId, req.ip);

    broadcastSyncEventToShop(targetShopId, {
      type: 'device:revoked',
      timestamp: Date.now(),
      payload: { deviceId: id },
    });

    for (const [cid, client] of sseClients.entries()) {
      if (client.deviceId === id) {
        try { client.res.end(); } catch {}
        sseClients.delete(cid);
      }
    }

    broadcastSyncEventToShop(targetShopId, {
      type: 'device:updated',
      timestamp: Date.now(),
      payload: registeredDevices.filter((d) => d.shopId === targetShopId).map((d) => ({ ...d, sessionToken: undefined })),
    });

    return res.json({ success: true, message: 'Device removed.' });
  }));

  // -------------------------------------------------------------
  // 4. REAL-TIME SERVER-SENT EVENTS (SSE) STREAM (PER-SHOP SCOPED)
  // -------------------------------------------------------------
  app.get('/api/sync/events', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const deviceId = (req.query.deviceId as string) || (req.headers['x-device-id'] as string) || 'UNKNOWN-DEV';
    const shopStore = await ensureShopStoreHydratedFromKv(actor.shopId);

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders?.();

    if (actor.device) {
      actor.device.lastActive = new Date().toLocaleTimeString('en-PK', { hour: '2-digit', minute: '2-digit' });
      actor.device.connectionState = 'online';
    }

    // On Vercel Serverless Functions, close gracefully after sending initial ping
    // so client switches cleanly to adaptive polling instead of hitting 504 timeout.
    if (isServerlessRuntime) {
      res.write('retry: 10000\n');
      res.write(
        `data: ${JSON.stringify({
          type: 'ping',
          mode: 'serverless',
          timestamp: Date.now(),
          shopId: actor.shopId,
          syncVersion: shopStore.syncVersion || 1,
          message: 'Connected to WAQAS POS Serverless Sync',
        })}\n\n`
      );
      return res.end();
    }

    const clientId = `CLIENT-${crypto.randomBytes(8).toString('hex')}`;
    const client: SseClient = {
      id: clientId,
      res,
      deviceId,
      shopId: actor.shopId,
      role: actor.role,
      canViewCostAndProfit: actor.role !== 'STAFF' || Boolean(actor.permissions?.canViewCostAndProfit),
      ip: req.ip || '',
      connectedAt: Date.now(),
    };

    sseClients.set(clientId, client);

    res.write(
      `data: ${JSON.stringify({
        type: 'ping',
        timestamp: Date.now(),
        shopId: actor.shopId,
        syncVersion: shopStore.syncVersion || 1,
        message: 'Connected to WAQAS POS Live Sync Engine',
      })}\n\n`
    );

    const shopDevices = registeredDevices.filter((d) => d.shopId === actor.shopId).map((d) => ({ ...d, sessionToken: undefined }));
    broadcastSyncEventToShop(actor.shopId, {
      type: 'device:updated',
      timestamp: Date.now(),
      payload: shopDevices,
    });

    req.on('close', () => {
      sseClients.delete(clientId);
      const remaining = registeredDevices.filter((d) => d.shopId === actor.shopId).map((d) => ({ ...d, sessionToken: undefined }));
      broadcastSyncEventToShop(actor.shopId, {
        type: 'device:updated',
        timestamp: Date.now(),
        payload: remaining,
      });
    });
  }));

  // -------------------------------------------------------------
  // 5. AUTHORITATIVE PER-SHOP STORE STATE & ATOMIC MUTATIONS
  // -------------------------------------------------------------
  app.get('/api/store/state', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const shopStore = await ensureShopStoreHydratedFromKv(actor.shopId);
    const safeStore = sanitizeStoreForClient(shopStore, actor.role, actor.permissions);
    return res.json({
      success: true,
      shopId: actor.shopId,
      syncVersion: shopStore.syncVersion || 1,
      store: safeStore,
    });
  }));

  function canStaffPerformMutation(action: string, perms?: StaffModulePermissions): boolean {
    if (!perms) return false;
    switch (action) {
      case 'create_sale':
      case 'update_held_bills':
      case 'delete_held_bill':
      case 'delete_all_held_bills':
        return perms.newSale || perms.oldSale;
      case 'process_return':
        return perms.newSale || perms.oldSale || perms.customers;
      case 'create_purchase':
      case 'update_purchase':
      case 'delete_purchase':
      case 'delete_all_purchases':
      case 'sync_purchase_returns':
      case 'create_purchase_return':
        return perms.newPurchase || perms.oldPurchase;
      case 'update_stock':
      case 'reconcile_negative_stock':
      case 'sync_stock_history':
      case 'sync_categories':
        return perms.stock || perms.products;
      case 'update_customer_ledger':
        return perms.customers || perms.canAccessLedgers;
      case 'sync_supplier_ledgers':
        return perms.suppliers || perms.canAccessLedgers;
      case 'sync_expenses':
        return perms.expenses;
      case 'sync_daily_hisab':
        return perms.reports || perms.canAccessReports;
      case 'sync_printer_settings':
      case 'sync_receipt_settings':
        return perms.printing || perms.settings;
      case 'sync_staff':
      case 'update_branding':
        return perms.settings || perms.canAccessSettings;
      case 'sync_full':
        return true;
      case 'reset_all_zero':
      case 'load_seed_catalog':
        return false;
      default:
        return false;
    }
  }

  app.post('/api/store/sync', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({
        success: false,
        error: actor.error,
        message: actor.message,
      });
    }

    const { action, payload, sourceDeviceId, baseSyncVersion } = req.body || {};
    if (actor.role === 'STAFF' && !canStaffPerformMutation(action, actor.permissions)) {
      return res.status(403).json({
        success: false,
        error: 'PERMISSION_DENIED',
        message: `Your staff account does not have permission for "${action}".`,
      });
    }

    if (actor.device) {
      actor.device.lastActive = new Date().toLocaleTimeString('en-PK', { hour: '2-digit', minute: '2-digit' });
      actor.device.connectionState = 'online';
    }

    const targetShopId = actor.shopId;
    const shopStore = await ensureShopStoreHydratedFromKv(targetShopId);

    switch (action) {
      case 'create_sale': {
        const tx: SaleTransaction = payload;
        if (!tx || !tx.invoiceNumber) {
          return res.status(400).json({ success: false, message: 'Invalid transaction payload.' });
        }

        const exists = shopStore.transactions.some((t) => t.invoiceNumber === tx.invoiceNumber);
        if (!exists) {
          if (tx.mode === 'sale') {
            const stockCheck = validateSaleItemsStock(shopStore.products, tx.items || []);
            if (!stockCheck.ok) {
              return res.status(400).json({
                success: false,
                error: stockCheck.code,
                message: stockCheck.message,
              });
            }
          }

          const enrichedTx: SaleTransaction = {
            ...tx,
            items: (tx.items || []).map((it) => {
              const srvProd = findLiveProduct(shopStore.products, {
                productId: it.product?.id,
                barcode: it.product?.barcode,
              });
              return srvProd ? { ...it, product: { ...it.product, costPrice: srvProd.costPrice } } : it;
            }),
          };

          shopStore.transactions.unshift(enrichedTx);

          if (tx.mode === 'sale') {
            (tx.items || []).forEach((item) => {
              const product = findLiveProduct(shopStore.products, {
                productId: item.product?.id,
                barcode: item.product?.barcode,
              });
              if (product) {
                const reqQty = Number(item.quantity) || 0;
                const prevStock = roundStockQty(Number(product.stock) || 0);
                const nextStock = roundStockQty(prevStock - reqQty);
                product.stock = nextStock;
                recordShopStockTx(
                  shopStore,
                  product,
                  prevStock,
                  -reqQty,
                  nextStock,
                  'Sale',
                  tx.invoiceNumber,
                  tx.cashierName || actor.actorEmail
                );
              }
            });
          }

          if (tx.paymentMethod === 'credit' && (tx.customerId || tx.customerName)) {
            const advancePaid = Number(tx.cashTendered) || 0;
            const unpaidCredit = Math.max(0, (Number(tx.grandTotal) || 0) - advancePaid);
            const alreadyRecorded = shopStore.customerLedgers.some(
              (l) => l.invoiceNumber === tx.invoiceNumber && l.credit > 0
            );
            if (unpaidCredit > 0 && !alreadyRecorded) {
              const entry: CustomerLedgerEntry = {
                id: `LED-SALE-${tx.invoiceNumber}`,
                customerId: tx.customerId || 'CUST-GENERAL',
                date: tx.dateTime,
                details: `Sales Invoice #${tx.invoiceNumber}`,
                invoiceNumber: tx.invoiceNumber,
                credit: unpaidCredit,
                debit: 0,
                runningBalance: 0,
                paymentMethod: 'Credit / Pay Later',
                createdAt: new Date().toISOString(),
              };
              shopStore.customerLedgers.push(entry);
            }
          }

          persistShopStore(targetShopId);

          broadcastSyncEventToShop(targetShopId, {
            type: 'sale:completed',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: {
              transaction: sanitizeStoreForClient(shopStore, actor.role, actor.permissions).transactions[0],
              updatedProducts: shopStore.products,
              customerLedgers: shopStore.customerLedgers,
              stockHistory: shopStore.stockHistory,
              syncVersion: shopStore.syncVersion,
            },
          });
        }

        const safeStore = sanitizeStoreForClient(shopStore, actor.role, actor.permissions);
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: safeStore });
      }

      case 'create_purchase': {
        const purchase: PurchaseTransaction = payload;
        if (!purchase) {
          return res.status(400).json({ success: false, message: 'Invalid purchase payload.' });
        }

        const reconciled = applyPurchaseSaveToState({
          purchase,
          isEdit: false,
          products: shopStore.products,
          suppliers: shopStore.suppliers,
          purchases: shopStore.purchases,
          userName: purchase.userName || actor.actorEmail,
          timestampStr: formatStockAuditTimestamp(),
        });

        if (reconciled.stockError) {
          return res.status(400).json({
            success: false,
            error: reconciled.stockError.code,
            message: reconciled.stockError.message,
          });
        }

        if (!reconciled.duplicateBlocked) {
          shopStore.products = reconciled.products;
          shopStore.suppliers = reconciled.suppliers;
          shopStore.purchases = reconciled.purchases;
          shopStore.stockHistory = [...reconciled.stockEntries, ...(shopStore.stockHistory || [])].slice(0, 5000);

          persistShopStore(targetShopId);

          broadcastSyncEventToShop(targetShopId, {
            type: 'purchase:added',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: {
              purchase,
              updatedProducts: shopStore.products,
              suppliers: shopStore.suppliers,
              stockHistory: shopStore.stockHistory,
              syncVersion: shopStore.syncVersion,
            },
          });
        }

        const safeStore = sanitizeStoreForClient(shopStore, actor.role, actor.permissions);
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: safeStore });
      }

      case 'update_purchase': {
        const purchase: PurchaseTransaction = payload;
        if (!purchase || !purchase.id) {
          return res.status(400).json({ success: false, message: 'Invalid purchase update payload.' });
        }

        const existingPurchase = shopStore.purchases.find(
          (p) => p.id === purchase.id || p.purchaseInvoiceNo === purchase.purchaseInvoiceNo
        );
        if (
          existingPurchase &&
          typeof baseSyncVersion === 'number' &&
          existingPurchase.updatedAt &&
          purchase.updatedAt &&
          existingPurchase.updatedAt > purchase.updatedAt
        ) {
          return res.status(409).json({
            success: false,
            error: 'CONFLICT',
            message: `Purchase #${purchase.purchaseInvoiceNo} was updated more recently on another device.`,
            serverVersion: shopStore.syncVersion,
            store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions),
          });
        }

        const reconciled = applyPurchaseSaveToState({
          purchase,
          isEdit: Boolean(existingPurchase),
          products: shopStore.products,
          suppliers: shopStore.suppliers,
          purchases: shopStore.purchases,
          userName: purchase.editBy || purchase.userName || actor.actorEmail,
          timestampStr: formatStockAuditTimestamp(),
        });

        if (reconciled.stockError) {
          return res.status(400).json({
            success: false,
            error: reconciled.stockError.code,
            message: reconciled.stockError.message,
          });
        }

        if (!reconciled.duplicateBlocked) {
          shopStore.products = reconciled.products;
          shopStore.suppliers = reconciled.suppliers;
          shopStore.purchases = reconciled.purchases;
          shopStore.stockHistory = [...reconciled.stockEntries, ...(shopStore.stockHistory || [])].slice(0, 5000);
        }

        persistShopStore(targetShopId);

        broadcastSyncEventToShop(targetShopId, {
          type: 'purchase:updated',
          sourceDeviceId,
          timestamp: Date.now(),
          payload: {
            purchase,
            updatedProducts: shopStore.products,
            suppliers: shopStore.suppliers,
            stockHistory: shopStore.stockHistory,
            syncVersion: shopStore.syncVersion,
          },
        });

        const safeStore = sanitizeStoreForClient(shopStore, actor.role, actor.permissions);
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: safeStore });
      }

      case 'delete_purchase': {
        const { purchaseId } = payload || {};
        if (!purchaseId) {
          return res.status(400).json({ success: false, message: 'purchaseId is required.' });
        }

        const reversed = applyPurchaseDeleteToState({
          purchaseId,
          products: shopStore.products,
          suppliers: shopStore.suppliers,
          purchases: shopStore.purchases,
          userName: actor.actorEmail,
          timestampStr: formatStockAuditTimestamp(),
        });

        if (reversed.stockError) {
          return res.status(400).json({
            success: false,
            error: reversed.stockError.code,
            message: reversed.stockError.message,
          });
        }

        if (reversed.deletedPurchase) {
          shopStore.products = reversed.products;
          shopStore.suppliers = reversed.suppliers;
          shopStore.purchases = reversed.purchases;
          shopStore.stockHistory = [...reversed.stockEntries, ...(shopStore.stockHistory || [])].slice(0, 5000);
          persistShopStore(targetShopId);

          broadcastSyncEventToShop(targetShopId, {
            type: 'purchase:deleted',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: {
              purchaseId: reversed.deletedPurchase.id,
              updatedProducts: shopStore.products,
              suppliers: shopStore.suppliers,
              stockHistory: shopStore.stockHistory,
              syncVersion: shopStore.syncVersion,
            },
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'process_return': {
        const ret: SaleReturn = payload;
        if (!ret) {
          return res.status(400).json({ success: false, message: 'Invalid return payload.' });
        }

        const exists = shopStore.saleReturns.some((r) => r.returnNumber === ret.returnNumber);
        if (!exists) {
          shopStore.saleReturns.unshift(ret);

          ret.items.forEach((item) => {
            if (item.condition === 'resaleable') {
              const product = shopStore.products.find((p) => p.id === item.productId);
              if (product) {
                const prevStock = Number(product.stock) || 0;
                const nextStock = prevStock + item.quantity;
                product.stock = nextStock;
                recordShopStockTx(
                  shopStore,
                  product,
                  prevStock,
                  item.quantity,
                  nextStock,
                  'Sale Return',
                  ret.returnNumber,
                  ret.cashierName || actor.actorEmail
                );
              }
            }
          });

          if (ret.refundMethod === 'credit' && ret.customerId) {
            const entry: CustomerLedgerEntry = {
              id: `LED-RET-${ret.returnNumber}`,
              customerId: ret.customerId,
              date: ret.dateTime,
              details: `Sale Return #${ret.returnNumber}`,
              invoiceNumber: ret.originalInvoiceNumber,
              credit: 0,
              debit: ret.totalRefund,
              runningBalance: 0,
              paymentMethod: 'Credit Adjustment',
              createdAt: new Date().toISOString(),
            };
            if (!shopStore.customerLedgers.some((l) => l.id === entry.id)) {
              shopStore.customerLedgers.push(entry);
            }
          }

          persistShopStore(targetShopId);

          broadcastSyncEventToShop(targetShopId, {
            type: 'return:processed',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: {
              saleReturn: ret,
              updatedProducts: shopStore.products,
              customerLedgers: shopStore.customerLedgers,
              stockHistory: shopStore.stockHistory,
              syncVersion: shopStore.syncVersion,
            },
          });
        }

        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'create_purchase_return': {
        const ret: PurchaseReturn = payload;
        if (!ret || !ret.returnNumber) {
          return res.status(400).json({ success: false, message: 'Invalid purchase return payload.' });
        }
        shopStore.purchaseReturns = shopStore.purchaseReturns || [];
        const exists = shopStore.purchaseReturns.some((r) => r.returnNumber === ret.returnNumber);
        if (!exists) {
          const stockCheck = validatePurchaseReturnStock(shopStore.products, ret.items || []);
          if (!stockCheck.ok) {
            return res.status(400).json({
              success: false,
              error: stockCheck.code,
              message: stockCheck.message,
            });
          }

          shopStore.purchaseReturns.unshift(ret);
          (ret.items || []).forEach((item) => {
            const product = findLiveProduct(shopStore.products, {
              productId: item.productId,
              barcode: item.barcode,
            });
            if (product) {
              const reqQty = Number(item.quantity) || 0;
              const prevStock = roundStockQty(Number(product.stock) || 0);
              const nextStock = roundStockQty(prevStock - reqQty);
              product.stock = nextStock;
              recordShopStockTx(
                shopStore,
                product,
                prevStock,
                -reqQty,
                nextStock,
                'Purchase Return',
                ret.returnNumber,
                actor.actorEmail
              );
            }
          });
          if (ret.refundType === 'reduce_payable' && ret.supplierId) {
            const supplier = shopStore.suppliers.find((s) => s.id === ret.supplierId);
            if (supplier) {
              supplier.payableBalance = Math.max(0, (Number(supplier.payableBalance) || 0) - ret.totalRefund);
            }
          }
          persistShopStore(targetShopId);

          broadcastSyncEventToShop(targetShopId, {
            type: 'purchase_return:processed',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: {
              purchaseReturn: ret,
              updatedProducts: shopStore.products,
              suppliers: shopStore.suppliers,
              stockHistory: shopStore.stockHistory,
              syncVersion: shopStore.syncVersion,
            },
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_staff': {
        if (Array.isArray(payload)) {
          const existingMap = new Map<string, any>();
          for (const st of shopStore.staffList || []) {
            existingMap.set(st.id, st);
          }

          const updatedStaff: StaffMember[] = payload.map((rawSt: any) => {
            const prev = existingMap.get(rawSt.id);
            let passwordSalt = prev?.passwordSalt;
            let passwordHash = prev?.passwordHash;
            const plainNewPass = typeof rawSt.newPassword === 'string' ? rawSt.newPassword.trim() : '';
            if (plainNewPass.length >= 4) {
              passwordSalt = crypto.randomBytes(16).toString('hex');
              passwordHash = hashPassword(plainNewPass, passwordSalt);
            }
            const cleanStaff: any = {
              ...rawSt,
              permissions: normalizeStaffPermissions(rawSt.permissions),
            };
            delete cleanStaff.newPassword;
            if (passwordSalt) cleanStaff.passwordSalt = passwordSalt;
            if (passwordHash) cleanStaff.passwordHash = passwordHash;
            return cleanStaff;
          });

          shopStore.staffList = updatedStaff;
          persistShopStore(targetShopId);

          const safeStaff = updatedStaff.map((st: any) => ({
            ...st,
            passwordHash: undefined,
            passwordSalt: undefined,
          }));
          broadcastSyncEventToShop(targetShopId, {
            type: 'staff:updated',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: safeStaff,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_expenses': {
        if (Array.isArray(payload)) {
          shopStore.expenses = payload;
          persistShopStore(targetShopId);
          broadcastSyncEventToShop(targetShopId, {
            type: 'expenses:updated',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore.expenses,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_supplier_ledgers': {
        const { suppliers: supList, ledgerEntries } = payload || {};
        if (Array.isArray(supList)) shopStore.suppliers = supList;
        if (Array.isArray(ledgerEntries)) shopStore.supplierLedgers = ledgerEntries;
        persistShopStore(targetShopId);
        broadcastSyncEventToShop(targetShopId, {
          type: 'supplier:ledger_updated',
          sourceDeviceId,
          timestamp: Date.now(),
          payload: {
            suppliers: shopStore.suppliers,
            supplierLedgers: shopStore.supplierLedgers,
          },
        });
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_printer_settings': {
        if (payload) {
          shopStore.printerSettings = payload;
          persistShopStore(targetShopId);
          broadcastSyncEventToShop(targetShopId, {
            type: 'printer:updated',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore.printerSettings,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_receipt_settings': {
        if (payload) {
          shopStore.receiptSettings = payload;
          persistShopStore(targetShopId);
          broadcastSyncEventToShop(targetShopId, {
            type: 'receipt:updated',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore.receiptSettings,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_categories': {
        if (Array.isArray(payload)) {
          shopStore.categories = payload;
          persistShopStore(targetShopId);
          broadcastSyncEventToShop(targetShopId, {
            type: 'categories:updated',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore.categories,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_daily_hisab': {
        if (payload && typeof payload === 'object') {
          if (
            typeof baseSyncVersion === 'number' &&
            baseSyncVersion > 0 &&
            (shopStore.syncVersion || 1) > baseSyncVersion &&
            req.body?.enforceConflictCheck === true
          ) {
            return res.status(409).json({
              success: false,
              error: 'CONFLICT',
              message: 'Daily Hisab was modified by another device while offline.',
              serverVersion: shopStore.syncVersion,
              store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions),
            });
          }
          shopStore.dailyHisab = payload;
          persistShopStore(targetShopId);
          broadcastSyncEventToShop(targetShopId, {
            type: 'daily_hisab:updated',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore.dailyHisab,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_stock_history': {
        if (Array.isArray(payload)) {
          shopStore.stockHistory = payload;
          persistShopStore(targetShopId);
          broadcastSyncEventToShop(targetShopId, {
            type: 'store:sync',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'update_stock': {
        const updatedProducts: Product[] = payload;
        if (Array.isArray(updatedProducts)) {
          if (
            typeof baseSyncVersion === 'number' &&
            baseSyncVersion > 0 &&
            (shopStore.syncVersion || 1) > baseSyncVersion &&
            req.body?.enforceConflictCheck === true
          ) {
            return res.status(409).json({
              success: false,
              error: 'CONFLICT',
              message: 'Product catalog changed on server while offline. Review server state before retrying.',
              serverVersion: shopStore.syncVersion,
              store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions),
            });
          }
          const existingById = new Map(shopStore.products.map((p) => [p.id, p]));
          const seenBarcodes = new Set<string>();
          for (const p of updatedProducts) {
            const prev = existingById.get(p.id);
            const nextStk = roundStockQty(Number(p.stock) || 0);
            if (!prev && nextStk < 0) {
              return res.status(400).json({
                success: false,
                error: 'STOCK_ALREADY_ZERO',
                message: STOCK_ALREADY_ZERO_MESSAGE,
              });
            }
            if (prev) {
              const prevStk = roundStockQty(Number(prev.stock) || 0);
              if (nextStk < 0 && nextStk < prevStk) {
                if (prevStk <= 0) {
                  return res.status(400).json({
                    success: false,
                    error: 'STOCK_ALREADY_ZERO',
                    message: STOCK_ALREADY_ZERO_MESSAGE,
                  });
                }
                return res.status(400).json({
                  success: false,
                  error: 'INSUFFICIENT_STOCK',
                  message: buildInsufficientStockMessage(prevStk),
                });
              }
            }

            const b = (p.barcode || '').trim().toLowerCase();
            if (b) {
              if (seenBarcodes.has(b)) {
                return res.status(400).json({
                  success: false,
                  error: 'DUPLICATE_BARCODE',
                  message: `Duplicate barcode "${p.barcode}" is not allowed on product "${p.name}".`,
                });
              }
              seenBarcodes.add(b);
            }
          }

          if (actor.role === 'STAFF' && !actor.permissions?.canViewCostAndProfit) {
            const costById = new Map(shopStore.products.map((p) => [p.id, p.costPrice]));
            shopStore.products = updatedProducts.map((p) => ({
              ...p,
              costPrice: costById.get(p.id) ?? p.costPrice,
            }));
          } else {
            shopStore.products = updatedProducts;
          }

          persistShopStore(targetShopId);

          broadcastSyncEventToShop(targetShopId, {
            type: 'stock:updated',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore.products,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'update_customer_ledger': {
        const { customers, ledgerEntries } = payload || {};
        if (Array.isArray(customers)) shopStore.customers = customers;
        if (Array.isArray(ledgerEntries)) shopStore.customerLedgers = ledgerEntries;
        persistShopStore(targetShopId);

        broadcastSyncEventToShop(targetShopId, {
          type: 'customer:ledger_updated',
          sourceDeviceId,
          timestamp: Date.now(),
          payload: {
            customers: shopStore.customers,
            customerLedgers: shopStore.customerLedgers,
          },
        });
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'update_branding': {
        const branding: StoreBranding = payload;
        if (branding) {
          shopStore.branding = branding;
          persistShopStore(targetShopId);

          broadcastSyncEventToShop(targetShopId, {
            type: 'store:sync',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'update_held_bills': {
        const heldBills: HeldBill[] = payload;
        if (Array.isArray(heldBills)) {
          shopStore.heldBills = heldBills;
          persistShopStore(targetShopId);

          broadcastSyncEventToShop(targetShopId, {
            type: 'store:sync',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore,
          });
        }
        return res.json({ success: true, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'delete_held_bill': {
        const { billId } = payload || {};
        if (billId) {
          shopStore.heldBills = (shopStore.heldBills || []).filter((b) => b.id !== billId);
          persistShopStore(targetShopId);

          broadcastSyncEventToShop(targetShopId, {
            type: 'store:sync',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore,
          });
        }
        return res.json({ success: true, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_purchase_returns': {
        if (Array.isArray(payload)) {
          shopStore.purchaseReturns = payload;
          persistShopStore(targetShopId);
          broadcastSyncEventToShop(targetShopId, {
            type: 'store:sync',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore,
          });
        }
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'delete_all_purchases': {
        const resDelete = applyPurchaseDeleteAllToState({
          products: shopStore.products,
          suppliers: shopStore.suppliers,
          purchases: shopStore.purchases,
          userName: actor.actorEmail,
          timestampStr: formatStockAuditTimestamp(),
        });
        if (!resDelete.ok || resDelete.stockError) {
          return res.status(400).json({
            success: false,
            error: resDelete.stockError?.code || 'INSUFFICIENT_STOCK',
            message: resDelete.stockError?.message || STOCK_ALREADY_ZERO_MESSAGE,
          });
        }
        shopStore.products = resDelete.products;
        shopStore.suppliers = resDelete.suppliers;
        shopStore.purchases = [];
        if (resDelete.stockEntries.length > 0) {
          shopStore.stockHistory = [...resDelete.stockEntries, ...(shopStore.stockHistory || [])].slice(0, 5000);
        }
        persistShopStore(targetShopId);
        broadcastSyncEventToShop(targetShopId, {
          type: 'store:sync',
          sourceDeviceId,
          timestamp: Date.now(),
          payload: shopStore,
        });
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'reconcile_negative_stock': {
        const rec = reconcileNegativeStockFromHistory({
          products: shopStore.products,
          purchases: shopStore.purchases,
          transactions: shopStore.transactions,
          saleReturns: shopStore.saleReturns,
          purchaseReturns: shopStore.purchaseReturns,
          actor: actor.actorEmail,
          timestampStr: formatStockAuditTimestamp(),
        });
        if (rec.reconciledCount > 0) {
          shopStore.products = rec.products;
          if (rec.stockEntries.length > 0) {
            shopStore.stockHistory = [...rec.stockEntries, ...(shopStore.stockHistory || [])].slice(0, 5000);
          }
          recordAuditLog(
            'NEGATIVE_STOCK_RECONCILED',
            actor.actorEmail,
            actor.role,
            `Reconciled ${rec.reconciledCount} negative stock product(s) to 0.`,
            targetShopId,
            req.ip
          );
          persistShopStore(targetShopId);
          broadcastSyncEventToShop(targetShopId, {
            type: 'store:sync',
            sourceDeviceId,
            timestamp: Date.now(),
            payload: shopStore,
          });
        }
        return res.json({
          success: true,
          reconciledCount: rec.reconciledCount,
          syncVersion: shopStore.syncVersion,
          store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions),
        });
      }

      case 'delete_all_held_bills': {
        shopStore.heldBills = [];
        persistShopStore(targetShopId);
        broadcastSyncEventToShop(targetShopId, {
          type: 'store:sync',
          sourceDeviceId,
          timestamp: Date.now(),
          payload: shopStore,
        });
        return res.json({ success: true, syncVersion: shopStore.syncVersion, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'sync_full': {
        const { transactions, purchases, saleReturns, products, customers, customerLedgers } = payload || {};
        if (Array.isArray(transactions)) {
          transactions.forEach((tx) => {
            if (!shopStore.transactions.some((t) => t.invoiceNumber === tx.invoiceNumber)) {
              shopStore.transactions.push(tx);
            }
          });
        }
        if (Array.isArray(purchases)) {
          purchases.forEach((p) => {
            if (!shopStore.purchases.some((x) => x.purchaseInvoiceNo === p.purchaseInvoiceNo)) {
              shopStore.purchases.push(p);
            }
          });
        }
        if (Array.isArray(saleReturns)) {
          saleReturns.forEach((r) => {
            if (!shopStore.saleReturns.some((x) => x.returnNumber === r.returnNumber)) {
              shopStore.saleReturns.push(r);
            }
          });
        }
        if (Array.isArray(products) && products.length > 0 && shopStore.products.length === 0) {
          shopStore.products = products;
        }
        if (Array.isArray(customers) && customers.length > 0 && shopStore.customers.length === 0) {
          shopStore.customers = customers;
        }
        if (Array.isArray(customerLedgers)) {
          customerLedgers.forEach((l) => {
            if (!shopStore.customerLedgers.some((x) => x.id === l.id)) {
              shopStore.customerLedgers.push(l);
            }
          });
        }

        persistShopStore(targetShopId);
        broadcastSyncEventToShop(targetShopId, {
          type: 'store:sync',
          sourceDeviceId,
          timestamp: Date.now(),
          payload: shopStore,
        });
        return res.json({ success: true, store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'reset_all_zero': {
        shopStore.products = [];
        shopStore.transactions = [];
        shopStore.purchases = [];
        shopStore.customers = [];
        shopStore.customerLedgers = [];
        shopStore.saleReturns = [];
        shopStore.heldBills = [];
        persistShopStore(targetShopId);

        broadcastSyncEventToShop(targetShopId, {
          type: 'store:sync',
          sourceDeviceId,
          timestamp: Date.now(),
          payload: shopStore,
        });

        return res.json({ success: true, message: 'All store data reset to clean zero state.', store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      case 'load_seed_catalog': {
        shopStore.products = SEED_CATALOG_PRODUCTS;
        persistShopStore(targetShopId);

        broadcastSyncEventToShop(targetShopId, {
          type: 'store:sync',
          sourceDeviceId,
          timestamp: Date.now(),
          payload: shopStore,
        });

        return res.json({ success: true, message: 'Reference 15 picture products loaded.', store: sanitizeStoreForClient(shopStore, actor.role, actor.permissions) });
      }

      default:
        return res.status(400).json({ success: false, message: `Unknown action: ${action}` });
    }
  }));

  // -------------------------------------------------------------
  // 6. SUPER ADMIN CONSOLE, MULTI-SHOP, SUBSCRIPTIONS & BACKUP API
  // -------------------------------------------------------------
  function buildSuperAdminOverviewPayload() {
    checkAndAutoExpireSubscriptions();
    const totalIncome = subscriptionsDb.payments.reduce((sum, p) => sum + (p.finalAmount || 0), 0);
    const amountReceived = subscriptionsDb.payments.reduce((sum, p) => sum + (p.amountPaid || 0), 0);
    const outstandingBalance = subscriptionsDb.shops.reduce((sum, s) => sum + (s.remainingBalance || 0), 0);
    const currentYearMonth = new Date().toISOString().slice(0, 7);
    const thisMonthIncome = subscriptionsDb.payments
      .filter((p) => (p.paymentDate || '').startsWith(currentYearMonth))
      .reduce((sum, p) => sum + (p.amountPaid || 0), 0);
    const currentYear = new Date().getFullYear().toString();
    const thisYearIncome = subscriptionsDb.payments
      .filter((p) => (p.paymentDate || '').startsWith(currentYear))
      .reduce((sum, p) => sum + (p.amountPaid || 0), 0);

    const onlineDeviceIds = new Set(Array.from(sseClients.values()).map((c) => c.deviceId));
    const enrichedDevices = registeredDevices.map((d) => {
      const sId = d.shopId || 'SHOP-WM-001';
      const shop = subscriptionsDb.shops.find((s) => s.id === sId);
      return {
        ...d,
        shopId: sId,
        shopName: sId === SUPER_ADMIN_POS_SHOP_ID ? 'SUPER ADMIN · MY POS APP' : shop?.shopName || 'WAQAS MART',
        sessionToken: undefined,
        connectionState: onlineDeviceIds.has(d.deviceId) ? ('online' as const) : ('offline' as const),
      };
    });

    const enrichedShops = subscriptionsDb.shops.map((s) => {
      const ownerUser = authDb.users.find((u) => u.shopId === s.id && u.role === 'SHOP_OWNER');
      const shopDevs = enrichedDevices.filter((d) => d.shopId === s.id);
      const fallbackUsername = s.ownerUsername || (s.email && s.email.includes('@') ? s.email.split('@')[0] : s.email) || '';
      return {
        ...s,
        ownerUsername: ownerUser?.username || fallbackUsername,
        hasOwnerLogin: Boolean(ownerUser && ownerUser.passwordHash && ownerUser.passwordSalt),
        deviceCount: shopDevs.filter((d) => d.status === 'active').length,
        onlineDeviceCount: shopDevs.filter((d) => d.connectionState === 'online').length,
      };
    });

    const activeDevices = enrichedDevices.filter((d) => d.status === 'active');
    const onlineDevicesCount = activeDevices.filter((d) => d.connectionState === 'online').length;

    return {
      metrics: {
        totalIncome,
        amountReceived,
        outstandingBalance,
        thisMonthIncome,
        thisYearIncome,
        totalRenewals: subscriptionsDb.renewals.length,
        totalShops: subscriptionsDb.shops.length,
        activeShops: subscriptionsDb.shops.filter((s) => s.status === 'active' || s.status === 'expiring_soon').length,
        inactiveShops: subscriptionsDb.shops.filter((s) => s.status === 'inactive').length,
        expiringSoon: subscriptionsDb.shops.filter((s) => s.status === 'expiring_soon').length,
        expiredShops: subscriptionsDb.shops.filter((s) => s.status === 'expired').length,
        suspendedShops: subscriptionsDb.shops.filter((s) => s.status === 'suspended').length,
        blockedShops: subscriptionsDb.shops.filter((s) => s.status === 'blocked').length,
        totalOwners: authDb.users.filter((u) => u.role === 'SHOP_OWNER').length,
        totalDevices: registeredDevices.length,
        onlineDevices: onlineDevicesCount,
        offlineDevices: Math.max(0, activeDevices.length - onlineDevicesCount),
        revokedDevices: registeredDevices.filter((d) => d.status === 'revoked').length,
      },
      shops: enrichedShops,
      payments: subscriptionsDb.payments,
      renewals: subscriptionsDb.renewals,
      devices: enrichedDevices,
      auditLogs: auditLogs.slice(0, 200),
      superAdmin: authDb.superAdmin
        ? {
            id: authDb.superAdmin.id,
            gmail: authDb.superAdmin.gmail,
            mobile: authDb.superAdmin.mobile,
            fullName: authDb.superAdmin.fullName,
            verifiedGmail: authDb.superAdmin.verifiedGmail,
            verifiedMobile: authDb.superAdmin.verifiedMobile,
            createdAt: authDb.superAdmin.createdAt,
            lastLoginAt: authDb.superAdmin.lastLoginAt,
          }
        : null,
    };
  }

  app.get('/api/super-admin/overview', requireSuperAdmin, (_req: Request, res: Response) => {
    return res.json({ success: true, ...buildSuperAdminOverviewPayload() });
  });

  app.get('/api/subscriptions/overview', requireSuperAdmin, (_req: Request, res: Response) => {
    return res.json({ success: true, ...buildSuperAdminOverviewPayload() });
  });

  app.get('/api/super-admin/shops', requireSuperAdmin, (_req: Request, res: Response) => {
    return res.json({ success: true, shops: buildSuperAdminOverviewPayload().shops });
  });

  app.get('/api/subscriptions/shops', requireSuperAdmin, (_req: Request, res: Response) => {
    return res.json({ success: true, shops: buildSuperAdminOverviewPayload().shops });
  });

  function handleCreateShop(req: Request, res: Response) {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const {
      shopName,
      ownerName,
      username,
      password,
      confirmPassword,
      email,
      phone,
      mobile,
      whatsapp,
      address,
      plan = '12_months',
      status = 'active',
      startDate,
      expiryDate,
      pricing,
      initialPayment = 0,
      paymentMethod = 'Cash',
    } = req.body || {};

    const cleanShopName = String(shopName || '').trim();
    const cleanOwnerName = String(ownerName || cleanShopName).trim();
    const cleanPhone = String(mobile || phone || '').trim();
    const rawUsername = String(username || '').trim().toLowerCase();
    const cleanEmail = String(email || '').trim().toLowerCase();
    const cleanPass = String(password || '').trim();
    const cleanConfirm = confirmPassword !== undefined ? String(confirmPassword).trim() : cleanPass;

    if (!cleanShopName || !cleanOwnerName || (!rawUsername && !cleanEmail) || !cleanPass) {
      return res.status(400).json({
        success: false,
        error: 'REQUIRED_FIELDS_MISSING',
        message: 'Mart Name, Mart Owner Name, Shop Username, and Shop Password are required.',
      });
    }
    if (rawUsername && !/^[a-z0-9_.-]+$/.test(rawUsername)) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_SHOP_USERNAME',
        message: 'Shop Username may only contain letters, numbers, dots (.), underscores (_), or hyphens (-), with no spaces.',
      });
    }
    if (rawUsername && rawUsername.length < 3) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_SHOP_USERNAME',
        message: 'Shop Username must be at least 3 characters long.',
      });
    }
    if (cleanPass.length < 6) {
      return res.status(400).json({
        success: false,
        error: 'WEAK_PASSWORD',
        message: 'Shop Password must be at least 6 characters long.',
      });
    }
    if (cleanPass !== cleanConfirm) {
      return res.status(400).json({
        success: false,
        error: 'PASSWORD_MISMATCH',
        message: 'Shop Password and Confirm Password do not match.',
      });
    }

    const effectiveUsername = rawUsername || cleanEmail.split('@')[0].replace(/[^a-z0-9_.-]/g, '');
    if (!effectiveUsername || effectiveUsername.length < 3) {
      return res.status(400).json({
        success: false,
        error: 'INVALID_SHOP_USERNAME',
        message: 'Please enter a valid Shop Username (minimum 3 characters, no spaces).',
      });
    }
    const effectiveEmail = cleanEmail || `${effectiveUsername}@shop.local`;

    const dupUser = authDb.users.find(
      (u) =>
        u.username?.trim().toLowerCase() === effectiveUsername ||
        (cleanEmail && u.email?.trim().toLowerCase() === cleanEmail)
    );
    const dupShop = subscriptionsDb.shops.find(
      (s) =>
        (s.ownerUsername && s.ownerUsername.trim().toLowerCase() === effectiveUsername) ||
        (cleanEmail && s.email?.trim().toLowerCase() === cleanEmail)
    );
    if (dupUser || dupShop) {
      return res.status(409).json({
        success: false,
        error: 'DUPLICATE_USERNAME',
        message: `Shop Username "${effectiveUsername}" is already taken by another Mart. Please choose a different username.`,
      });
    }

    let uniqueShopId = `SHOP-WM-${100 + subscriptionsDb.shops.length + Math.floor(Math.random() * 900)}`;
    while (
      subscriptionsDb.shops.some((s) => s.id.toLowerCase() === uniqueShopId.toLowerCase()) ||
      authDb.users.some((u) => (u.shopId || '').toLowerCase() === uniqueShopId.toLowerCase())
    ) {
      uniqueShopId = `SHOP-WM-${100 + subscriptionsDb.shops.length + Math.floor(Math.random() * 9000)}`;
    }

    const initialPaid = Number(initialPayment) || 0;
    const standardPlanPrices: Record<string, number> = {
      '1_month': Number(pricing?.oneMonth) || 1500,
      '3_months': Number(pricing?.threeMonths) || 4000,
      '6_months': Number(pricing?.sixMonths) || 7500,
      '12_months': Number(pricing?.twelveMonths) || 14000,
      custom: Number(pricing?.customPricePerMonth) || 1500,
    };
    const totalCost = standardPlanPrices[plan] || 1500;
    const remainingBalance = Math.max(0, totalCost - initialPaid);
    const durationDays =
      plan === '1_month' ? 30 : plan === '3_months' ? 90 : plan === '6_months' ? 180 : 365;

    const newShop: ShopAccount = {
      id: uniqueShopId,
      shopName: cleanShopName,
      ownerName: cleanOwnerName,
      ownerUsername: effectiveUsername,
      hasOwnerLogin: true,
      email: effectiveEmail,
      phone: cleanPhone,
      whatsapp: String(whatsapp || cleanPhone).trim(),
      address: String(address || '').trim(),
      status: status === 'inactive' || status === 'suspended' ? status : 'active',
      plan: plan as any,
      startDate: startDate || new Date().toISOString().split('T')[0],
      expiryDate:
        expiryDate ||
        new Date(Date.now() + durationDays * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
      pricing: {
        oneMonth: Number(pricing?.oneMonth) || 1500,
        threeMonths: Number(pricing?.threeMonths) || 4000,
        sixMonths: Number(pricing?.sixMonths) || 7500,
        twelveMonths: Number(pricing?.twelveMonths) || 14000,
        customPricePerMonth: Number(pricing?.customPricePerMonth) || 1500,
        lastUpdated: new Date().toISOString().split('T')[0],
      },
      initialPayment: initialPaid,
      remainingBalance,
      totalPaid: initialPaid,
      totalIncome: totalCost,
      renewalsCount: 0,
      mustChangePasswordOnFirstLogin: false,
      createdAt: new Date().toISOString(),
    };

    const salt = crypto.randomBytes(16).toString('hex');
    const newOwnerUser: StoredUser = {
      id: `USR-${uniqueShopId}`,
      username: effectiveUsername,
      email: effectiveEmail,
      fullName: cleanOwnerName,
      phone: cleanPhone,
      whatsapp: String(whatsapp || cleanPhone).trim(),
      role: 'SHOP_OWNER',
      shopId: uniqueShopId,
      passwordSalt: salt,
      passwordHash: hashPassword(cleanPass, salt),
      mustChangePasswordOnFirstLogin: false,
      createdAt: new Date().toISOString(),
    };

    subscriptionsDb.shops.unshift(newShop);
    authDb.users.push(newOwnerUser);

    if (initialPaid > 0) {
      const receiptNumber = `RCP-${crypto.randomInt(1000, 9999)}`;
      subscriptionsDb.payments.unshift({
        id: `PAY-${Date.now()}`,
        receiptNumber,
        shopId: uniqueShopId,
        shopName: cleanShopName,
        ownerName: cleanOwnerName,
        ownerEmail: effectiveEmail,
        plan: newShop.plan,
        durationLabel: newShop.plan.replace('_', ' ').toUpperCase(),
        totalAmount: totalCost,
        discount: 0,
        finalAmount: totalCost,
        amountPaid: initialPaid,
        remainingBalance,
        paymentMethod: paymentMethod as any,
        paymentDate: new Date().toISOString().split('T')[0],
        confirmedBy: saSession?.fullName || 'Super Admin',
        status: remainingBalance === 0 ? 'Paid' : 'Partially Paid',
        createdAt: new Date().toISOString(),
      });
    }

    persistAuthDb();
    persistSubscriptions();

    const newStore = getShopStore(uniqueShopId);
    newStore.branding.storeName = cleanShopName;
    newStore.branding.primaryName = cleanShopName.split(' ')[0] || cleanShopName;
    newStore.branding.accentName = cleanShopName.split(' ').slice(1).join(' ') || 'POS';
    newStore.branding.sidebarPromoBadge = cleanShopName;
    newStore.userProfile = {
      id: newOwnerUser.id,
      displayName: cleanOwnerName,
      email: effectiveEmail,
      phone: cleanPhone,
      role: 'Shop Owner',
      createdAt: new Date().toLocaleDateString('en-GB'),
    };
    persistShopStore(uniqueShopId, false);

    recordAuditLog(
      'SHOP_CREATED',
      saSession?.email || 'superadmin',
      'SUPER_ADMIN',
      `Created isolated mart "${cleanShopName}" (${uniqueShopId}) for owner "${cleanOwnerName}" (username: ${effectiveUsername})`,
      uniqueShopId,
      req.ip
    );

    return res.json({
      success: true,
      shopId: uniqueShopId,
      shop: { ...newShop, ownerUsername: effectiveUsername, hasOwnerLogin: true },
      ownerUsername: effectiveUsername,
    });
  }

  app.post('/api/super-admin/shops', requireSuperAdmin, handleCreateShop);
  app.post('/api/subscriptions/shops', requireSuperAdmin, handleCreateShop);

  app.put('/api/super-admin/shops/:id', requireSuperAdmin, (req: Request, res: Response) => {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const { id } = req.params;
    const shop = subscriptionsDb.shops.find((s) => s.id === id);
    if (!shop) {
      return res.status(404).json({ success: false, message: 'Shop not found.' });
    }

    const { shopName, ownerName, ownerUsername, phone, whatsapp, email, address, status, plan, expiryDate, pricing } = req.body || {};
    const ownerUser = authDb.users.find((u) => u.shopId === id && u.role === 'SHOP_OWNER');

    if (ownerUsername !== undefined) {
      const cleanNewUsername = String(ownerUsername || '').trim().toLowerCase();
      if (!cleanNewUsername || cleanNewUsername.length < 3 || !/^[a-z0-9_.-]+$/.test(cleanNewUsername)) {
        return res.status(400).json({
          success: false,
          error: 'INVALID_SHOP_USERNAME',
          message: 'Shop Username must be at least 3 characters (letters, numbers, ., _, - with no spaces).',
        });
      }
      const dupUsername = authDb.users.find(
        (u) => u.id !== ownerUser?.id && u.username?.trim().toLowerCase() === cleanNewUsername
      );
      const dupShop = subscriptionsDb.shops.find(
        (s) => s.id !== shop.id && s.ownerUsername?.trim().toLowerCase() === cleanNewUsername
      );
      if (dupUsername || dupShop) {
        return res.status(409).json({
          success: false,
          error: 'DUPLICATE_USERNAME',
          message: 'That Shop Username is already in use by another mart.',
        });
      }
      if (ownerUser) ownerUser.username = cleanNewUsername;
      shop.ownerUsername = cleanNewUsername;
    }

    if (shopName) shop.shopName = String(shopName).trim();
    if (ownerName) shop.ownerName = String(ownerName).trim();
    if (phone !== undefined) shop.phone = String(phone).trim();
    if (whatsapp !== undefined) shop.whatsapp = String(whatsapp).trim();
    if (email !== undefined) shop.email = String(email).trim().toLowerCase();
    if (address !== undefined) shop.address = String(address).trim();
    if (status) shop.status = status;
    if (plan) shop.plan = plan;
    if (expiryDate) shop.expiryDate = expiryDate;
    if (pricing) {
      shop.pricing = { ...shop.pricing, ...pricing, lastUpdated: new Date().toISOString().split('T')[0] };
    }

    if (ownerUser) {
      if (ownerName) ownerUser.fullName = shop.ownerName;
      if (phone !== undefined) ownerUser.phone = shop.phone;
      if (whatsapp !== undefined) ownerUser.whatsapp = shop.whatsapp;
      if (email) ownerUser.email = shop.email;
      persistAuthDb();
    }

    if (shop.status === 'inactive' || shop.status === 'suspended' || shop.status === 'blocked') {
      for (const token of Object.keys(authDb.sessions)) {
        if (authDb.sessions[token].shopId === shop.id) {
          revokeTokenInDb(token, authDb.sessions[token].expiresAt);
          delete authDb.sessions[token];
        }
      }
      authDb.revokedBefore = authDb.revokedBefore || {};
      authDb.revokedBefore[`shop:${shop.id}`] = Date.now();
      if (ownerUser) {
        authDb.revokedBefore[ownerUser.id] = Date.now();
      }
      persistAuthDb();
      disconnectShopClients(shop.id, 'auth:shop_restricted', `Shop status changed to ${shop.status}`);
    }

    persistSubscriptions();
    recordAuditLog(
      'SHOP_UPDATED',
      saSession?.email || 'superadmin',
      'SUPER_ADMIN',
      `Updated shop "${shop.shopName}" (${shop.id})`,
      shop.id,
      req.ip
    );
    return res.json({ success: true, shop: { ...shop, ownerUsername: ownerUser?.username } });
  });

  app.put('/api/subscriptions/shops/:id', requireSuperAdmin, (req: Request, res: Response) => {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const { id } = req.params;
    const shop = subscriptionsDb.shops.find((s) => s.id === id);
    if (!shop) return res.status(404).json({ success: false, message: 'Shop not found' });
    const { status, pricing, expiryDate, plan } = req.body || {};
    const prevStatus = shop.status;
    if (status) shop.status = status;
    if (pricing) shop.pricing = { ...shop.pricing, ...pricing, lastUpdated: new Date().toISOString().split('T')[0] };
    if (expiryDate) shop.expiryDate = expiryDate;
    if (plan) shop.plan = plan;
    persistSubscriptions();
    if (shop.status === 'inactive' || shop.status === 'suspended' || shop.status === 'blocked') {
      for (const token of Object.keys(authDb.sessions)) {
        if (authDb.sessions[token].shopId === shop.id) {
          revokeTokenInDb(token, authDb.sessions[token].expiresAt);
          delete authDb.sessions[token];
        }
      }
      authDb.revokedBefore = authDb.revokedBefore || {};
      authDb.revokedBefore[`shop:${shop.id}`] = Date.now();
      persistAuthDb();
      disconnectShopClients(shop.id, 'auth:shop_restricted', `Shop status changed to ${shop.status}`);
    }
    recordAuditLog('SHOP_STATUS_UPDATED', saSession?.email || 'superadmin', 'SUPER_ADMIN', `Updated ${shop.shopName} (${shop.id}): ${prevStatus} -> ${shop.status}`, shop.id, req.ip);
    return res.json({ success: true, shop });
  });

  app.post('/api/super-admin/shops/:id/status', requireSuperAdmin, (req: Request, res: Response) => {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const { id } = req.params;
    const shop = subscriptionsDb.shops.find((s) => s.id === id);
    if (!shop) {
      return res.status(404).json({ success: false, message: 'Shop not found.' });
    }

    const { status } = req.body || {};
    const allowedStatuses = ['active', 'inactive', 'suspended', 'blocked', 'expired', 'expiring_soon'];
    const newStatus = allowedStatuses.includes(status) ? status : shop.status === 'active' ? 'suspended' : 'active';
    const prevStatus = shop.status;
    shop.status = newStatus;
    persistSubscriptions();

    if (newStatus === 'inactive' || newStatus === 'suspended' || newStatus === 'blocked' || newStatus === 'expired') {
      for (const token of Object.keys(authDb.sessions)) {
        if (authDb.sessions[token].shopId === shop.id) {
          revokeTokenInDb(token, authDb.sessions[token].expiresAt);
          delete authDb.sessions[token];
        }
      }
      authDb.revokedBefore = authDb.revokedBefore || {};
      authDb.revokedBefore[`shop:${shop.id}`] = Date.now();
      persistAuthDb();
      disconnectShopClients(shop.id, 'auth:shop_restricted', `Shop access ${newStatus}`);
    }

    recordAuditLog(
      'SHOP_STATUS_CHANGED',
      saSession?.email || 'superadmin',
      'SUPER_ADMIN',
      `Shop "${shop.shopName}" (${shop.id}) status changed: ${prevStatus} -> ${newStatus}`,
      shop.id,
      req.ip
    );
    return res.json({ success: true, status: newStatus, shop });
  });

  app.post('/api/super-admin/shops/:id/reset-password', requireSuperAdmin, (req: Request, res: Response) => {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const { id } = req.params;
    const shop = subscriptionsDb.shops.find((s) => s.id === id);
    if (!shop) {
      return res.status(404).json({ success: false, message: 'Shop not found.' });
    }

    const { password, confirmPassword, username: requestedUsername, mustChangeOnNextLogin = false } = req.body || {};
    const cleanPass = String(password || '').trim();
    if (!cleanPass || cleanPass.length < 6) {
      return res.status(400).json({ success: false, message: 'Password must be at least 6 characters long.' });
    }
    if (confirmPassword !== undefined && cleanPass !== String(confirmPassword).trim()) {
      return res.status(400).json({ success: false, message: 'Password and Confirm Password do not match.' });
    }

    let shopUser = authDb.users.find((u) => u.shopId === id && u.role === 'SHOP_OWNER');
    const salt = crypto.randomBytes(16).toString('hex');

    if (!shopUser) {
      const candidateUsername = String(
        requestedUsername ||
          shop.ownerUsername ||
          (shop.email && shop.email.includes('@') ? shop.email.split('@')[0] : '') ||
          shop.id.toLowerCase()
      )
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_.-]/g, '');

      if (!candidateUsername || candidateUsername.length < 3) {
        return res.status(400).json({ success: false, message: 'A valid Shop Username (min 3 chars) is required.' });
      }
      const dupUsername = authDb.users.find((u) => u.username?.trim().toLowerCase() === candidateUsername);
      if (dupUsername) {
        return res.status(409).json({
          success: false,
          message: `Username "${candidateUsername}" is already used by another account. Provide a different Shop Username.`,
        });
      }

      shopUser = {
        id: `USR-${shop.id}`,
        username: candidateUsername,
        email: shop.email || `${candidateUsername}@shop.local`,
        fullName: shop.ownerName || shop.shopName,
        phone: shop.phone || '',
        whatsapp: shop.whatsapp || shop.phone || '',
        role: 'SHOP_OWNER',
        shopId: shop.id,
        passwordSalt: salt,
        passwordHash: hashPassword(cleanPass, salt),
        mustChangePasswordOnFirstLogin: Boolean(mustChangeOnNextLogin),
        createdAt: shop.createdAt || new Date().toISOString(),
      };
      authDb.users.push(shopUser);
      shop.ownerUsername = candidateUsername;
      persistSubscriptions();
    } else {
      if (requestedUsername !== undefined && String(requestedUsername).trim()) {
        const cleanU = String(requestedUsername).trim().toLowerCase();
        if (!/^[a-z0-9_.-]+$/.test(cleanU) || cleanU.length < 3) {
          return res.status(400).json({
            success: false,
            message: 'Shop Username must be at least 3 characters (letters, numbers, ., _, - with no spaces).',
          });
        }
        const dupU = authDb.users.find((u) => u.id !== shopUser!.id && u.username?.trim().toLowerCase() === cleanU);
        if (dupU) {
          return res.status(409).json({ success: false, message: `Shop Username "${cleanU}" is already taken.` });
        }
        shopUser.username = cleanU;
        shop.ownerUsername = cleanU;
        persistSubscriptions();
      }
      shopUser.passwordSalt = salt;
      shopUser.passwordHash = hashPassword(cleanPass, salt);
      shopUser.mustChangePasswordOnFirstLogin = Boolean(mustChangeOnNextLogin);
    }

    for (const token of Object.keys(authDb.sessions)) {
      if (authDb.sessions[token].userId === shopUser.id) {
        revokeTokenInDb(token, authDb.sessions[token].expiresAt);
        delete authDb.sessions[token];
      }
    }
    authDb.revokedBefore = authDb.revokedBefore || {};
    authDb.revokedBefore[shopUser.id] = Date.now();
    persistAuthDb();

    recordAuditLog(
      'SHOP_PASSWORD_RESET',
      saSession?.email || 'superadmin',
      'SUPER_ADMIN',
      `Super Admin set/reset password for Shop Owner "${shopUser.username}" of ${id}`,
      id,
      req.ip
    );
    return res.json({
      success: true,
      shopId: shop.id,
      ownerUsername: shopUser.username,
      message: `Shop Owner login updated successfully (${shop.id} / ${shopUser.username}).`,
    });
  });

  app.delete('/api/super-admin/shops/:id', requireSuperAdmin, (req: Request, res: Response) => {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const { id } = req.params;
    if (id === 'SHOP-WM-001' && req.body?.allowPrimaryDelete !== true) {
      return res.status(400).json({ success: false, message: 'Primary branch SHOP-WM-001 cannot be deleted directly.' });
    }
    const shop = subscriptionsDb.shops.find((s) => s.id === id);
    if (!shop) {
      return res.status(404).json({ success: false, message: 'Shop not found.' });
    }
    const confirmText = String(req.body?.confirmText || '').trim();
    if (confirmText !== shop.id && confirmText.toLowerCase() !== shop.shopName.toLowerCase()) {
      return res.status(400).json({
        success: false,
        message: `Confirmation failed. Type "${shop.id}" or "${shop.shopName}" to delete this shop.`,
      });
    }

    const archiveDir = path.join(DATA_DIR, 'backups', 'deleted_shops');
    try {
      fs.mkdirSync(archiveDir, { recursive: true });
      const shopFile = getShopStoreFile(id);
      if (fs.existsSync(shopFile)) {
        fs.copyFileSync(shopFile, path.join(archiveDir, `${id}_${Date.now()}.json`));
        fs.unlinkSync(shopFile);
      }
    } catch {}
    shopStoreCache.delete(id);

    subscriptionsDb.shops = subscriptionsDb.shops.filter((s) => s.id !== id);
    persistSubscriptions();

    authDb.users = authDb.users.filter((u) => u.shopId !== id);
    for (const token of Object.keys(authDb.sessions)) {
      if (authDb.sessions[token].shopId === id) {
        revokeTokenInDb(token, authDb.sessions[token].expiresAt);
        delete authDb.sessions[token];
      }
    }
    authDb.revokedBefore = authDb.revokedBefore || {};
    authDb.revokedBefore[`shop:${id}`] = Date.now();
    persistAuthDb();

    disconnectShopClients(id, 'device:revoked', 'Shop account deleted by Super Admin');
    registeredDevices = registeredDevices.filter((d) => d.shopId !== id);
    persistDevices();

    recordAuditLog(
      'SHOP_DELETED',
      saSession?.email || 'superadmin',
      'SUPER_ADMIN',
      `Deleted shop "${shop.shopName}" (${id}) with archive backup`,
      id,
      req.ip
    );
    return res.json({ success: true, message: `Shop "${shop.shopName}" deleted cleanly.` });
  });

  function handleRecordPayment(req: Request, res: Response) {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const {
      shopId,
      plan,
      durationLabel,
      totalAmount,
      discount = 0,
      amountPaid,
      paymentMethod = 'Cash',
      paymentDate,
      transactionReference,
      notes,
    } = req.body || {};

    const shop = subscriptionsDb.shops.find((s) => s.id === shopId);
    if (!shop) {
      return res.status(404).json({ success: false, message: 'Shop not found.' });
    }

    const total = Number(totalAmount) || 0;
    const disc = Number(discount) || 0;
    const finalAmt = Math.max(0, total - disc);
    const paid = Number(amountPaid) || 0;
    const remaining = Math.max(0, finalAmt - paid);

    const receiptNumber = `RCP-${crypto.randomInt(1000, 9999)}`;
    const paymentRecord: SubscriptionPaymentRecord = {
      id: `PAY-${Date.now()}`,
      receiptNumber,
      shopId: shop.id,
      shopName: shop.shopName,
      ownerName: shop.ownerName,
      ownerEmail: shop.email,
      plan: plan || shop.plan,
      durationLabel: durationLabel || 'Subscription Payment',
      totalAmount: total,
      discount: disc,
      finalAmount: finalAmt,
      amountPaid: paid,
      remainingBalance: remaining,
      paymentMethod,
      paymentDate: paymentDate || new Date().toISOString().split('T')[0],
      transactionReference: transactionReference || undefined,
      confirmedBy: saSession?.fullName || 'Super Admin',
      notes: notes || undefined,
      status: remaining === 0 ? 'Paid' : paid > 0 ? 'Partially Paid' : 'Unpaid',
      createdAt: new Date().toISOString(),
    };

    subscriptionsDb.payments.unshift(paymentRecord);
    shop.totalPaid = (shop.totalPaid || 0) + paid;
    shop.remainingBalance = remaining;
    if (paid > 0 && shop.status === 'expired') {
      shop.status = 'active';
    }
    persistSubscriptions();

    recordAuditLog(
      'PAYMENT_RECORDED',
      saSession?.email || 'superadmin',
      'SUPER_ADMIN',
      `Recorded PKR ${paid.toLocaleString()} for ${shop.shopName} (${shop.id}) via ${paymentMethod} — Receipt: ${receiptNumber}`,
      shop.id,
      req.ip
    );

    return res.json({ success: true, payment: paymentRecord, shop });
  }

  app.post('/api/super-admin/payments', requireSuperAdmin, handleRecordPayment);
  app.post('/api/subscriptions/payments', requireSuperAdmin, handleRecordPayment);

  function handleRenewSubscription(req: Request, res: Response) {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const { shopId, plan, additionalDays, amount, paymentMethod, notes } = req.body || {};
    const shop = subscriptionsDb.shops.find((s) => s.id === shopId);
    if (!shop) {
      return res.status(404).json({ success: false, message: 'Shop not found' });
    }

    const durationDays =
      Number(additionalDays) ||
      (plan === '1_month' ? 30 : plan === '3_months' ? 90 : plan === '6_months' ? 180 : plan === '12_months' ? 365 : 30);

    const previousExpiry = shop.expiryDate;
    const currentExpiryDate = new Date(shop.expiryDate);
    const now = new Date();
    const baseDate = currentExpiryDate > now ? currentExpiryDate : now;
    const newExpiry = new Date(baseDate.getTime() + durationDays * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    shop.expiryDate = newExpiry;
    shop.status = 'active';
    shop.plan = plan || shop.plan;
    shop.renewalsCount = (shop.renewalsCount || 0) + 1;

    const receiptNumber = `RCP-${crypto.randomInt(1000, 9999)}`;
    const paidAmt = Number(amount) || 0;

    const renewalRecord: SubscriptionRenewalRecord = {
      id: `REN-${Date.now()}`,
      shopId: shop.id,
      shopName: shop.shopName,
      plan: shop.plan,
      durationDays,
      previousExpiry,
      newExpiry,
      paymentReceiptNumber: receiptNumber,
      amount: paidAmt,
      renewedAt: new Date().toISOString(),
      confirmedBy: saSession?.fullName || 'Super Admin',
    };
    subscriptionsDb.renewals.unshift(renewalRecord);

    if (paidAmt > 0) {
      subscriptionsDb.payments.unshift({
        id: `PAY-${Date.now()}`,
        receiptNumber,
        shopId: shop.id,
        shopName: shop.shopName,
        ownerName: shop.ownerName,
        ownerEmail: shop.email,
        plan: shop.plan,
        durationLabel: `${durationDays} Days Renewal`,
        totalAmount: paidAmt,
        discount: 0,
        finalAmount: paidAmt,
        amountPaid: paidAmt,
        remainingBalance: 0,
        paymentMethod: paymentMethod || 'Cash',
        paymentDate: new Date().toISOString().split('T')[0],
        confirmedBy: saSession?.fullName || 'Super Admin',
        notes: notes || 'Renewal payment confirmed',
        status: 'Paid',
        createdAt: new Date().toISOString(),
      });
      shop.totalPaid = (shop.totalPaid || 0) + paidAmt;
    }

    persistSubscriptions();
    recordAuditLog(
      'SUBSCRIPTION_RENEWED',
      saSession?.email || 'superadmin',
      'SUPER_ADMIN',
      `Renewed ${shop.shopName} (${shop.id}) by ${durationDays} days. New expiry: ${newExpiry}`,
      shop.id,
      req.ip
    );
    return res.json({ success: true, shop, renewal: renewalRecord, receiptNumber });
  }

  app.post('/api/super-admin/shops/:id/extend-subscription', requireSuperAdmin, (req: Request, res: Response) => {
    req.body = { ...req.body, shopId: req.params.id };
    return handleRenewSubscription(req, res);
  });
  app.post('/api/subscriptions/renew', requireSuperAdmin, handleRenewSubscription);

  app.get('/api/super-admin/devices', requireSuperAdmin, (_req: Request, res: Response) => {
    return res.json({ success: true, devices: buildSuperAdminOverviewPayload().devices });
  });

  app.post('/api/super-admin/devices/:id/revoke', requireSuperAdmin, (req: Request, res: Response) => {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const { id } = req.params;
    const target = registeredDevices.find((d) => d.deviceId === id);
    if (!target) return res.status(404).json({ success: false, message: 'Device not found.' });
    target.status = 'revoked';
    target.connectionState = 'offline';
    persistDevices();
    const targetShopId = target.shopId || 'SHOP-WM-001';
    recordAuditLog('DEVICE_REVOKED', saSession?.email || 'superadmin', 'SUPER_ADMIN', `Super Admin revoked device "${target.deviceName}" (${id})`, targetShopId, req.ip);
    broadcastSyncEventToShop(targetShopId, { type: 'device:revoked', sourceDeviceId: id, timestamp: Date.now(), payload: { deviceId: id } });
    for (const [cid, client] of sseClients.entries()) {
      if (client.deviceId === id) {
        try { client.res.end(); } catch {}
        sseClients.delete(cid);
      }
    }
    broadcastSyncEventToShop(targetShopId, {
      type: 'device:updated',
      timestamp: Date.now(),
      payload: registeredDevices.filter((d) => d.shopId === targetShopId).map((d) => ({ ...d, sessionToken: undefined })),
    });
    return res.json({ success: true, message: 'Device revoked.' });
  });

  app.post('/api/super-admin/devices/:id/restore', requireSuperAdmin, (req: Request, res: Response) => {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const { id } = req.params;
    const target = registeredDevices.find((d) => d.deviceId === id);
    if (!target) return res.status(404).json({ success: false, message: 'Device not found.' });
    target.status = 'active';
    removedDeviceIds.delete(id);
    persistDevices();
    const targetShopId = target.shopId || 'SHOP-WM-001';
    recordAuditLog('DEVICE_RESTORED', saSession?.email || 'superadmin', 'SUPER_ADMIN', `Super Admin restored device "${target.deviceName}" (${id})`, targetShopId, req.ip);
    broadcastSyncEventToShop(targetShopId, {
      type: 'device:updated',
      timestamp: Date.now(),
      payload: registeredDevices.filter((d) => d.shopId === targetShopId).map((d) => ({ ...d, sessionToken: undefined })),
    });
    return res.json({ success: true, message: 'Device restored.' });
  });

  app.delete('/api/super-admin/devices/:id', requireSuperAdmin, (req: Request, res: Response) => {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const { id } = req.params;
    const target = registeredDevices.find((d) => d.deviceId === id);
    if (!target) return res.status(404).json({ success: false, message: 'Device not found.' });
    const targetShopId = target.shopId || 'SHOP-WM-001';
    registeredDevices = registeredDevices.filter((d) => d.deviceId !== id);
    removedDeviceIds.add(id);
    persistDevices();
    recordAuditLog('DEVICE_REMOVED', saSession?.email || 'superadmin', 'SUPER_ADMIN', `Super Admin removed device "${target.deviceName}" (${id})`, targetShopId, req.ip);
    broadcastSyncEventToShop(targetShopId, { type: 'device:revoked', timestamp: Date.now(), payload: { deviceId: id } });
    for (const [cid, client] of sseClients.entries()) {
      if (client.deviceId === id) {
        try { client.res.end(); } catch {}
        sseClients.delete(cid);
      }
    }
    return res.json({ success: true, message: 'Device removed.' });
  });

  app.get('/api/super-admin/audit-logs', requireSuperAdmin, (_req: Request, res: Response) => {
    return res.json({ success: true, logs: auditLogs });
  });

  app.post('/api/super-admin/sessions/revoke-others', requireSuperAdmin, (req: Request, res: Response) => {
    const currentTok = extractSessionTokenFromReq(req);
    const currentSess = (req as any).superAdminSession as AuthSessionData;
    let revoked = 0;
    for (const [tok, sess] of Object.entries(authDb.sessions)) {
      if (sess.role === 'SUPER_ADMIN' && tok !== currentTok) {
        delete authDb.sessions[tok];
        revoked += 1;
      }
    }
    authDb.revokedBefore = authDb.revokedBefore || {};
    authDb.revokedBefore[currentSess.userId] = currentSess.createdAt;
    persistAuthDb();

    recordAuditLog(
      'SUPER_ADMIN_SESSIONS_REVOKED',
      currentSess?.email || 'superadmin',
      'SUPER_ADMIN',
      `Revoked ${revoked} other active Super Admin session(s).`,
      undefined,
      req.ip
    );
    return res.json({ success: true, message: `Terminated ${revoked} other Super Admin session(s).` });
  });

  app.get('/api/settings/mart-owner', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const shopId = actor.shopId;
    const shopStore = await ensureShopStoreHydratedFromKv(shopId);
    const isSuperAdminPos = shopId === SUPER_ADMIN_POS_SHOP_ID;
    return res.json({
      success: true,
      martOwner: shopStore.martOwner || {
        martName: shopStore.branding.storeName || (isSuperAdminPos ? 'MY POS APP' : 'WAQAS MART'),
        badge: isSuperAdminPos ? 'SUPER ADMIN POS' : 'MART OWNER',
        ownerName: shopStore.userProfile.displayName || (isSuperAdminPos ? 'Muhammad Saad' : 'Shop Owner'),
        designation: isSuperAdminPos ? 'Super Admin Personal POS' : 'Proprietor & Manager',
        mobileNumber: shopStore.userProfile.phone || '03228570782',
        whatsappNumber: shopStore.userProfile.phone || '03228570782',
        address: isSuperAdminPos ? 'Super Admin Personal Terminal' : 'Commercial Market, Pakistan',
        photoUrl: isSuperAdminPos
          ? '/5c454cea45cc8c39a24b57a83fbbc5cf_0.webp'
          : '/assets/aistudio/female_owner_portrait.svg',
      },
      softwareOwner: {
        name: 'Muhammad Waqas',
        designation: 'WAQAS POS SOFTWARE',
        whatsapp: '03228570782',
        mobile: '03429634133',
        photoUrl: '/5c454cea45cc8c39a24b57a83fbbc5cf_0.webp',
        isPermanentlyLocked: true,
      },
    });
  }));

  app.post('/api/settings/mart-owner', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const shopStore = await ensureShopStoreHydratedFromKv(actor.shopId);
    const { martName, ownerName, designation, mobileNumber, whatsappNumber, address, photoUrl } = req.body || {};
    shopStore.martOwner = {
      martName: martName || shopStore.branding.storeName || 'WAQAS MART',
      badge: 'MART OWNER',
      ownerName: ownerName || shopStore.userProfile.displayName || 'Shop Owner',
      designation: designation || 'Proprietor & Manager',
      mobileNumber: mobileNumber || '03228570782',
      whatsappNumber: whatsappNumber || mobileNumber || '03228570782',
      address: address || '',
      photoUrl: photoUrl || '/assets/aistudio/female_owner_portrait.svg',
    };
    persistShopStore(actor.shopId);
    broadcastSyncEventToShop(actor.shopId, {
      type: 'store:sync',
      timestamp: Date.now(),
      payload: shopStore,
    });
    return res.json({ success: true, martOwner: shopStore.martOwner });
  }));

  app.get('/api/super-admin/backup/export', requireSuperAdmin, (_req: Request, res: Response) => {
    const allShopData: Record<string, any> = {};
    if (fs.existsSync(SHOPS_DIR)) {
      const files = fs.readdirSync(SHOPS_DIR);
      for (const f of files) {
        if (f.endsWith('.json')) {
          const sId = f.replace('.json', '');
          try {
            allShopData[sId] = JSON.parse(fs.readFileSync(path.join(SHOPS_DIR, f), 'utf-8'));
          } catch {}
        }
      }
    }
    return res.json({
      success: true,
      timestamp: new Date().toISOString(),
      version: '3.0-multishop',
      superAdmin: authDb.superAdmin
        ? { id: authDb.superAdmin.id, gmail: authDb.superAdmin.gmail, mobile: authDb.superAdmin.mobile, fullName: authDb.superAdmin.fullName }
        : null,
      shops: subscriptionsDb.shops,
      shopDatabases: allShopData,
      payments: subscriptionsDb.payments,
      renewals: subscriptionsDb.renewals,
      auditLogs,
      devices: registeredDevices.map((d) => ({ ...d, sessionToken: undefined })),
    });
  });

  app.post('/api/super-admin/backup/import', requireSuperAdmin, (req: Request, res: Response) => {
    const saSession = (req as any).superAdminSession as AuthSessionData;
    const restoredData = req.body?.restoredData || req.body?.data || req.body;
    if (!restoredData || typeof restoredData !== 'object') {
      return res.status(400).json({ success: false, message: 'Invalid backup payload.' });
    }

    const safetyDir = path.join(DATA_DIR, 'backups', `safety_before_restore_${Date.now()}`);
    try {
      fs.mkdirSync(safetyDir, { recursive: true });
      if (fs.existsSync(AUTH_DB_FILE)) fs.copyFileSync(AUTH_DB_FILE, path.join(safetyDir, 'auth_db.json'));
      if (fs.existsSync(STORE_DB_FILE)) fs.copyFileSync(STORE_DB_FILE, path.join(safetyDir, 'store_db.json'));
      if (fs.existsSync(SUBSCRIPTIONS_DB_FILE)) fs.copyFileSync(SUBSCRIPTIONS_DB_FILE, path.join(safetyDir, 'subscriptions_db.json'));
    } catch {}

    if (Array.isArray(restoredData.shops)) {
      subscriptionsDb.shops = restoredData.shops;
    }
    if (Array.isArray(restoredData.payments)) {
      subscriptionsDb.payments = restoredData.payments;
    }
    if (Array.isArray(restoredData.renewals)) {
      subscriptionsDb.renewals = restoredData.renewals;
    }
    persistSubscriptions();

    if (restoredData.shopDatabases && typeof restoredData.shopDatabases === 'object') {
      for (const [sId, sData] of Object.entries(restoredData.shopDatabases)) {
        setShopStore(sId, sData as StoreData);
        broadcastSyncEventToShop(sId, {
          type: 'store:sync',
          timestamp: Date.now(),
          payload: getShopStore(sId),
        });
      }
    }

    recordAuditLog('SYSTEM_BACKUP_RESTORED', saSession?.email || 'superadmin', 'SUPER_ADMIN', 'Super Admin restored multi-shop system backup.', undefined, req.ip);
    return res.json({ success: true, message: 'System backup restored across shops.' });
  });

  app.get('/api/shop/backup/export', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    if (actor.role === 'STAFF' && !actor.permissions?.settings) {
      return res.status(403).json({ success: false, message: 'Backup export requires Settings permission.' });
    }
    const shopStore = await ensureShopStoreHydratedFromKv(actor.shopId);
    const shopMeta = subscriptionsDb.shops.find((s) => s.id === actor.shopId);
    return res.json({
      success: true,
      backup: {
        softwareName: 'WAQAS POS SOFTWARE',
        exportedAt: new Date().toISOString(),
        shopId: actor.shopId,
        martName: shopMeta?.shopName || shopStore.branding.storeName,
        storeDb: shopStore,
      },
    });
  }));

  app.post('/api/shop/backup/import', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    if (actor.role === 'STAFF' && !actor.permissions?.settings) {
      return res.status(403).json({ success: false, message: 'Backup restore requires Settings permission.' });
    }

    const restoredData = req.body?.restoredData || req.body?.data || req.body;
    if (!restoredData || typeof restoredData !== 'object') {
      return res.status(400).json({ success: false, message: 'Invalid backup payload.' });
    }

    const shopId = actor.shopId;
    const current = await ensureShopStoreHydratedFromKv(shopId);
    const safetyDir = path.join(DATA_DIR, 'backups', `safety_shop_${shopId}_${Date.now()}`);
    try {
      fs.mkdirSync(safetyDir, { recursive: true });
      const f = getShopStoreFile(shopId);
      if (fs.existsSync(f)) fs.copyFileSync(f, path.join(safetyDir, `${shopId}.json`));
    } catch {}

    const sourceStore = restoredData.storeDb || restoredData.store || restoredData;
    const updated = setShopStore(shopId, {
      ...current,
      products: Array.isArray(sourceStore.products) ? sourceStore.products : current.products,
      transactions: Array.isArray(sourceStore.transactions) ? sourceStore.transactions : current.transactions,
      purchases: Array.isArray(sourceStore.purchases) ? sourceStore.purchases : current.purchases,
      customers: Array.isArray(sourceStore.customers) ? sourceStore.customers : current.customers,
      customerLedgers: Array.isArray(sourceStore.customerLedgerEntries)
        ? sourceStore.customerLedgerEntries
        : Array.isArray(sourceStore.customerLedgers)
        ? sourceStore.customerLedgers
        : current.customerLedgers,
      suppliers: Array.isArray(sourceStore.suppliers) ? sourceStore.suppliers : current.suppliers,
      supplierLedgers: Array.isArray(sourceStore.supplierLedgers) ? sourceStore.supplierLedgers : current.supplierLedgers,
      saleReturns: Array.isArray(sourceStore.saleReturns) ? sourceStore.saleReturns : current.saleReturns,
      purchaseReturns: Array.isArray(sourceStore.purchaseReturns) ? sourceStore.purchaseReturns : current.purchaseReturns,
      expenses: Array.isArray(sourceStore.expenses) ? sourceStore.expenses : current.expenses,
      staffList: Array.isArray(sourceStore.staffList) ? sourceStore.staffList : current.staffList,
      stockHistory: Array.isArray(sourceStore.stockHistory) ? sourceStore.stockHistory : current.stockHistory,
      dailyHisab: sourceStore.dailyHisab || current.dailyHisab,
      categories: Array.isArray(sourceStore.categories) ? sourceStore.categories : current.categories,
      branding: sourceStore.branding || current.branding,
    });

    recordAuditLog('SHOP_BACKUP_RESTORED', actor.actorEmail, actor.role, `Restored backup for shop ${shopId}`, shopId, req.ip);

    broadcastSyncEventToShop(shopId, {
      type: 'store:sync',
      timestamp: Date.now(),
      payload: updated,
    });

    return res.json({ success: true, message: 'Shop backup restored & synchronized.', store: updated });
  }));

  app.get('/api/security/overview', asyncHandler(async (req: Request, res: Response) => {
    const actor = await resolveShopActorAsync(req);
    if (!actor.ok) {
      return res.status(actor.statusCode).json({ success: false, error: actor.error, message: actor.message });
    }
    const shopId = actor.shopId;
    const shopDevs = registeredDevices.filter((d) => (d.shopId || 'SHOP-WM-001') === shopId);
    const shopStore = await ensureShopStoreHydratedFromKv(shopId);
    const shopSseCount = Array.from(sseClients.values()).filter((c) => c.shopId === shopId).length;
    const shopAudit = auditLogs.filter((l) => !l.shopId || l.shopId === shopId).slice(0, 60);

    return res.json({
      success: true,
      deviceAuthorized: true,
      activeDevicesCount: shopDevs.filter((d) => d.status === 'active').length,
      revokedDevicesCount: shopDevs.filter((d) => d.status === 'revoked').length,
      connectedSseClients: shopSseCount,
      syncVersion: shopStore.syncVersion || 1,
      auditLogs: shopAudit,
    });
  }));

  // Catch-all for unmatched /api/* routes: guarantees JSON instead of HTML
  app.all('/api/*', (req: Request, res: Response) => {
    res.status(404).json({
      success: false,
      error: 'API_ENDPOINT_NOT_FOUND',
      message: `The API route ${req.method} ${req.path} was not found on WAQAS POS Server.`,
      path: req.path,
    });
  });

  // JSON Error Handler for malformed JSON bodies or unexpected runtime errors
  app.use((err: any, req: Request, res: Response, next: NextFunction) => {
    if (req.path.startsWith('/api')) {
      if (err?.type === 'entity.parse.failed' || err instanceof SyntaxError) {
        return res.status(400).json({
          success: false,
          error: 'INVALID_JSON_BODY',
          message: 'Request body must be valid JSON.',
        });
      }
      console.error('[API Error]', err);
      return res.status(err?.statusCode || err?.status || 500).json({
        success: false,
        error: 'INTERNAL_SERVER_ERROR',
        message: 'An unexpected backend error occurred.',
      });
    }
    next(err);
  });

  cachedApp = app;
  return app;
}
