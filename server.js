'use strict';

const https = require('https');
const http = require('http');
const zlib = require('zlib');
const url = require('url');
const querystring = require('querystring');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ============================================================
// Load .env file for sensitive configuration (QUEEN only)
// ============================================================
const envFile = path.join(__dirname, '.env');
if (fs.existsSync(envFile)) {
    const envContent = fs.readFileSync(envFile, 'utf8');
    envContent.split('\n').forEach(line => {
        const trimmed = line.trim();
        if (trimmed && !trimmed.startsWith('#') && trimmed.includes('=')) {
            const [key, ...valueParts] = trimmed.split('=');
            const value = valueParts.join('=').trim();
            if (!process.env[key]) {
                process.env[key] = value;
            }
        }
    });
    console.log('[CONFIG] Loaded .env file');
}

// ============================================================
// MANUAL CONFIGURATION - Change these to switch modes
// ============================================================

// MODE: 'standalone', 'bee', or 'queen'
// - standalone: Single server, handles registration + API
// - bee: Connects to queen, syncs data
// - queen: Central server, manages bees, sends emails
const SERVER_ROLE = process.env.SERVER_ROLE || 'standalone';

// CODENAME: Unique name for this server (a-z0-9, no spaces)
// Required for standalone/queen, optional for bee (default: bee01)
const SERVER_CODE_NAME = process.env.SERVER_CODE_NAME || 'bee01';

// Used for HTTPS certificate path and email from address
const DOMAIN_SUFFIX = process.env.DOMAIN_SUFFIX || 'cccc.cn';

// QUEEN ADDRESS (for bee mode only)
const QUEEN_HOST = process.env.QUEEN_HOST || '';      // e.g., '192.168.1.100'
const QUEEN_PORT = process.env.QUEEN_PORT || '8080';
const QUEEN_HTTPS = process.env.QUEEN_HTTPS === 'true' || process.env.QUEEN_HTTPS === '1';  // Set to 'true' if queen uses HTTPS

// First queen address (for second queen joining)
const FIRST_QUEEN_HOST = process.env.FIRST_QUEEN_HOST || '';
const FIRST_QUEEN_PORT = process.env.FIRST_QUEEN_PORT || '8080';
const FIRST_QUEEN_HTTPS = process.env.FIRST_QUEEN_HTTPS === 'true' || process.env.FIRST_QUEEN_HTTPS === '1';

// ============================================================

// Configuration
const CONFIG = {
    port: process.env.PORT || 8080,
    serverCodename: SERVER_CODE_NAME,
    email: 'n.j.loman@bham.ac.uk',
    cacheTime: 60 * 60 * 24, // 24 hours in seconds
    entrezBaseUrl: 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils',
    apiKeysFile: path.join(__dirname, 'api_keys.json'),
    apiKeysBackupFile: path.join(__dirname, 'api_keys.backup.json'),
    backupInterval: 24 * 60 * 60 * 1000, // 24 hours in milliseconds

    // ============================================================
    // HTTPS/TLS Configuration (certbot)
    // ============================================================
    // Default domain: {codename}.{suffix}
    defaultDomain: SERVER_CODE_NAME && SERVER_CODE_NAME !== 'bee01'
        ? `${SERVER_CODE_NAME}.${DOMAIN_SUFFIX}`
        : '',
    certPath: process.env.CERT_PATH || '/etc/letsencrypt/live',
    keyFile: process.env.KEY_FILE || 'privkey.pem',
    certFile: process.env.CERT_FILE || 'fullchain.pem',

    // Server URL - auto-generated from domain if not set
    serverUrl: process.env.SERVER_URL || (SERVER_CODE_NAME && SERVER_CODE_NAME !== 'bee01'
        ? `https://${SERVER_CODE_NAME}.${DOMAIN_SUFFIX}:${process.env.PORT || 8080}`
        : `https://localhost:${process.env.PORT || 8080}`),
    smtpHost: process.env.SMTP_HOST || '',
    smtpPort: process.env.SMTP_PORT || 587,
    smtpUser: process.env.SMTP_USER || '',
    smtpPass: process.env.SMTP_PASS || '',
    smtpFrom: process.env.SMTP_FROM || '',  // Sender email address (e.g., noreply@yourdomain.com)
    // Multiple API keys for rotation (for higher rate limits)
    entrezApiKeys: (process.env.ENTREZ_API_KEYS || '').split(',').filter(k => k.trim()),
    trendingFile: path.join(__dirname, 'trending.json'),
    trendingUpdateInterval: 24 * 60 * 60 * 1000, // 24 hours

    // ============================================================
    // Distributed System Configuration (Queen/Bee)
    // ============================================================
    serverRole: SERVER_ROLE,

    // Queen server address (for bee to connect)
    queenHost: QUEEN_HOST,
    queenPort: QUEEN_PORT,
    queenHttps: QUEEN_HTTPS,

    // For second queen joining (optional)
    firstQueenHost: FIRST_QUEEN_HOST,
    firstQueenPort: FIRST_QUEEN_PORT,
    firstQueenHttps: FIRST_QUEEN_HTTPS,

    // Sync interval (1 hour)
    syncInterval: 60 * 60 * 1000,

    // Bee list file (queen only)
    beesFile: path.join(__dirname, 'bees.json'),

    // Shared cache file
    sharedCacheFile: path.join(__dirname, 'shared_cache.json')
};

// ============================================================
// Encryption for Queen-Bee communication
// ============================================================
// Shared encryption key for Queen-Bee communication
const ENCRYPTION_KEY = crypto.createHash('sha256').update('QueenBee').digest();

// Validate server codename (lowercase letters and numbers only, no spaces)
// Queen/standalone role requires unique codename; bee can use default
const CODE_NAME_PATTERN = /^[a-z0-9]+$/;
if ((SERVER_ROLE === 'queen' || SERVER_ROLE === 'standalone') && (!SERVER_CODE_NAME || SERVER_CODE_NAME === 'bee01')) {
    console.error('\n' +
        `ERROR: ${SERVER_ROLE.toUpperCase()} mode requires a unique SERVER_CODE_NAME!\n\n` +
        '=== QUICK START ===\n' +
        'To run as BEE (connect to queen):\n' +
        '  export SERVER_ROLE=bee\n' +
        '  export QUEEN_HOST=192.168.1.100\n' +
        '  export QUEEN_PORT=8080\n\n' +
        'To run as QUEEN (central server):\n' +
        '  export SERVER_ROLE=queen\n' +
        '  export SERVER_CODE_NAME=queen01\n\n' +
        'To run as STANDALONE (single server):\n' +
        '  export SERVER_ROLE=standalone\n' +
        '  export SERVER_CODE_NAME=myserver01\n\n' +
        'Or create .env file with above settings.\n');
    process.exit(1);
}
if (CONFIG.serverCodename && CONFIG.serverCodename !== 'bee01' && !CODE_NAME_PATTERN.test(CONFIG.serverCodename)) {
    console.error('\n' +
        'ERROR: Invalid SERVER_CODE_NAME: "' + CONFIG.serverCodename + '"\n\n' +
        'Codename must contain only:\n' +
        '  - Lowercase letters (a-z)\n' +
        '  - Numbers (0-9)\n' +
        '  - No spaces\n\n' +
        'Example: export SERVER_CODE_NAME=queen01\n');
    process.exit(1);
}

// Rate limiting
const rateLimitStore = new Map();
const RATE_LIMIT_WINDOW = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX = 30; // 30 requests per minute per IP

function checkRateLimit(ip) {
    const now = Date.now();
    const record = rateLimitStore.get(ip);

    if (!record || now - record.windowStart > RATE_LIMIT_WINDOW) {
        rateLimitStore.set(ip, { windowStart: now, count: 1 });
        return true;
    }

    if (record.count >= RATE_LIMIT_MAX) {
        return false;
    }

    record.count++;
    return true;
}

// Optimized in-memory cache with compression and size-based eviction
class EntrezCache {
    constructor() {
        this.cache = new Map();
        this.maxEntries = 5000;  // Max number of cached entries
        this.maxMemoryMB = 512;   // Max memory for cache (512MB)
        this.currentSizeMB = 0;   // Current cache size estimate
        this.cleanupInterval = 30 * 60 * 1000; // Check every 30 minutes
        this.minCompressionSize = 1024; // Compress if > 1KB

        // Start periodic cleanup
        setInterval(() => this.cleanup(), this.cleanupInterval);
    }

    createCacheKey(interfaceName, params) {
        const keydict = { interface: interfaceName, ...params };
        const keys = Object.keys(keydict).sort();
        return keys.map(k => `${k}=${keydict[k]}`).join('&');
    }

    get(key) {
        const entry = this.cache.get(key);
        if (entry && entry.expires > Date.now()) {
            // Update access time for LRU
            entry.lastAccessed = Date.now();
            return entry.data;
        }
        this.cache.delete(key);
        return null;
    }

    set(key, data, ttl = CONFIG.cacheTime) {
        // Check memory and cleanup if needed
        this.checkMemory();

        // Estimate size of data
        const jsonStr = JSON.stringify(data);
        const rawSize = Buffer.byteLength(jsonStr, 'utf8');

        // Compress if large enough
        let storedData = data;
        let compressed = false;
        let compressedSize = rawSize;

        if (rawSize > this.minCompressionSize) {
            try {
                const compressedBuf = zlib.gzipSync(Buffer.from(jsonStr, 'utf8'));
                compressedSize = compressedBuf.length;
                // Only store compressed if it saves space
                if (compressedSize < rawSize) {
                    storedData = compressedBuf.toString('base64');
                    compressed = true;
                }
            } catch (e) {
                // Compression failed, store uncompressed
            }
        }

        // Update total size
        const entrySizeMB = compressedSize / (1024 * 1024);
        this.currentSizeMB += entrySizeMB;

        this.cache.set(key, {
            data: storedData,
            compressed: compressed,
            rawSize: rawSize,
            sizeMB: entrySizeMB,
            expires: Date.now() + (ttl * 1000),
            lastAccessed: Date.now()
        });
    }

    // Get data (with decompression if needed)
    get(key) {
        const entry = this.cache.get(key);
        if (!entry) return null;

        if (entry.expires < Date.now()) {
            this.currentSizeMB -= entry.sizeMB;
            this.cache.delete(key);
            return null;
        }

        // Update access time for LRU
        entry.lastAccessed = Date.now();

        // Decompress if needed
        if (entry.compressed) {
            try {
                const buf = Buffer.from(entry.data, 'base64');
                const decompressed = zlib.gunzipSync(buf);
                return JSON.parse(decompressed.toString('utf8'));
            } catch (e) {
                return null;
            }
        }

        return entry.data;
    }

    getSize() {
        return {
            entries: this.cache.size,
            sizeMB: this.currentSizeMB.toFixed(2)
        };
    }

    // Cleanup expired entries
    cleanup() {
        const now = Date.now();
        let removed = 0;
        let freedMB = 0;

        for (const [key, entry] of this.cache) {
            if (entry.expires < now) {
                freedMB += entry.sizeMB;
                this.cache.delete(key);
                removed++;
            }
        }

        this.currentSizeMB -= freedMB;

        if (removed > 0) {
            console.log(`[CACHE] Cleaned up ${removed} expired entries (freed ${freedMB.toFixed(2)}MB)`);
        }
    }

    // Check memory usage and cleanup if needed
    checkMemory() {
        // If cache exceeds entry limit or memory limit, remove oldest entries (LRU)
        const needsEviction = this.cache.size >= this.maxEntries || this.currentSizeMB >= this.maxMemoryMB;

        if (needsEviction) {
            const entries = Array.from(this.cache.entries());
            // Sort by lastAccessed (oldest first)
            entries.sort((a, b) => a[1].lastAccessed - b[1].lastAccessed);

            // Remove oldest 20% or enough to get under 70% of limit
            let targetCount = Math.floor(this.maxEntries * 0.2);
            let targetMB = this.maxMemoryMB * 0.7;
            let removed = 0;
            let freedMB = 0;

            for (let i = 0; i < entries.length && removed < targetCount; i++) {
                if (this.currentSizeMB > targetMB || removed < targetCount) {
                    freedMB += entries[i][1].sizeMB;
                    this.cache.delete(entries[i][0]);
                    removed++;
                }
            }

            this.currentSizeMB -= freedMB;
            console.log(`[CACHE] Evicted ${removed} entries (freed ${freedMB.toFixed(2)}MB)`);
        }

        // Check process memory
        const used = process.memoryUsage();
        const heapUsedMB = Math.round(used.heapUsed / 1024 / 1024);
        const heapTotalMB = Math.round(used.heapTotal / 1024 / 1024);

        // If heap usage > 80%, aggressive cleanup
        if (used.heapUsed / used.heapTotal > 0.8) {
            console.log(`[CACHE] High memory usage: ${heapUsedMB}MB / ${heapTotalMB}MB`);

            // Aggressive cleanup - remove 50% of oldest entries
            const entries = Array.from(this.cache.entries());
            entries.sort((a, b) => a[1].lastAccessed - b[1].lastAccessed);

            const removeCount = Math.floor(entries.length * 0.5);
            for (let i = 0; i < removeCount; i++) {
                this.cache.delete(entries[i][0]);
            }

            console.log(`[CACHE] Emergency cleanup: removed ${removeCount} entries`);
        }
    }

    // Get cache statistics
    getStats() {
        const used = process.memoryUsage();
        return {
            entries: this.cache.size,
            maxEntries: this.maxEntries,
            sizeMB: this.currentSizeMB.toFixed(2),
            maxSizeMB: this.maxMemoryMB,
            heapUsed: Math.round(used.heapUsed / 1024 / 1024) + 'MB',
            heapTotal: Math.round(used.heapTotal / 1024 / 1024) + 'MB'
        };
    }
}

const entrezCache = new EntrezCache();

// API Keys storage (file-based, persisted)
// ============================================================
// Security: IP Tracking and Banning
// ============================================================

const BANNED_IPS_FILE = path.join(__dirname, 'banned_ips.json');

// IP ban management
class IpBanManager {
    constructor() {
        this.bannedIps = new Set(); // Direct IP bans
        this.bannedSubnets = new Map(); // Subnet bans (e.g., /24)
        this.ipViolationCounts = new Map(); // Track violations per IP
        this.loadBannedIps();
    }

    loadBannedIps() {
        try {
            if (fs.existsSync(BANNED_IPS_FILE)) {
                const data = JSON.parse(fs.readFileSync(BANNED_IPS_FILE, 'utf8'));
                this.bannedIps = new Set(data.direct || []);
                // Subnets stored as { "192.168.1": { count: 5, banned: true } }
                this.bannedSubnets = new Map(Object.entries(data.subnets || {}));
                console.log(`[SECURITY] Loaded ${this.bannedIps.size} banned IPs and ${this.bannedSubnets.size} banned subnets`);
            }
        } catch (e) {
            console.log('[SECURITY] No banned IPs file, starting fresh');
        }
    }

    saveBannedIps() {
        try {
            const data = {
                direct: Array.from(this.bannedIps),
                subnets: Object.fromEntries(this.bannedSubnets)
            };
            fs.writeFileSync(BANNED_IPS_FILE, JSON.stringify(data, null, 2));
        } catch (e) {
            console.error(`[SECURITY] Failed to save banned IPs: ${e.message}`);
        }
    }

    // Check if IP is IPv4
    isIpv4(ip) {
        return ip.includes('.') && !ip.includes(':');
    }

    // Check if IP is IPv6
    isIpv6(ip) {
        return ip.includes(':');
    }

    // Get subnet from IP (IPv4: /24, IPv6: /48)
    getSubnet(ip) {
        if (this.isIpv4(ip)) {
            const parts = ip.split('.');
            if (parts.length === 4) {
                return 'ipv4:' + parts.slice(0, 3).join('.'); // e.g., "ipv4:192.168.1"
            }
        } else if (this.isIpv6(ip)) {
            // IPv6: use /48 subnet (first 3 hextets)
            // Remove IPv6 prefix (::ffff: or leading zeros compression)
            const normalized = ip.replace(/^::ffff:/, '').replace(/^::/, '');
            const parts = normalized.split(':');
            if (parts.length >= 3) {
                return 'ipv6:' + parts.slice(0, 3).join(':'); // e.g., "ipv6:2001:db8:1"
            }
        }
        return null;
    }

    // Check if IP is banned
    isBanned(ip) {
        if (this.bannedIps.has(ip)) return true;

        // Check subnet
        const subnet = this.getSubnet(ip);
        if (subnet && this.bannedSubnets.has(subnet)) {
            const subnetData = this.bannedSubnets.get(subnet);
            if (subnetData.banned) return true;
        }

        return false;
    }

    // Record a violation for an IP
    recordViolation(ip) {
        const subnet = this.getSubnet(ip);

        // Track individual IP violations
        const count = (this.ipViolationCounts.get(ip) || 0) + 1;
        this.ipViolationCounts.set(ip, count);

        // If individual IP has 5+ violations, ban it
        if (count >= 5) {
            this.banIp(ip);
            return;
        }

        // Track subnet violations
        if (subnet) {
            const subnetCount = (this.bannedSubnets.get(subnet)?.count || 0) + 1;
            this.bannedSubnets.set(subnet, { count: subnetCount, banned: false });

            // If /24 subnet has 10+ violations from different IPs, ban the subnet
            if (subnetCount >= 10) {
                this.banSubnet(subnet);
            }
        }
    }

    // Ban an IP
    banIp(ip) {
        if (!this.bannedIps.has(ip)) {
            this.bannedIps.add(ip);
            console.log(`[SECURITY] Banned IP: ${ip}`);
            this.saveBannedIps();
        }
    }

    // Ban a subnet (IPv4: /24, IPv6: /48)
    banSubnet(subnet) {
        const existing = this.bannedSubnets.get(subnet);
        const subnetType = subnet.startsWith('ipv6') ? '/48' : '/24';
        if (!existing || !existing.banned) {
            this.bannedSubnets.set(subnet, { count: existing?.count || 0, banned: true });
            console.log(`[SECURITY] Banned subnet: ${subnet}${subnetType}`);
            this.saveBannedIps();
        }
    }

    // Unban an IP
    unbanIp(ip) {
        if (this.bannedIps.delete(ip)) {
            console.log(`[SECURITY] Unbanned IP: ${ip}`);
            this.saveBannedIps();
        }
    }

    // Unban a subnet
    unbanSubnet(subnet) {
        if (this.bannedSubnets.has(subnet)) {
            this.bannedSubnets.delete(subnet);
            const subnetType = subnet.startsWith('ipv6') ? '/48' : '/24';
            console.log(`[SECURITY] Unbanned subnet: ${subnet}${subnetType}`);
            this.saveBannedIps();
        }
    }

    // Get banned IPs summary
    getSummary() {
        const subnetBans = Array.from(this.bannedSubnets.entries())
            .filter(([_, v]) => v.banned)
            .map(([k, v]) => {
                const type = k.startsWith('ipv6') ? '/48' : '/24';
                const subnet = k.replace(/^(ipv4:|ipv6:)/, '');
                return `${subnet}${type}`;
            });

        // Separate IPv4 and IPv6 counts
        const ipv4Bans = Array.from(this.bannedIps).filter(ip => this.isIpv4(ip));
        const ipv6Bans = Array.from(this.bannedIps).filter(ip => this.isIpv6(ip));

        return {
            directBans: Array.from(this.bannedIps),
            ipv4Bans: ipv4Bans,
            ipv6Bans: ipv6Bans,
            subnetBans: subnetBans,
            violationCounts: Object.fromEntries(this.ipViolationCounts)
        };
    }

    // Daily summary and cleanup
    dailySummary() {
        const summary = this.getSummary();
        console.log('[SECURITY] Daily IP Security Summary:');
        console.log(`  - Banned IPv4 IPs: ${summary.ipv4Bans.length}`);
        console.log(`  - Banned IPv6 IPs: ${summary.ipv6Bans.length}`);
        console.log(`  - Banned subnets: ${summary.subnetBans.length}`);

        // Reset violation counts (start fresh each day)
        this.ipViolationCounts.clear();

        return summary;
    }
}

const ipBanManager = new IpBanManager();

// ============================================================
// API Key Store with IP Tracking
// ============================================================

class ApiKeyStore {
    constructor() {
        this.keys = new Map();
        this.pending = new Map(); // Pending verifications
        this.load();
        this.loadPending();
    }

    load() {
        try {
            if (fs.existsSync(CONFIG.apiKeysFile)) {
                const data = JSON.parse(fs.readFileSync(CONFIG.apiKeysFile, 'utf8'));
                this.keys = new Map(Object.entries(data));
                console.log(`[KEYSTORE] Loaded ${this.keys.size} API keys from ${CONFIG.apiKeysFile}`);
            } else if (fs.existsSync(CONFIG.apiKeysBackupFile)) {
                // Restore from backup if main file doesn't exist
                const data = JSON.parse(fs.readFileSync(CONFIG.apiKeysBackupFile, 'utf8'));
                this.keys = new Map(Object.entries(data));
                console.log(`[KEYSTORE] Restored ${this.keys.size} API keys from backup`);
                // Save to main file to restore normal operation
                this.save();
            }
        } catch (e) {
            console.log('[KEYSTORE] No existing API keys file, starting fresh');
        }
    }

    save() {
        const data = Object.fromEntries(this.keys);
        fs.writeFileSync(CONFIG.apiKeysFile, JSON.stringify(data, null, 2));
    }

    get(apiKey) {
        const entry = this.keys.get(apiKey);
        if (entry && entry.verified) {
            return entry;
        }
        return null;
    }

    set(apiKey, data) {
        this.keys.set(apiKey, data);
        this.save();
    }

    exists(apiKey) {
        const entry = this.keys.get(apiKey);
        return entry && entry.verified === true;
    }

    toolIdExists(toolId) {
        for (const [key, value] of this.keys) {
            if (value.toolId === toolId && value.verified) return true;
        }
        return false;
    }

    // Add pending registration
    addPending(apiKey, data) {
        this.pending.set(apiKey, { ...data, createdAt: Date.now() });
        this.savePending();
    }

    // Save pending to file
    savePending() {
        try {
            const pendingFile = path.join(__dirname, 'api_keys_pending.json');
            const data = Object.fromEntries(this.pending);
            fs.writeFileSync(pendingFile, JSON.stringify(data, null, 2));
        } catch (e) {
            console.error(`[KEYSTORE] Failed to save pending: ${e.message}`);
        }
    }

    // Load pending from file
    loadPending() {
        try {
            const pendingFile = path.join(__dirname, 'api_keys_pending.json');
            if (fs.existsSync(pendingFile)) {
                const data = JSON.parse(fs.readFileSync(pendingFile, 'utf8'));
                this.pending = new Map(Object.entries(data));
                // Clean expired (24h) pending registrations
                const now = Date.now();
                for (const [key, value] of this.pending) {
                    if (now - value.createdAt > 24 * 60 * 60 * 1000) {
                        this.pending.delete(key);
                    }
                }
                console.log(`[KEYSTORE] Loaded ${this.pending.size} pending registrations`);
            }
        } catch (e) {
            console.log('[KEYSTORE] No pending registrations file');
        }
    }

    // Get pending registration
    getPending(apiKey) {
        return this.pending.get(apiKey) || null;
    }

    // Verify and activate
    verifyAndActivate(apiKey, token) {
        const pending = this.pending.get(apiKey);
        if (pending && pending.token === token) {
            // Check if not expired (24 hours)
            if (Date.now() - pending.createdAt > 24 * 60 * 60 * 1000) {
                return { success: false, error: 'Verification token expired' };
            }
            // Activate
            this.keys.set(apiKey, {
                ...pending,
                verified: true,
                activatedAt: new Date().toISOString()
            });
            this.pending.delete(apiKey);
            this.save();
            this.savePending();
            return { success: true };
        }
        return { success: false, error: 'Invalid token' };
    }

    // Record registration IP
    recordRegistrationIp(apiKey, ip) {
        const pending = this.pending.get(apiKey);
        if (pending) {
            pending.registrationIp = ip;
            this.savePending();
        }
    }

    // Record API key usage IP
    recordUsageIp(apiKey, ip) {
        const entry = this.keys.get(apiKey);
        if (entry) {
            if (!entry.recentIps) {
                entry.recentIps = [];
            }
            // Add to front, keep last 3
            entry.recentIps = [ip, ...entry.recentIps.filter(i => i !== ip)].slice(0, 3);
            this.save();
        }
    }

    // Get API key info including recent IPs
    getKeyInfo(apiKey) {
        const entry = this.keys.get(apiKey);
        if (entry) {
            return {
                contactName: entry.contactName,
                websiteUrl: entry.websiteUrl,
                email: entry.email,
                toolId: entry.toolId,
                recentIps: entry.recentIps || [],
                createdOn: entry.createdOn,
                activatedAt: entry.activatedAt
            };
        }
        return null;
    }
}

const apiKeyStore = new ApiKeyStore();

// Email validation function
function isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}

// Send verification email
async function sendVerificationEmail(email, toolId, apiKey, token) {
    const verifyUrl = `${CONFIG.serverUrl}/verify?key=${apiKey}&token=${token}`;

    const emailContent = `
Subject: EntrezJS Email Verification

Hello,

You (or someone claiming to be you) has registered for EntrezJS API access with the following details:

Tool ID: ${toolId}
Email: ${email}

To activate your API key, please click the following link:
${verifyUrl}

This link will expire in 24 hours.

If you did not register for EntrezJS, please ignore this email.

Best regards,
EntrezJS Team
    `.trim();

    // Bee mode: sync to queen for email sending (queen handles SMTP)
    if (CONFIG.serverRole === 'bee' && CONFIG.queenHost) {
        console.log(`[EMAIL] Bee mode: pending registration synced to queen for email sending`);
        return true;
    }

    // If SMTP is configured, send real email (queen/standalone only)
    if (CONFIG.smtpHost && CONFIG.smtpUser && CONFIG.smtpPass) {

        // Dynamic require - only load nodemailer when needed (queen/standalone)
        let nodemailer;
        try {
            nodemailer = require('nodemailer');
        } catch (e) {
            console.log('[EMAIL] nodemailer not installed, skipping email send');
            console.log(`[EMAIL] Verify URL: ${verifyUrl}`);
            return true;
        }

        // create reusable transporter object using SMTP transport
        let transporter = nodemailer.createTransport({
            host: CONFIG.smtpHost,
            port: CONFIG.smtpPort,
            auth: {
                user: CONFIG.smtpUser,
                pass: CONFIG.smtpPass
            }
        })
        let recp_tos = [ email ] // list of receivers
        // setup e-mail data with unicode symbols
        // Use configured SMTP_FROM or fallback to smtpUser@domain
        const fromAddress = CONFIG.smtpFrom || CONFIG.smtpUser;
        let mailOptions = {
            from: fromAddress,
            to: recp_tos.join(', '),
            subject: 'From EntrezJS',
            text: emailContent,
            attachments: [
                {
                    filename: 'from-entrezjs.txt',
                    content: emailContent
                }
            ]
        }
        transporter.sendMail(mailOptions, function(error, info){
            if(error){
                console.log(error);
                console.log(CONFIG.serverUrl + '/verify?key=' + apiKey + '&token=' + token); // LC
                return false;
            }
            console.log('[SMTP] ' + info.response);
        });

    } else {
        // Log to console for development
        console.log(`[EMAIL] Verification email (dev mode):`);
        console.log(emailContent);
    }

    return true;
}

// Backup API keys with timestamp
function backupApiKeys() {
    try {
        const data = Object.fromEntries(apiKeyStore.keys);
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const backupFile = path.join(__dirname, `api_keys.backup.${timestamp}.json`);
        fs.writeFileSync(backupFile, JSON.stringify(data, null, 2));
        console.log(`[BACKUP] API keys backed up to ${backupFile}`);
    } catch (e) {
        console.error(`[BACKUP] Failed to backup API keys: ${e.message}`);
    }
}

// Schedule periodic backup (Queen only, every 24 hours)
if (CONFIG.serverRole === 'queen') {
    setInterval(backupApiKeys, CONFIG.backupInterval);
}

// Backup on normal shutdown (Queen only)
if (CONFIG.serverRole === 'queen') {
    process.on('SIGINT', () => {
        console.log('\n[SHUTDOWN] Saving backup before exit...');
        backupApiKeys();
        process.exit(0);
    });

    process.on('SIGTERM', () => {
        console.log('\n[SHUTDOWN] Saving backup before exit...');
        backupApiKeys();
        process.exit(0);
    });
}

// ============================================================
// Daily Trending Data Collection
// ============================================================

const trendingData = {
    topSearchTerms: [],
    topPmids: [],
    lastUpdated: null
};

// Load trending data from file
function loadTrendingData() {
    try {
        if (fs.existsSync(CONFIG.trendingFile)) {
            const data = JSON.parse(fs.readFileSync(CONFIG.trendingFile, 'utf8'));
            Object.assign(trendingData, data);
            console.log(`[TRENDING] Loaded trending data from ${CONFIG.trendingFile}`);
        }
    } catch (e) {
        console.log('[TRENDING] No trending data file, will fetch new data');
    }
}

// Save trending data to file
function saveTrendingData() {
    try {
        fs.writeFileSync(CONFIG.trendingFile, JSON.stringify(trendingData, null, 2));
        console.log(`[TRENDING] Saved trending data to ${CONFIG.trendingFile}`);
    } catch (e) {
        console.error(`[TRENDING] Failed to save trending data: ${e.message}`);
    }
}

// Fetch top 10 search terms from PubMed
async function fetchTopSearchTerms() {
    try {
        // Use common search terms - in production, you'd track actual searches
        // For now, use popular biomedical topics
        const popularTerms = [
            'COVID-19',
            'cancer',
            'diabetes',
            'Alzheimer',
            'HIV',
            'influenza',
            'vaccine',
            'CRISPR',
            'coronavirus',
            'Parkinson'
        ];

        const results = [];
        for (const term of popularTerms) {
            try {
                const searchResult = await ncbiRequest('esearch', {
                    db: 'pubmed',
                    term: term,
                    retmode: 'xml',
                    retmax: '1'
                });
                const count = parseInt(searchResult.Count) || 0;
                results.push({ term, count });
            } catch (e) {
                // Continue on individual term failure
            }
        }

        // Sort by count descending
        results.sort((a, b) => b.count - a.count);
        return results.slice(0, 10);
    } catch (e) {
        console.error(`[TRENDING] Failed to fetch top search terms: ${e.message}`);
        return trendingData.topSearchTerms; // Return old data on failure
    }
}

// Fetch top 10 recent PMIDs
async function fetchTopPmids() {
    try {
        // Get most recent PubMed articles
        const searchResult = await ncbiRequest('esearch', {
            db: 'pubmed',
            term: 'latest[dp]',
            retmode: 'xml',
            retmax: '10',
            sort: 'pub_date'
        });

        const pmids = searchResult.IdList || [];
        const results = [];

        // Get details for each PMID
        if (pmids.length > 0) {
            const summaryResult = await ncbiRequest('esummary', {
                db: 'pubmed',
                id: pmids.join(','),
                retmode: 'xml'
            });

            for (const pmid of pmids) {
                if (summaryResult[pmid]) {
                    const item = summaryResult[pmid];
                    results.push({
                        pmid: pmid,
                        title: item.Title || '',
                        source: item.Source || '',
                        pubdate: item.PubDate || ''
                    });
                }
            }
        }

        return results;
    } catch (e) {
        console.error(`[TRENDING] Failed to fetch top PMIDs: ${e.message}`);
        return trendingData.topPmids;
    }
}

// Post to Slack webhook (Queen only)
async function postToSlack() {
    const webhookUrl = process.env.SLACK_WEBHOOK_URL;
    if (!webhookUrl) {
        return;
    }

    // Check if there's data to post
    if ((!trendingData.topSearchTerms || trendingData.topSearchTerms.length === 0) &&
        (!trendingData.topPmids || trendingData.topPmids.length === 0)) {
        console.log('[SLACK] No trending data to post');
        return;
    }

    const message = {
        text: '📊 *EntrezJS Daily Trending Update*',
        blocks: [
            {
                type: 'header',
                text: { type: 'plain_text', text: '📊 EntrezJS Daily Trending Update' }
            },
            {
                type: 'section',
                text: {
                    type: 'mrkdwn',
                    text: `*Last Updated:* ${new Date().toISOString()}`
                }
            },
            {
                type: 'divider'
            },
            {
                type: 'section',
                text: {
                    type: 'mrkdwn',
                    text: '*🔥 Top 10 Search Terms*' + '\n' +
                        trendingData.topSearchTerms.map((t, i) =>
                            `${i + 1}. ${t.term} (${parseInt(t.count).toLocaleString()} results)`
                        ).join('\n')
                }
            },
            {
                type: 'divider'
            },
            {
                type: 'section',
                text: {
                    type: 'mrkdwn',
                    text: '*📄 Top 10 Recent PMIDs*' + '\n' +
                        trendingData.topPmids.map((p, i) =>
                            `${i + 1}. PMID: ${p.pmid} - ${p.title.substring(0, 50)}...`
                        ).join('\n')
                }
            }
        ]
    };

    return new Promise((resolve, reject) => {
        const parsedUrl = new URL(webhookUrl);
        const options = {
            hostname: parsedUrl.hostname,
            path: parsedUrl.pathname + parsedUrl.search,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 200) {
                    console.log('[SLACK] Posted trending data to Slack');
                    resolve();
                } else {
                    console.error(`[SLACK] Failed to post: ${res.statusCode}`);
                    reject(new Error(`Slack error: ${res.statusCode}`));
                }
            });
        });

        req.on('error', (e) => {
            console.error(`[SLACK] Error posting: ${e.message}`);
            reject(e);
        });

        req.write(JSON.stringify(message));
        req.end();
    });
}

// Update trending data
async function updateTrendingData() {
    console.log('[TRENDING] Updating trending data...');

    try {
        trendingData.topSearchTerms = await fetchTopSearchTerms();
        trendingData.topPmids = await fetchTopPmids();
        trendingData.lastUpdated = new Date().toISOString();

        saveTrendingData();
        console.log('[TRENDING] Trending data updated successfully');

        // Post to Slack
        await postToSlack();
    } catch (e) {
        console.error(`[TRENDING] Failed to update trending data: ${e.message}`);
    }
}

// Schedule trending data update (Queen only)
if (CONFIG.serverRole === 'queen') {
    loadTrendingData();
    setInterval(updateTrendingData, CONFIG.trendingUpdateInterval);

    // Initial update after 5 seconds (to not block startup)
    setTimeout(updateTrendingData, 5000);

    // Daily security summary - run every 24 hours
    setInterval(() => {
        const summary = ipBanManager.dailySummary();
        console.log('[SECURITY] Daily IP summary:', summary);
    }, 24 * 60 * 60 * 1000);
}

// Backup on uncaught exception
process.on('uncaughtException', (err) => {
    console.error('\n[CRASH] Uncaught exception, saving backup...');
    console.error(err);
    backupApiKeys();
    process.exit(1);
});

// Allowed parameters for each endpoint
const ALLOWED_PARAMS = {
    espell: ['db', 'term'],
    einfo: ['db'],
    esearch: ['db', 'term', 'field', 'reldate', 'mindate', 'maxdate', 'datetype', 'retstart', 'retmax', 'rettype', 'sort'],
    esummary: ['db', 'id', 'retstart', 'retmax'],
    efetch: ['db', 'id', 'report', 'dispstart', 'dispmax'],
    elink: ['db', 'id', 'reldate', 'mindate', 'maxdate', 'datetype', 'term', 'retmode', 'dbfrom', 'cmd', 'holding', 'version']
};

const DEFAULT_PARAMS = {
    esearch: { retmode: 'xml' },
    esummary: { retmode: 'xml' },
    elink: { retmode: 'xml' },
    efetch: { mode: 'xml', rettype: 'xml' }
};

// Filter and prepare parameters
function filterParams(fnName, query) {
    const allowed = ALLOWED_PARAMS[fnName] || [];
    const defaults = DEFAULT_PARAMS[fnName] || {};
    const result = {};

    for (const key of allowed) {
        if (query[key] !== undefined) {
            result[key] = query[key];
        } else if (defaults[key] !== undefined) {
            result[key] = defaults[key];
        }
    }

    return result;
}

// Check API key
function checkApiKey(req, res, callback) {
    const clientIp = req.socket.remoteAddress || req.connection.remoteAddress;

    // Check if IP is banned
    if (ipBanManager.isBanned(clientIp)) {
        return sendJsonResponse(res, { error: 'Your IP has been banned' }, 403);
    }

    const urlObj = new URL(req.url, `http://${req.headers.host}`);
    const query = Object.fromEntries(urlObj.searchParams);
    const apiKey = query.apikey;

    if (!apiKey) {
        return sendJsonResponse(res, { error: 'No API key was supplied' }, 403);
    }

    const reg = apiKeyStore.get(apiKey);
    if (!reg) {
        // Record violation for invalid API key attempt
        ipBanManager.recordViolation(clientIp);
        return sendJsonResponse(res, { error: 'No API key matching the supplied value was found' }, 403);
    }

    // Record this IP as used by the API key
    apiKeyStore.recordUsageIp(apiKey, clientIp);

    req.entrezajax_developer_registration = reg;
    callback(query);
}

// ============================================================
// API Key rotation mechanism - adaptive based on request rate
// ============================================================

let currentKeyIndex = 0;
const keyUsageCount = new Map();

// Request rate tracking
const requestTimestamps = [];
const RATE_WINDOW_MS = 1000; // 1 second window
const API_KEY_THRESHOLD = 5; // Use API keys when > 5 req/s (NCBI allows 10/s with key)

// Track request timestamps for rate calculation
function recordRequest() {
    const now = Date.now();
    requestTimestamps.push(now);

    // Remove timestamps older than the window
    while (requestTimestamps.length > 0 && now - requestTimestamps[0] > RATE_WINDOW_MS) {
        requestTimestamps.shift();
    }
}

// Get current request rate (requests per second)
function getRequestRate() {
    return requestTimestamps.length;
}

// Check if we should use API keys based on rate
function shouldUseApiKeys() {
    return getRequestRate() > API_KEY_THRESHOLD && CONFIG.entrezApiKeys.length > 0;
}

function getNextApiKey() {
    // Don't use API keys if rate is low (save for when needed)
    if (!shouldUseApiKeys()) {
        return null;
    }

    if (CONFIG.entrezApiKeys.length === 0) {
        return null;
    }

    // Round-robin rotation
    const key = CONFIG.entrezApiKeys[currentKeyIndex];
    currentKeyIndex = (currentKeyIndex + 1) % CONFIG.entrezApiKeys.length;

    // Track usage
    const count = keyUsageCount.get(key) || 0;
    keyUsageCount.set(key, count + 1);

    return key;
}

function getKeyUsageStats() {
    const stats = {};
    for (const [key, count] of keyUsageCount) {
        stats[key.substring(0, 8) + '...'] = count;
    }
    return stats;
}

// Make request to NCBI E-utilities
async function ncbiRequest(entrezFunction, params) {
    // Record this request for rate tracking
    recordRequest();

    // Use API key rotation only when rate is high
    const apiKey = shouldUseApiKeys() ? getNextApiKey() : null;
    if (apiKey) {
        params.api_key = apiKey;
    }

    const paramString = Object.entries(params)
        .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
        .join('&');

    const requestUrl = `${CONFIG.entrezBaseUrl}/${entrezFunction}.cgi?${paramString}`;

    return new Promise((resolve, reject) => {
        const parsedUrl = new URL(requestUrl);
        const options = {
            hostname: parsedUrl.hostname,
            path: parsedUrl.pathname + parsedUrl.search,
            method: 'GET',
            headers: {
                'User-Agent': 'EntrezJS/2.0'
            }
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    const result = parseXmlToJson(data);
                    resolve(result);
                } catch (e) {
                    reject(e);
                }
            });
        });

        req.on('error', reject);
        req.end();
    });
}

// Parse NCBI XML response to JSON (simplified - handles common response types)
function parseXmlToJson(xml) {
    // Handle JSON responses directly (if retmode=json)
    try {
        return JSON.parse(xml);
    } catch (e) {
        // Not JSON, try simple XML parsing for common structures
    }

    // Simple XML parsing for Entrez responses
    const result = {};

    // Handle eSearchResult
    const esearchMatch = xml.match(/<eSearchResult>([\s\S]*?)<\/eSearchResult>/);
    if (esearchMatch) {
        const inner = esearchMatch[1];
        result.Count = extractXmlValue(inner, 'Count');
        result.Retranslate = extractXmlValue(inner, 'Retranslate');
        result.IdList = extractXmlValues(inner, 'Id');
        result.QueryTranslation = extractXmlValue(inner, 'QueryTranslation');
        result.RetMax = extractXmlValue(inner, 'RetMax') || result.IdList?.length || 0;
        result.RetStart = extractXmlValue(inner, 'RetStart') || '0';
        result.SearchResults = extractXmlValue(inner, 'SearchResults');
        return result;
    }

    // Handle eSummaryResult
    const summaryMatch = xml.match(/<eSummaryResult>([\s\S]*?)<\/eSummaryResult>/);
    if (summaryMatch) {
        const inner = summaryMatch[1];
        const docsumMatches = inner.match(/<DocSum>[\s\S]*?<\/DocSum>/g) || [];
        const results = {};
        for (const docsum of docsumMatches) {
            const id = extractXmlValue(docsum, 'Id');
            const items = {};
            const itemMatches = docsum.match(/<Item Name="([^"]+)" Type="([^"]+)">([\s\S]*?)<\/Item>/g) || [];
            for (const item of itemMatches) {
                const nameMatch = item.match(/Name="([^"]+)"/);
                const typeMatch = item.match(/Type="([^"]+)"/);
                const valueMatch = item.match(/>([^<]+)<\/Item>/);
                if (nameMatch && valueMatch) {
                    items[nameMatch[1]] = {
                        Type: typeMatch ? typeMatch[1] : 'String',
                        Content: valueMatch[1]
                    };
                }
            }
            results[id] = items;
        }
        return results;
    }

    // Handle eLinkResult
    const elinkMatch = xml.match(/<eLinkResult>([\s\S]*?)<\/eLinkResult>/);
    if (elinkMatch) {
        const inner = elinkMatch[1];
        const linkSetDbMatch = inner.match(/<LinkSetDb>([\s\S]*?)<\/LinkSetDb>/g);
        if (linkSetDbMatch) {
            const linkSetDbs = {};
            for (const lsdb of linkSetDbMatch) {
                const dbTo = extractXmlValue(lsdb, 'DbTo');
                const links = extractXmlValues(lsdb, 'Id');
                if (dbTo) {
                    linkSetDbs[dbTo] = links;
                }
            }
            if (Object.keys(linkSetDbs).length > 0) {
                return linkSetDbs;
            }
        }
        // Check for LinkSet
        const linkSetMatch = inner.match(/<LinkSet>([\s\S]*?)<\/LinkSet>/g);
        if (linkSetMatch) {
            const results = {};
            for (const ls of linkSetMatch) {
                const dbFrom = extractXmlValue(ls, 'DbFrom');
                const dbTo = extractXmlValue(ls, 'DbTo');
                const links = extractXmlValues(ls, 'Id');
                if (dbFrom && dbTo) {
                    results[`${dbFrom}_to_${dbTo}`] = links;
                }
            }
            return results;
        }
    }

    // Handle eInfo
    const einfoMatch = xml.match(/<eInfoResult>([\s\S]*?)<\/eInfoResult>/);
    if (!einfoMatch) {
        const dbinfoMatch = xml.match(/<DbInfo ([\s\S]*?)\/>/);
        if (dbinfoMatch) {
            return { DbInfo: parseXmlAttributes(dbinfoMatch[1]) };
        }
    } else {
        const inner = einfoMatch[1];
        const dbList = extractXmlValues(inner, 'DbName');
        const dbInfos = {};
        const dbinfoMatches = inner.match(/<DbInfo ([\s\S]*?)\/>/g) || [];
        for (const dbi of dbinfoMatches) {
            const attrs = parseXmlAttributes(dbi);
            const dbName = attrs.DbName;
            if (dbName) {
                delete attrs.DbName;
                dbInfos[dbName] = attrs;
            }
        }
        return { DbList: dbList, DbInfo: dbInfos };
    }

    // Handle eSpell
    const espellMatch = xml.match(/<eSpellResult>([\s\S]*?)<\/eSpellResult>/);
    if (espellMatch) {
        const inner = espellMatch[1];
        const suggestions = {};
        const suggestionsMatch = inner.match(/<Suggestion>[\s\S]*?<\/Suggestion>/g) || [];
        for (const s of suggestionsMatch) {
            const db = extractXmlValue(s, 'Db');
            const term = extractXmlValue(s, 'Term');
            if (db) {
                suggestions[db] = term;
            }
        }
        return { Suggestion: suggestions };
    }

    // For efetch, return raw XML
    return xml;
}

function extractXmlValue(xml, tag) {
    const match = xml.match(new RegExp(`<${tag}>([^<]*)</${tag}>`));
    return match ? match[1] : null;
}

function extractXmlValues(xml, tag) {
    const regex = new RegExp(`<${tag}>([^<]*)</${tag}>`, 'g');
    const matches = [];
    let match;
    while ((match = regex.exec(xml)) !== null) {
        matches.push(match[1]);
    }
    return matches;
}

function parseXmlAttributes(attrString) {
    const result = {};
    const regex = /(\w+)="([^"]*)"/g;
    let match;
    while ((match = regex.exec(attrString)) !== null) {
        result[match[1]] = match[2];
    }
    return result;
}

// Execute Entrez function with caching
async function executeAndCache(fnName, reg, params) {
    const cacheKey = entrezCache.createCacheKey(fnName, params);
    const cached = entrezCache.get(cacheKey);
    if (cached) {
        console.log(`[CACHE HIT] ${fnName}: ${cacheKey}`);
        return cached;
    }

    console.log(`[CACHE MISS] ${fnName}: ${cacheKey}`);
    const result = await ncbiRequest(fnName, {
        ...params,
        email: reg.email,
        tool: reg.toolId,
        usehistory: 'y'
    });

    entrezCache.set(cacheKey, result, CONFIG.cacheTime);
    return result;
}

// Paginate ID list
function paginate(query, idList) {
    const start = parseInt(query.start) || 0;
    const max = parseInt(query.max) || 20;
    return idList.slice(start, start + max);
}

// Send JSON response with optional JSONP
function sendJsonResponse(res, data, statusCode = 200, query = {}) {
    const result = { entrezjs: { error: false }, ...data };
    const callback = query.callback;
    let body;

    if (callback) {
        body = `${callback}(${JSON.stringify(result)})`;
        res.setHeader('Content-Type', 'application/javascript');
    } else {
        body = JSON.stringify(result);
        res.setHeader('Content-Type', 'application/json');
    }

    res.writeHead(statusCode);
    res.end(body);
}

// Send error response
function sendError(res, message, statusCode = 500) {
    const result = { entrezjs: { error: true, message: message } };
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(result));
}

// Route handlers
async function handleEspell(req, res, query) {
    const params = filterParams('espell', query);
    const result = await executeAndCache('espell', req.entrezajax_developer_registration, params);
    sendJsonResponse(res, { result: result }, 200, query);
}

async function handleEinfo(req, res, query) {
    const params = filterParams('einfo', query);
    const result = await executeAndCache('einfo', req.entrezajax_developer_registration, params);
    sendJsonResponse(res, { result: result }, 200, query);
}

async function handleEsearch(req, res, query) {
    const params = filterParams('esearch', query);
    const result = await executeAndCache('esearch', req.entrezajax_developer_registration, params);
    sendJsonResponse(res, { result: result }, 200, query);
}

async function handleEsummary(req, res, query) {
    const params = filterParams('esummary', query);
    const result = await executeAndCache('esummary', req.entrezajax_developer_registration, params);
    sendJsonResponse(res, { result: result }, 200, query);
}

async function handleEfetch(req, res, query) {
    const params = filterParams('efetch', query);
    const result = await executeAndCache('efetch', req.entrezajax_developer_registration, params);
    sendJsonResponse(res, { result: result }, 200, query);
}

async function handleElink(req, res, query) {
    const params = filterParams('elink', query);
    const result = await executeAndCache('elink', req.entrezajax_developer_registration, params);
    sendJsonResponse(res, { result: result }, 200, query);
}

// Combined handlers
async function handleEsearchAndEsummary(req, res, query) {
    const reg = req.entrezajax_developer_registration;
    const searchParams = filterParams('esearch', query);
    const searchResult = await executeAndCache('esearch', reg, searchParams);

    if (!searchResult.IdList || searchResult.IdList.length === 0) {
        return sendJsonResponse(res, { result: searchResult }, 200, query);
    }

    const summaryParams = filterParams('esummary', query);
    const idList = paginate(query, searchResult.IdList);
    summaryParams.id = idList.join(',');

    const summaryResult = await executeAndCache('esummary', reg, summaryParams);
    sendJsonResponse(res, {
        result: summaryResult,
        entrezjs: { error: false }
    }, 200, query);
}

async function handleEsearchAndEfetch(req, res, query) {
    const reg = req.entrezajax_developer_registration;
    const searchParams = filterParams('esearch', query);
    const searchResult = await executeAndCache('esearch', reg, searchParams);

    if (!searchResult.IdList || searchResult.IdList.length === 0) {
        return sendJsonResponse(res, { result: searchResult }, 200, query);
    }

    const fetchParams = filterParams('efetch', query);
    const idList = paginate(query, searchResult.IdList);
    fetchParams.id = idList.join(',');

    const fetchResult = await executeAndCache('efetch', reg, fetchParams);
    sendJsonResponse(res, { result: fetchResult }, 200, query);
}

async function handleEsearchAndElink(req, res, query) {
    const reg = req.entrezajax_developer_registration;
    const searchParams = filterParams('esearch', query);
    const searchResult = await executeAndCache('esearch', reg, searchParams);

    if (!searchResult.IdList || searchResult.IdList.length === 0) {
        return sendJsonResponse(res, { result: searchResult }, 200, query);
    }

    const linkParams = filterParams('elink', query);
    linkParams.id = searchResult.IdList.join(',');

    const linkResult = await executeAndCache('elink', reg, linkParams);
    sendJsonResponse(res, { result: linkResult }, 200, query);
}

async function handleElinkAndEsummary(req, res, query) {
    const reg = req.entrezajax_developer_registration;
    const linkParams = filterParams('elink', query);
    const linkResult = await executeAndCache('elink', reg, linkParams);

    // Extract ID list from link result (new dict format)
    let idList = [];
    if (linkResult && typeof linkResult === 'object') {
        for (const db of Object.keys(linkResult)) {
            idList = idList.concat(linkResult[db]);
        }
    }

    if (!idList || idList.length === 0) {
        return sendJsonResponse(res, { result: linkResult }, 200, query);
    }

    const idListLen = idList.length;
    idList = paginate(query, idList);

    const summaryParams = filterParams('esummary', query);
    summaryParams.id = idList.join(',');

    const summaryResult = await executeAndCache('esummary', reg, summaryParams);
    sendJsonResponse(res, { result: summaryResult, count: idListLen }, 200, query);
}

async function handleElinkAndEfetch(req, res, query) {
    const reg = req.entrezajax_developer_registration;
    const linkParams = filterParams('elink', query);
    const linkResult = await executeAndCache('elink', reg, linkParams);

    // Extract ID list from link result (new dict format)
    let idList = [];
    if (linkResult && typeof linkResult === 'object') {
        for (const db of Object.keys(linkResult)) {
            idList = idList.concat(linkResult[db]);
        }
    }

    if (!idList || idList.length === 0) {
        return sendJsonResponse(res, { result: linkResult }, 200, query);
    }

    const idListLen = idList.length;
    idList = paginate(query, idList);

    const fetchParams = filterParams('efetch', query);
    fetchParams.id = idList.join(',');

    const fetchResult = await executeAndCache('efetch', reg, fetchParams);
    sendJsonResponse(res, { result: fetchResult, count: idListLen }, 200, query);
}

// Register new developer - shows HTML form
function handleRegister(req, res) {
    const clientIp = req.socket.remoteAddress || req.connection.remoteAddress;
    const method = req.method;

    // Check if IP is banned
    if (ipBanManager.isBanned(clientIp)) {
        return sendError(res, 'Your IP has been banned', 403);
    }

    // GET request - show registration form
    if (method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Register - EntrezJS</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 40px auto; padding: 20px; line-height: 1.6; }
        h1 { color: #333; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        input { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
        button { background: #007bff; color: white; padding: 12px 24px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
        button:hover { background: #0056b3; }
        .note { background: #f8f9fa; padding: 15px; border-radius: 4px; margin-top: 20px; }
        .examples { background: #f0f0f0; padding: 20px; border-radius: 4px; margin-top: 30px; }
        code { background: #e9ecef; padding: 2px 6px; border-radius: 3px; }
        pre { background: #2d2d2d; color: #f8f8f2; padding: 15px; border-radius: 4px; overflow-x: auto; }
    </style>
</head>
<body>
    <h1>EntrezJS Registration</h1>
    <p>Register to get an API key for accessing NCBI Entrez databases.</p>

    <form method="POST" action="/register">
        <div class="form-group">
            <label for="tool_id">Tool ID *</label>
            <input type="text" id="tool_id" name="tool_id" placeholder="e.g., myapp, mywebsite" required>
            <small>A unique identifier for your application</small>
        </div>

        <div class="form-group">
            <label for="contact_name">Contact Name *</label>
            <input type="text" id="contact_name" name="contact_name" placeholder="Your name" required>
        </div>

        <div class="form-group">
            <label for="email">Email Address *</label>
            <input type="email" id="email" name="email" placeholder="your@email.com" required>
            <small>Verification link will be sent to this email</small>
        </div>

        <div class="form-group">
            <label for="website_url">Website URL *</label>
            <input type="url" id="website_url" name="website_url" placeholder="https://example.com" required>
        </div>

        <button type="submit">Register</button>
    </form>

    <div class="note">
        <strong>Note:</strong> After registration, you will receive a verification email. Click the link in the email to activate your API key.
    </div>

    <div class="examples">
        <h2>Usage Examples</h2>

        <h3>1. esearch + esummary (Combined)</h3>
        <p>Search PubMed and get summaries in a single request:</p>
        <pre>/esearch+esummary?apikey=YOUR_API_KEY&db=pubmed&term=cancer</pre>

        <h3>2. esearch + efetch (Combined)</h3>
        <p>Search and fetch full records:</p>
        <pre>/esearch+efetch?apikey=YOUR_API_KEY&db=pubmed&term=cancer&rettype=abstract&retmode=text</pre>

        <h3>3. Separate Requests</h3>
        <p>Step 1 - Search:</p>
        <pre>/esearch?apikey=YOUR_API_KEY&db=pubmed&term=cancer&retmax=5</pre>
        <p>Step 2 - Get summaries:</p>
        <pre>/esummary?apikey=YOUR_API_KEY&db=pubmed&id=38354089,38354088</pre>
        <p>Step 3 - Fetch details:</p>
        <pre>/efetch?apikey=YOUR_API_KEY&db=pubmed&id=38354089&rettype=abstract&retmode=text</pre>

        <h3>4. Other Databases</h3>
        <p>Protein:</p>
        <pre>/esearch+esummary?apikey=YOUR_API_KEY&db=protein&term=BRCA1&retmax=3</pre>
        <p>Nucleotide:</p>
        <pre>/esearch+efetch?apikey=YOUR_API_KEY&db=nucleotide&term=BRCA1&rettype=fasta&retmode=text</pre>

        <h3>5. JSONP Callback</h3>
        <pre>/esearch+esummary?apikey=YOUR_API_KEY&db=pubmed&term=cancer&callback=myCallback</pre>
    </div>
</body>
</html>
        `);
        return;
    }

    // POST request - process form submission
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
        try {
            const data = querystring.parse(body);
            const { contact_name, website_url, email, tool_id } = data;

            // Validation
            if (!contact_name || !website_url || !email || !tool_id) {
                return sendError(res, 'All fields are required', 400);
            }

            // Email validation
            if (!isValidEmail(email)) {
                return sendError(res, 'Invalid email address format', 400);
            }

            if (apiKeyStore.toolIdExists(tool_id)) {
                return sendError(res, 'A tool with this name has already been registered!', 400);
            }

            // Generate API key and verification token
            const apiKey = crypto.createHash('md5').update(tool_id).digest('hex');
            const token = crypto.randomBytes(32).toString('hex');

            // Store as pending (not verified yet) - include registration IP
            apiKeyStore.addPending(apiKey, {
                contactName: contact_name,
                websiteUrl: website_url,
                email: email,
                toolId: tool_id,
                token: token,
                registrationIp: clientIp
            });

            // Send verification email (queen/standalone sends directly, bee syncs to queen)
            if (CONFIG.serverRole === 'bee' && currentQueen.host) {
                // Bee: sync pending registration to queen, queen will send email
                try {
                    const syncData = {
                        pendingRegistrations: [{
                            apiKey: apiKey,
                            toolId: tool_id,
                            email: email,
                            contactName: contact_name,
                            websiteUrl: website_url,
                            token: token,
                            registrationIp: clientIp
                        }]
                    };

                    await httpRequest(
                        currentQueen.host,
                        currentQueen.port,
                        'POST',
                        `/internal/sync-pending?bee=${CONFIG.serverCodename}`,
                        JSON.stringify(syncData),
                        true,
                        CONFIG.queenHttps
                    );
                    console.log(`[REGISTER] Synced pending registration to queen for ${tool_id}`);
                } catch (e) {
                    console.error(`[REGISTER] Failed to sync to queen: ${e.message}`);
                    console.log(CONFIG.serverUrl + '/verify?key=' + apiKey + '&token=' + token); // LC
                }
            } else {
                // Queen/standalone: send verification email directly
                await sendVerificationEmail(email, tool_id, apiKey, token);
            }

            console.log(`[REGISTER] Pending registration: ${tool_id} -> ${apiKey} from IP: ${clientIp}`);

            // Return success HTML page
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            res.end(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Registration Successful - EntrezJS</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 40px auto; padding: 20px; line-height: 1.6; }
        h1 { color: #28a745; }
        .success-box { background: #d4edda; border: 1px solid #c3e6cb; padding: 20px; border-radius: 4px; margin: 20px 0; }
        .warning-box { background: #fff3cd; border: 1px solid #ffeeba; padding: 15px; border-radius: 4px; margin: 20px 0; }
        .examples { background: #f0f0f0; padding: 20px; border-radius: 4px; margin-top: 30px; }
        code { background: #e9ecef; padding: 2px 6px; border-radius: 3px; }
        pre { background: #2d2d2d; color: #f8f8f2; padding: 15px; border-radius: 4px; overflow-x: auto; }
        a { color: #007bff; }
    </style>
</head>
<body>
    <h1>Registration Successful!</h1>

    <div class="success-box">
        <strong>API Key (pending):</strong> <code>${apiKey}</code>
    </div>

    <div class="warning-box">
        <strong>Please check your email!</strong>
        <p>A verification link has been sent to <strong>${email}</strong>.</p>
        <p>Click the link in the email to activate your API key. The link will expire in 24 hours.</p>
    </div>

    <h2>What to do next</h2>
    <ol>
        <li>Check your email inbox (and spam folder) for the verification message</li>
        <li>Click the verification link</li>
        <li>Once verified, use your API key to access the Entrez API</li>
    </ol>

    <div class="examples">
        <h2>Quick Start Examples</h2>

        <h3>1. esearch + esummary (Combined)</h3>
        <p>Search PubMed and get summaries in one request:</p>
        <pre>/esearch+esummary?apikey=${apiKey}&db=pubmed&term=cancer</pre>

        <h3>2. esearch + efetch (Combined)</h3>
        <p>Search and fetch full records:</p>
        <pre>/esearch+efetch?apikey=${apiKey}&db=pubmed&term=cancer&rettype=abstract&retmode=text</pre>

        <h3>3. JSONP Callback</h3>
        <pre>/esearch+esummary?apikey=${apiKey}&db=pubmed&term=cancer&callback=myCallback</pre>

        <h3>4. More Examples</h3>
        <p>See more examples at: <a href="/register">/register</a></p>
    </div>
</body>
</html>
            `);
        } catch (e) {
            sendError(res, e.message, 500);
        }
    });
}

// Verify email
async function handleVerify(req, res, query) {
    const apiKey = query.key;
    const token = query.token;

    if (!apiKey || !token) {
        return sendError(res, 'Missing key or token', 400);
    }

    const result = apiKeyStore.verifyAndActivate(apiKey, token);

    if (result.success) {
        console.log(`[VERIFY] API key verified: ${apiKey}`);

        // If bee, sync verified key to queen
        if (CONFIG.serverRole === 'bee' && currentQueen.host) {
            const keyData = apiKeyStore.keys.get(apiKey);
            if (keyData) {
                try {
                    const syncData = {
                        apiKeys: [{
                            key: apiKey,
                            toolId: keyData.toolId,
                            email: keyData.email
                        }]
                    };
                    await httpRequest(
                        currentQueen.host,
                        currentQueen.port,
                        'POST',
                        `/internal/sync?bee=${CONFIG.serverCodename}`,
                        JSON.stringify(syncData),
                        true,
                        CONFIG.queenHttps
                    );
                    console.log(`[VERIFY] Synced verified key to queen: ${keyData.toolId}`);
                } catch (e) {
                    console.error(`[VERIFY] Failed to sync to queen: ${e.message}`);
                }
            }
        }

        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`
<!DOCTYPE html>
<html>
<head>
    <title>Email Verified - EntrezJS</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
        .success { color: green; }
    </style>
</head>
<body>
    <h1>Email Verified!</h1>
    <p class="success">Your EntrezJS account has been successfully verified.</p>
    <p>Your API key: <code>${apiKey}</code></p>
    <p>You can now use the API.</p>
</body>
</html>
        `);
    } else {
        res.writeHead(400, { 'Content-Type': 'text/html' });
        res.end(`
<!DOCTYPE html>
<html>
<head>
    <title>Verification Failed - EntrezJS</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
        .error { color: red; }
    </style>
</head>
<body>
    <h1>Verification Failed</h1>
    <p class="error">${result.error}</p>
</body>
</html>
        `);
    }
}

// Handle static files
function handleStatic(req, res) {
    const staticDir = path.join(__dirname, 'static');
    const filePath = path.join(staticDir, req.url === '/' ? 'index.html' : req.url);

    fs.readFile(filePath, (err, data) => {
        if (err) {
            res.writeHead(404);
            res.end('Not Found');
            return;
        }

        const ext = path.extname(filePath);
        const contentTypes = {
            '.html': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.json': 'application/json',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.gif': 'image/gif'
        };

        res.writeHead(200, { 'Content-Type': contentTypes[ext] || 'text/plain' });
        res.end(data);
    });
}

// Main request handler
async function handleRequest(req, res) {
    const urlObj = new URL(req.url, `http://${req.headers.host}`);
    const pathname = urlObj.pathname.replace(/\/+$/, '') || '/';
    const query = Object.fromEntries(urlObj.searchParams);

    console.log(`[REQUEST] ${pathname}`);

    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    // Security headers
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('X-XSS-Protection', '1; mode=block');

    if (req.method === 'OPTIONS') {
        res.writeHead(200);
        res.end();
        return;
    }

    // Rate limiting
    const clientIp = req.socket.remoteAddress || req.connection.remoteAddress;
    if (!checkRateLimit(clientIp)) {
        res.writeHead(429, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Too many requests. Please try again later.' }));
        return;
    }

    // Handle internal distributed system routes
    if (pathname.startsWith('/internal/')) {
        handleInternalRequest(req, res, pathname, query);
        return;
    }

    try {
        // Routes
        switch (pathname) {
            case '/espell':
                checkApiKey(req, res, q => handleEspell(req, res, q));
                break;

            case '/einfo':
                checkApiKey(req, res, q => handleEinfo(req, res, q));
                break;

            case '/esearch':
                checkApiKey(req, res, q => handleEsearch(req, res, q));
                break;

            case '/esummary':
                checkApiKey(req, res, q => handleEsummary(req, res, q));
                break;

            case '/efetch':
                checkApiKey(req, res, q => handleEfetch(req, res, q));
                break;

            case '/elink':
                checkApiKey(req, res, q => handleElink(req, res, q));
                break;

            case '/esearch+esummary':
                checkApiKey(req, res, q => handleEsearchAndEsummary(req, res, q));
                break;

            case '/esearch+efetch':
                checkApiKey(req, res, q => handleEsearchAndEfetch(req, res, q));
                break;

            case '/esearch+elink':
                checkApiKey(req, res, q => handleEsearchAndElink(req, res, q));
                break;

            case '/elink+esummary':
                checkApiKey(req, res, q => handleElinkAndEsummary(req, res, q));
                break;

            case '/elink+efetch':
                checkApiKey(req, res, q => handleElinkAndEfetch(req, res, q));
                break;

            case '/register':
                // All roles can handle registration (queen, standalone, bee)
                handleRegister(req, res);
                break;

            case '/verify':
                // All roles can handle verification
                handleVerify(req, res, query);
                break;

            case '/status/memcache':
                const stats = entrezCache.getStats();
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(stats));
                break;

            case '/status/trending':
                if (CONFIG.serverRole !== 'queen' && CONFIG.serverRole !== 'standalone') {
                    res.writeHead(503);
                    res.end(JSON.stringify({ error: 'Trending only available on Queen server' }));
                    return;
                }
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(trendingData, null, 2));
                break;

            case '/status/keys':
                if (CONFIG.serverRole !== 'queen' && CONFIG.serverRole !== 'standalone') {
                    res.writeHead(503);
                    res.end(JSON.stringify({ error: 'API key status only available on Queen server' }));
                    return;
                }
                // Only show API key usage stats (not the actual keys)
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({
                    count: CONFIG.entrezApiKeys.length,
                    requestRate: getRequestRate(),
                    apiKeyThreshold: API_KEY_THRESHOLD,
                    apiKeysInUse: shouldUseApiKeys(),
                    usage: getKeyUsageStats()
                }));
                break;

            case '/status/security':
                if (CONFIG.serverRole !== 'queen' && CONFIG.serverRole !== 'standalone') {
                    res.writeHead(503);
                    res.end(JSON.stringify({ error: 'Security status only available on Queen server' }));
                    return;
                }
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(ipBanManager.getSummary()));
                break;

            case '/':
            case '/index.html':
            case '/developer.html':
            case '/faq.html':
            case '/implementation.html':
                handleStatic(req, res);
                break;

            default:
                // Check if it's a static file request
                if (pathname.startsWith('/static/') || pathname.startsWith('/examples/')) {
                    handleStatic(req, res);
                } else {
                    res.writeHead(404);
                    res.end('Not Found');
                }
        }
    } catch (e) {
        console.error(`[ERROR] ${e.message}`);
        sendError(res, e.message, 500);
    }
}

// ============================================================
// Distributed System: Queen/Bee Communication
// ============================================================

// Bee registration (for queen to track all bees)
class BeeRegistry {
    constructor() {
        this.bees = new Map(); // codename -> { host, port, lastSeen }
        this.load();
    }

    load() {
        try {
            if (fs.existsSync(CONFIG.beesFile)) {
                const data = JSON.parse(fs.readFileSync(CONFIG.beesFile, 'utf8'));
                this.bees = new Map(Object.entries(data));
                console.log(`[BEE] Loaded ${this.bees.size} bees`);
            }
        } catch (e) {
            console.log('[BEE] No bees file, starting fresh');
        }
    }

    save() {
        try {
            fs.writeFileSync(CONFIG.beesFile, JSON.stringify(Object.fromEntries(this.bees), null, 2));
        } catch (e) {
            console.error(`[BEE] Failed to save bees: ${e.message}`);
        }
    }

    register(codename, host, port) {
        this.bees.set(codename, { host, port, lastSeen: Date.now() });
        this.save();
        console.log(`[BEE] Registered bee: ${codename} at ${host}:${port}`);
    }

    unregister(codename) {
        this.bees.delete(codename);
        this.save();
    }

    getAll() {
        return Object.fromEntries(this.bees);
    }

    heartbeat(codename) {
        if (this.bees.has(codename)) {
            const bee = this.bees.get(codename);
            bee.lastSeen = Date.now();
            this.save();
        }
    }
}

const beeRegistry = new BeeRegistry();

// Current queen info (for bees)
let currentQueen = {
    host: CONFIG.queenHost,
    port: CONFIG.queenPort,
    codename: null
};

function encryptData(data) {
    if (!data) return null;
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', ENCRYPTION_KEY, iv);
    const encrypted = Buffer.concat([cipher.update(JSON.stringify(data), 'utf8'), cipher.final()]);
    const authTag = cipher.getAuthTag();
    return {
        iv: iv.toString('base64'),
        data: encrypted.toString('base64'),
        tag: authTag.toString('base64')
    };
}

function decryptData(encryptedObj) {
    if (!encryptedObj) return null;
    try {
        const iv = Buffer.from(encryptedObj.iv, 'base64');
        const encrypted = Buffer.from(encryptedObj.data, 'base64');
        const authTag = Buffer.from(encryptedObj.tag, 'base64');
        const decipher = crypto.createDecipheriv('aes-256-gcm', ENCRYPTION_KEY, iv);
        decipher.setAuthTag(authTag);
        const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);
        return JSON.parse(decrypted.toString('utf8'));
    } catch (e) {
        console.error('[ENCRYPTION] Decryption failed:', e.message);
        return null;
    }
}


// ============================================================
// SSRF Protection - Block private/internal IPs
// ============================================================
const PRIVATE_IP_RANGES = [
    // IPv4 private ranges
    /^10\./,                           // 10.0.0.0/8
    /^172\.(1[6-9]|2[0-9]|3[0-1])\./, // 172.16.0.0/12
    /^192\.168\./,                      // 192.168.0.0/16
    /^127\./,                           // 127.0.0.0/8 (loopback)
    /^169\.254\./,                      // 169.254.0.0/16 (link-local)
    /^0\./,                             // 0.0.0.0/8
    /^100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\./, // 100.64.0.0/10 (CGN)
    /^224\./,                           // 224.0.0.0/4 (multicast)
    /^240\./,                           // 240.0.0.0/4 (reserved)
    // IPv6 private ranges
    /^::1$/,                            // localhost
    /^fe80:/i,                          // fe80::/10 (link-local)
    /^fc/i,                             // fc00::/7 (unique local)
    /^fd/i,                             // fd00::/8 (unique local)
    /^2001:db8:/i,                      // 2001:db8::/32 (documentation)
    /^64:ff9b::/i,                      // 64:ff9b::/96 (NAT64)
    /^100::/i,                          // 100::/64 (ancer)
    /^::ffff:(127|169\.254|10|172\.(1[6-9]|2[0-9]|3[0-1])|192\.168)\./i, // IPv4-mapped IPv6
];

function isPrivateIP(hostname) {
    // Check if it's an IP address
    const ipv4Regex = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;
    const ipv6Regex = /^([0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}$/i;

    // Check IPv4
    const ipv4Match = hostname.match(ipv4Regex);
    if (ipv4Match) {
        const ip = hostname;
        for (const range of PRIVATE_IP_RANGES) {
            if (range.test(ip)) {
                return true;
            }
        }
        return false;
    }

    // Check IPv6
    const ipv6Match = hostname.match(ipv6Regex);
    if (ipv6Match || hostname.includes(':')) {
        // Normalize IPv6 (expand ::)
        let normalized = hostname.toLowerCase();
        if (normalized === '::1') return true;
        if (normalized.startsWith('::ffff:')) {
            // IPv4-mapped IPv6 - check the IPv4 part
            const ipv4Part = normalized.substring(7);
            return isPrivateIP(ipv4Part);
        }
        for (const range of PRIVATE_IP_RANGES) {
            if (range.test(normalized)) {
                return true;
            }
        }
        return false;
    }

    // Not an IP address - might be a hostname, resolve it
    return null; // Need DNS resolution to determine
}

// Validate host for SSRF protection
function validateHost(hostname) {
    const isPrivate = isPrivateIP(hostname);
    if (isPrivate === true) {
        return { valid: false, error: 'Private IP not allowed' };
    }
    return { valid: true };
}

// HTTP/HTTPS helper for inter-server communication (with encryption and SSRF protection)
function httpRequest(host, port, path, method = 'GET', data = null, encrypt = true, useHttps = false) {
    // SSRF Protection: validate target host
    const validation = validateHost(host);
    if (!validation.valid) {
        return Promise.reject(new Error(`[SSRF] Blocked: ${validation.error} - ${host}`));
    }

    return new Promise((resolve, reject) => {
        const options = {
            hostname: host,
            port: port,
            path: path,
            method: method,
            headers: {
                'Content-Type': 'application/json',
                'User-Agent': 'EntrezJS/3.0-Distributed'
            }
        };

        // Encrypt data if enabled and data exists (standalone doesn't encrypt)
        // Queen and Bee both use shared key for encryption
        let payload = data;
        if (encrypt && data && CONFIG.serverRole !== 'standalone') {
            options.headers['X-Encrypted'] = 'true';
            payload = encryptData(data);
        }

        // Use https or http based on useHttps parameter
        let client;
        if (useHttps) {
            // For HTTPS, allow self-signed certificates in development
            const httpsAgent = new https.Agent({
                rejectUnauthorized: process.env.NODE_ENV !== 'development'
            });
            options.agent = httpsAgent;
            client = https;
        } else {
            client = http;
        }

        const req = client.request(options, (res) => {
            let responseData = '';
            res.on('data', chunk => responseData += chunk);
            res.on('end', () => {
                // Check if response is encrypted
                if (res.headers['x-encrypted'] === 'true') {
                    try {
                        const decrypted = decryptData(JSON.parse(responseData));
                        resolve(decrypted);
                    } catch (e) {
                        reject(new Error('Failed to decrypt response'));
                    }
                } else {
                    try {
                        resolve(JSON.parse(responseData));
                    } catch (e) {
                        resolve(responseData);
                    }
                }
            });
        });

        req.on('error', (e) => {
            console.error(`[HTTP] Request to ${host}:${port}${path} failed: ${e.message}`);
            reject(e);
        });

        // Send data after connection
        if (payload) {
            // If payload is already a string, don't stringify again
            const dataToSend = typeof payload === 'string' ? payload : JSON.stringify(payload);
            req.write(dataToSend);
        }
        req.end();
    });
}

// Sync data to/from queen
async function syncToQueen() {
    if (CONFIG.serverRole !== 'bee' || !currentQueen.host) {
        return;
    }

    try {
        console.log(`[SYNC] Syncing to queen at ${currentQueen.host}:${currentQueen.port}`);

        // 1. Report new API keys to queen
        const myApiKeys = [];
        for (const [key, value] of apiKeyStore.keys) {
            if (value.verified) {
                myApiKeys.push({
                    key: key,
                    toolId: value.toolId,
                    email: value.email
                });
            }
        }

        // 2. Report blocked IPs to queen
        const myBlockedIps = Array.from(ipBanManager.bannedIps);

        // 3. Compress and send cache data (only small metadata, not full data)
        const cacheMeta = [];
        for (const [key, entry] of entrezCache.cache) {
            cacheMeta.push({
                key: key,
                expires: entry.expires,
                size: JSON.stringify(entry.data).length
            });
        }

        // Send sync data
        const result = await httpRequest(
            currentQueen.host,
            currentQueen.port,
            `/internal/sync?bee=${CONFIG.serverCodename}`,
            'POST',
            {
                apiKeys: myApiKeys,
                blockedIps: myBlockedIps,
                cacheMeta: cacheMeta,
                timestamp: Date.now()
            },
            true,
            CONFIG.queenHttps
        );

        console.log(`[SYNC] Sync result:`, result);

        // If queen responded with new data, update local
        if (result.apiKeys) {
            // Merge API keys from queen
            for (const ak of result.apiKeys) {
                if (!apiKeyStore.keys.has(ak.key)) {
                    apiKeyStore.keys.set(ak.key, {
                        contactName: 'synced',
                        email: ak.email,
                        toolId: ak.toolId,
                        verified: true,
                        createdOn: new Date().toISOString()
                    });
                }
            }
            apiKeyStore.save();
        }

        if (result.blockedIps) {
            // Merge blocked IPs from queen
            for (const ip of result.blockedIps) {
                if (!ipBanManager.bannedIps.has(ip)) {
                    ipBanManager.banIp(ip);
                }
            }
        }

    } catch (e) {
        console.error(`[SYNC] Failed to sync: ${e.message}`);
    }
}

// Sync data from queen (for bees to pull)
async function syncFromQueen() {
    // This is called by queen to pull data from bees
}

// Queen/standalone collects data from all bees
async function collectFromBees() {
    if (CONFIG.serverRole !== 'queen' && CONFIG.serverRole !== 'standalone') return;

    console.log('[QUEEN] Collecting data from bees...');

    for (const [codename, bee] of beeRegistry.bees) {
        try {
            const response = await httpRequest(
                bee.host,
                bee.port,
                `/internal/status?queen=${CONFIG.serverCodename}`
            );

            // Merge API keys
            if (response.apiKeys) {
                for (const ak of response.apiKeys) {
                    if (!apiKeyStore.keys.has(ak.key)) {
                        apiKeyStore.keys.set(ak.key, {
                            contactName: 'synced',
                            email: ak.email,
                            toolId: ak.toolId,
                            verified: true,
                            createdOn: new Date().toISOString()
                        });
                    }
                }
            }

            // Merge blocked IPs
            if (response.blockedIps) {
                for (const ip of response.blockedIps) {
                    ipBanManager.banIp(ip);
                }
            }

            console.log(`[QUEEN] Collected from ${codename}`);
        } catch (e) {
            console.error(`[QUEEN] Failed to collect from ${codename}: ${e.message}`);
        }
    }

    apiKeyStore.save();
    ipBanManager.saveBannedIps();
}

// Queen election: second queen joins
async function handleQueenElection() {
    if (CONFIG.serverRole !== 'queen' || !CONFIG.firstQueenHost) {
        return;
    }

    console.log(`[QUEEN] Joining as new queen, connecting to first queen at ${CONFIG.firstQueenHost}:${CONFIG.firstQueenPort}`);

    try {
        // Get API keys from first queen (no encryption for initial handshake)
        const firstQueenData = await httpRequest(
            CONFIG.firstQueenHost,
            CONFIG.firstQueenPort,
            `/internal/queen-handover?newQueen=${CONFIG.serverCodename}`,
            'GET',
            null,
            false,  // No encryption for initial handover
            CONFIG.firstQueenHttps
        );

        if (firstQueenData.apiKeys) {
            for (const [key, value] of Object.entries(firstQueenData.apiKeys)) {
                apiKeyStore.keys.set(key, value);
            }
            apiKeyStore.save();
            console.log(`[QUEEN] Imported ${Object.keys(firstQueenData.apiKeys).length} API keys`);
        }

        // Get trending data
        if (firstQueenData.trending) {
            Object.assign(trendingData, firstQueenData.trending);
            saveTrendingData();
        }

        // Get blocked IPs
        if (firstQueenData.blockedIps) {
            for (const ip of firstQueenData.blockedIps) {
                ipBanManager.banIp(ip);
            }
        }

        // Tell first queen to become bee and broadcast to all bees
        console.log('[QUEEN] First queen should now become bee');

    } catch (e) {
        console.error(`[QUEEN] Failed to join: ${e.message}`);
    }
}

// Broadcast new queen address to all bees
async function broadcastNewQueen(newQueenHost, newQueenPort) {
    console.log(`[QUEEN] Broadcasting new queen address to all bees`);

    for (const [codename, bee] of beeRegistry.bees) {
        try {
            await httpRequest(
                bee.host,
                bee.port,
                '/internal/queen-update',
                'POST',
                { host: newQueenHost, port: newQueenPort }
            );
            console.log(`[QUEEN] Notified ${codename} of new queen`);
        } catch (e) {
            console.error(`[QUEEN] Failed to notify ${codename}: ${e.message}`);
        }
    }
}

// Setup sync intervals based on role
function setupDistributedSync() {
    if (CONFIG.serverRole === 'bee' && CONFIG.queenHost) {
        // Bee: sync to queen every hour
        console.log(`[BEE] Will sync to queen at ${CONFIG.queenHost}:${CONFIG.queenPort}`);
        setInterval(syncToQueen, CONFIG.syncInterval);
    } else if (CONFIG.serverRole === 'queen') {
        // Queen: collect from bees every hour
        console.log('[QUEEN] Will collect data from bees');
        setInterval(collectFromBees, CONFIG.syncInterval);

        // Check for second queen joining
        if (CONFIG.firstQueenHost) {
            handleQueenElection();
        }
    }
}

// Internal endpoints for queen-bee communication
function handleInternalRequest(req, res, pathname, query) {
    const urlObj = new URL(req.url, `http://${req.headers.host}`);
    const path = urlObj.pathname;

    // Bee registers with queen
    if (path === '/internal/register' && CONFIG.serverRole === 'queen') {
        const codename = query.bee;
        const host = query.host;
        const port = query.port || CONFIG.port;

        // SSRF Protection: validate the host before registering
        const validation = validateHost(host);
        if (!validation.valid) {
            console.error(`[SSRF] Blocked bee registration from private IP: ${host}`);
            res.writeHead(403);
            res.end(JSON.stringify({ error: 'Private IP not allowed' }));
            return;
        }

        beeRegistry.register(codename, host, port);
        console.log(`[BEE] Registered: ${codename} from ${host}:${port}`);
        res.writeHead(200);
        res.end(JSON.stringify({ success: true }));
        return;
    }

    // Bee syncs data to queen
    if (path.startsWith('/internal/sync') && CONFIG.serverRole === 'queen') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            try {
                let data;
                if (req.headers['x-encrypted'] === 'true') {
                    data = decryptData(JSON.parse(body));
                } else {
                    data = JSON.parse(body);
                }
                const beeCodename = query.bee;

                // Update bee last seen
                beeRegistry.heartbeat(beeCodename);

                // Merge data
                if (data.apiKeys) {
                    for (const ak of data.apiKeys) {
                        if (!apiKeyStore.keys.has(ak.key)) {
                            apiKeyStore.keys.set(ak.key, {
                                contactName: 'synced',
                                email: ak.email,
                                toolId: ak.toolId,
                                verified: true,
                                createdOn: new Date().toISOString()
                            });
                        }
                    }
                }

                if (data.blockedIps) {
                    for (const ip of data.blockedIps) {
                        ipBanManager.banIp(ip);
                    }
                }

                apiKeyStore.save();
                ipBanManager.saveBannedIps();

                // Get all API keys and blocked IPs to share
                const allApiKeys = [];
                for (const [key, value] of apiKeyStore.keys) {
                    allApiKeys.push({ key, email: value.email, toolId: value.toolId });
                }

                const responseData = {
                    apiKeys: allApiKeys,
                    blockedIps: Array.from(ipBanManager.bannedIps)
                };
                res.writeHead(200, { 'Content-Type': 'application/json', 'X-Encrypted': 'true' });
                res.end(JSON.stringify(encryptData(responseData)));
            } catch (e) {
                res.writeHead(500);
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // Bee syncs pending registrations to queen (queen sends verification email)
    if (path.startsWith('/internal/sync-pending') && (CONFIG.serverRole === 'queen' || CONFIG.serverRole === 'standalone')) {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            try {
                let data;
                if (req.headers['x-encrypted'] === 'true') {
                    data = decryptData(JSON.parse(body));
                } else {
                    data = JSON.parse(body);
                }
                const beeCodename = query.bee;

                console.log(`[SYNC] Received pending registrations from bee: ${beeCodename}`);

                // Process pending registrations
                if (data.pendingRegistrations) {
                    for (const pending of data.pendingRegistrations) {
                        // Store locally
                        if (!apiKeyStore.toolIdExists(pending.toolId)) {
                            apiKeyStore.addPending(pending.apiKey, {
                                contactName: pending.contactName,
                                websiteUrl: pending.websiteUrl,
                                email: pending.email,
                                toolId: pending.toolId,
                                token: pending.token,
                                registrationIp: pending.registrationIp
                            });

                            // Send verification email
                            await sendVerificationEmail(pending.email, pending.toolId, pending.apiKey, pending.token);
                            console.log(`[SYNC] Stored pending registration from bee and sent verification email: ${pending.toolId}`);
                        }
                    }
                }

                res.writeHead(200);
                res.end(JSON.stringify({ success: true }));
            } catch (e) {
                console.error(`[SYNC] Failed to process pending registrations: ${e.message}`);
                res.writeHead(500);
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // Queen handover to new queen
    if (path === '/internal/queen-handover' && CONFIG.serverRole === 'queen') {
        const newQueen = query.newQueen;
        console.log(`[QUEEN] Handover requested by ${newQueen}`);

        // Get all data to transfer
        const apiKeysData = {};
        for (const [key, value] of apiKeyStore.keys) {
            apiKeysData[key] = value;
        }

        // Broadcast new queen to all bees
        const newQueenHost = req.headers.host.split(':')[0];
        const newQueenPort = CONFIG.port;
        broadcastNewQueen(newQueenHost, newQueenPort);

        // Become bee
        CONFIG.serverRole = 'bee';
        currentQueen = { host: newQueenHost, port: newQueenPort, codename: newQueen };

        console.log(`[QUEEN] Demoted to bee, new queen: ${newQueen}`);

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            apiKeys: apiKeysData,
            trending: trendingData,
            blockedIps: Array.from(ipBanManager.bannedIps)
        }));
        return;
    }

    // Bee receives queen update
    if (path === '/internal/queen-update' && CONFIG.serverRole === 'bee') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            try {
                let data;
                if (req.headers['x-encrypted'] === 'true') {
                    data = decryptData(JSON.parse(body));
                } else {
                    data = JSON.parse(body);
                }
                currentQueen.host = data.host;
                currentQueen.port = data.port;
                console.log(`[BEE] Queen updated to ${data.host}:${data.port}`);
                res.writeHead(200, { 'X-Encrypted': 'true' });
                res.end(JSON.stringify(encryptData({ success: true })));
            } catch (e) {
                res.writeHead(500);
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // Status endpoint for queen to collect
    if (path === '/internal/status' && CONFIG.serverRole === 'bee') {
        const myApiKeys = [];
        for (const [key, value] of apiKeyStore.keys) {
            if (value.verified) {
                myApiKeys.push({ key, email: value.email, toolId: value.toolId });
            }
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            apiKeys: myApiKeys,
            blockedIps: Array.from(ipBanManager.bannedIps)
        }));
        return;
    }

    // 404 for internal routes
    res.writeHead(404);
    res.end('Not Found');
}

// Create and start server (HTTP or HTTPS)
let server;
let protocol = 'http';

// Check for certbot certificates
const certBaseDir = CONFIG.certPath;
const certDomainDir = path.join(certBaseDir, CONFIG.defaultDomain);
const keyFilePath = path.join(certDomainDir, CONFIG.keyFile);
const certFilePath = path.join(certDomainDir, CONFIG.certFile);

const httpsEnabled = CONFIG.defaultDomain && fs.existsSync(keyFilePath) && fs.existsSync(certFilePath);

if (httpsEnabled) {
    const httpsOptions = {
        key: fs.readFileSync(keyFilePath),
        cert: fs.readFileSync(certFilePath)
    };

    // Create HTTPS server
    server = https.createServer(httpsOptions, handleRequest);
    protocol = 'https';
    console.log(`[HTTPS] Using certificates from ${certDomainDir}`);

    // Note: HTTP->HTTPS redirect should be handled by reverse proxy (nginx/caddy)
    // or use iptables/port 80 redirect. Don't bind both HTTP and HTTPS on same port.
} else {
    server = http.createServer(handleRequest);
    if (CONFIG.defaultDomain) {
        console.log(`[HTTP] No certificates found at ${certDomainDir}, using HTTP`);
    }
}

server.listen(CONFIG.port, () => {
    const apiKeyInfo = CONFIG.entrezApiKeys.length > 0
        ? `API key rotation: ${CONFIG.entrezApiKeys.length} keys`
        : 'API key rotation: disabled';

    const codenameLine = CONFIG.serverCodename
        ? `║  [${CONFIG.serverCodename}]                                         ║`
        : '';

    const titleLine = CONFIG.serverCodename
        ? `║                   EntrezJS Server [${CONFIG.serverCodename}]`
        : `║                   EntrezJS Server                          ║`;

    // Role display
    let roleInfo = '';
    if (CONFIG.serverRole === 'queen') {
        roleInfo = `║  Role: QUEEN (backup, trending, IP blocking)             ║`;
    } else if (CONFIG.serverRole === 'standalone') {
        roleInfo = `║  Role: STANDALONE (backup, trending, IP blocking)        ║`;
    } else if (CONFIG.serverRole === 'bee') {
        roleInfo = `║  Role: BEE -> queen: ${CONFIG.queenHost}:${CONFIG.queenPort}           ║`;
    }

    // Server URL for display
    const serverUrl = CONFIG.defaultDomain
        ? `${protocol}://${CONFIG.defaultDomain}:${CONFIG.port}`
        : `${protocol}://localhost:${CONFIG.port}`;

    console.log(`
╔═══════════════════════════════════════════════════════════╗
${titleLine}${' '.repeat(60 - titleLine.length)}║
║                       (EntrezJS)                          ║
╠═══════════════════════════════════════════════════════════╗
║  Server running at ${serverUrl.padEnd(53)}║
${codenameLine}
${roleInfo}
║  Available endpoints:                                      ║
║    /espell          - Spelling suggestions                ║
║    /einfo           - Database information                ║
║    /esearch         - Search NCBI databases               ║
║    /esummary        - Get document summaries              ║
║    /efetch          - Fetch records                       ║
║    /elink           - Find linked records                 ║
║    /esearch+esummary - Combined search + summary         ║
║    /esearch+efetch  - Combined search + fetch             ║
║    /esearch+elink   - Combined search + link              ║
║    /elink+esummary  - Combined link + summary             ║
║    /elink+efetch    - Combined link + fetch               ║
║    /register        - Developer registration              ║
║    /verify          - Email verification                  ║
║    /status/memcache - Cache/memory status                 ║
║    /status/trending - Daily trending data                  ║
║    /status/keys     - API key usage stats                 ║
║    /status/security - IP ban status & violations          ║
║                                                           ║
╠═══════════════════════════════════════════════════════════╣
║  DISTRIBUTED:                                             ║
║    Internal: /internal/register, /internal/sync           ║
║    /internal/status, /internal/queen-update               ║
║                                                           ║
╠═══════════════════════════════════════════════════════════╣
║  SECURITY:                                                ║
║    - API key required for all main endpoints              ║
║    - Email verification required for new registrations    ║
║    - Rate limiting: 30 requests/minute per IP             ║
║    - IP banning: 5+ violations = ban single IP           ║
║    - Subnet banning: 10+ violations in /24 = ban subnet   ║
║    - Auto cleanup on high memory (>80%)                  ║
║    - Backups saved every 24h + on shutdown/crash          ║
║                                                           ║
${(CONFIG.serverRole === 'queen' || CONFIG.serverRole === 'standalone') ? `║  ${apiKeyInfo.padEnd(59)}║\n║    - Slack webhook: ${process.env.SLACK_WEBHOOK_URL ? 'enabled' : 'disabled'.padEnd(36)}║\n║    - SMTP: ${CONFIG.smtpHost ? 'configured' : 'not configured'.padEnd(45)}║` : ''}
║  Cache time: ${CONFIG.cacheTime} seconds (${CONFIG.cacheTime / 3600} hours)              ║
╚═══════════════════════════════════════════════════════════╝
    `);

    // Setup distributed sync
    setupDistributedSync();
});

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\nShutting down...');
    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});
