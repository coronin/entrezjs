'use strict';

const https = require('https');
const http = require('http');
const url = require('url');
const querystring = require('querystring');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// Configuration
const CONFIG = {
    port: process.env.PORT || 8080,
    serverCodename: process.env.SERVER_CODE_NAME || 'entrezjs1',
    email: 'n.j.loman@bham.ac.uk',
    cacheTime: 60 * 60 * 24, // 24 hours in seconds
    entrezBaseUrl: 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils',
    apiKeysFile: path.join(__dirname, 'api_keys.json'),
    apiKeysBackupFile: path.join(__dirname, 'api_keys.backup.json'),
    backupInterval: 24 * 60 * 60 * 1000, // 24 hours in milliseconds
    serverUrl: process.env.SERVER_URL || 'http://localhost:8080',
    smtpHost: process.env.SMTP_HOST || '',
    smtpPort: process.env.SMTP_PORT || 587,
    smtpUser: process.env.SMTP_USER || '',
    smtpPass: process.env.SMTP_PASS || '',
    // Multiple API keys for rotation (for higher rate limits)
    entrezApiKeys: (process.env.ENTREZ_API_KEYS || '').split(',').filter(k => k.trim()),
    trendingFile: path.join(__dirname, 'trending.json'),
    trendingUpdateInterval: 24 * 60 * 60 * 1000, // 24 hours

    // ============================================================
    // Distributed System Configuration (Queen/Bee)
    // ============================================================
    // Role: 'queen' or 'bee' (default: standalone mode, no sync)
    serverRole: process.env.SERVER_ROLE || 'standalone',

    // Queen server address (for bee to connect)
    queenHost: process.env.QUEEN_HOST || '',
    queenPort: process.env.QUEEN_PORT || '8080',

    // For second queen joining (optional)
    firstQueenHost: process.env.FIRST_QUEEN_HOST || '',
    firstQueenPort: process.env.FIRST_QUEEN_PORT || '8080',

    // Sync interval (1 hour)
    syncInterval: 60 * 60 * 1000,

    // Bee list file (queen only)
    beesFile: path.join(__dirname, 'bees.json'),

    // Shared cache file
    sharedCacheFile: path.join(__dirname, 'shared_cache.json')
};

// Validate server codename (lowercase letters and numbers only, no spaces)
const CODE_NAME_PATTERN = /^[a-z0-9]+$/;
if (CONFIG.serverCodename && !CODE_NAME_PATTERN.test(CONFIG.serverCodename)) {
    console.error(`
╔═══════════════════════════════════════════════════════════╗
║                     ERROR                                 ║
╠═══════════════════════════════════════════════════════════╣
║  Invalid SERVER_CODE_NAME: "${CONFIG.serverCodename}"     ║
║                                                           ║
║  Codename must contain only:                              ║
║    - Lowercase letters (a-z)                             ║
║    - Numbers (0-9)                                       ║
║    - No spaces                                            ║
║                                                           ║
║  Example: export SERVER_CODE_NAME=entrezprod01            ║
╚═══════════════════════════════════════════════════════════╝
    `);
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

// In-memory cache using node-cache
class EntrezCache {
    constructor() {
        this.cache = new Map();
        this.maxSize = 10000; // Max number of cached entries
        this.cleanupInterval = 60 * 60 * 1000; // Check every hour

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

        this.cache.set(key, {
            data: data,
            expires: Date.now() + (ttl * 1000),
            lastAccessed: Date.now()
        });
    }

    getSize() {
        return this.cache.size;
    }

    // Cleanup expired entries
    cleanup() {
        const now = Date.now();
        let removed = 0;

        for (const [key, entry] of this.cache) {
            if (entry.expires < now) {
                this.cache.delete(key);
                removed++;
            }
        }

        if (removed > 0) {
            console.log(`[CACHE] Cleaned up ${removed} expired entries`);
        }
    }

    // Check memory usage and cleanup if needed
    checkMemory() {
        // If cache is full, remove oldest entries (LRU)
        if (this.cache.size >= this.maxSize) {
            const entries = Array.from(this.cache.entries());
            // Sort by lastAccessed (oldest first)
            entries.sort((a, b) => a[1].lastAccessed - b[1].lastAccessed);

            // Remove oldest 20%
            const removeCount = Math.floor(this.maxSize * 0.2);
            for (let i = 0; i < removeCount; i++) {
                this.cache.delete(entries[i][0]);
            }

            console.log(`[CACHE] Evicted ${removeCount} oldest entries (cache full)`);
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
            maxSize: this.maxSize,
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

    // If SMTP is configured, send real email
    if (CONFIG.smtpHost && CONFIG.smtpUser && CONFIG.smtpPass) {
        // Use nodemailer or similar - for now, log to console
        console.log(`[EMAIL] Would send verification email to ${email}`);
        console.log(`[EMAIL] Verify URL: ${verifyUrl}`);
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

// Schedule periodic backup (every 24 hours)
setInterval(backupApiKeys, CONFIG.backupInterval);

// Backup on normal shutdown
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

// Post to Slack webhook
async function postToSlack() {
    const webhookUrl = process.env.SLACK_WEBHOOK_URL;
    if (!webhookUrl) {
        console.log('[SLACK] No webhook URL configured');
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

// Schedule trending data update
loadTrendingData();
setInterval(updateTrendingData, CONFIG.trendingUpdateInterval);

// Initial update after 5 seconds (to not block startup)
setTimeout(updateTrendingData, 5000);

// Daily security summary - run every 24 hours
setInterval(() => {
    const summary = ipBanManager.dailySummary();
    console.log('[SECURITY] Daily IP summary:', summary);
}, 24 * 60 * 60 * 1000);

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
    const result = { entrezajax: { error: false }, ...data };
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
    const result = { entrezajax: { error: true, message: message } };
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
        entrezajax: { error: false }
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

// Register new developer
function handleRegister(req, res) {
    const clientIp = req.socket.remoteAddress || req.connection.remoteAddress;

    // Check if IP is banned
    if (ipBanManager.isBanned(clientIp)) {
        return sendError(res, 'Your IP has been banned', 403);
    }

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

            // Send verification email
            await sendVerificationEmail(email, tool_id, apiKey, token);

            console.log(`[REGISTER] Pending registration: ${tool_id} -> ${apiKey} from IP: ${clientIp}`);

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                success: true,
                message: 'Registration successful. Please check your email to verify your account.',
                apiKey: apiKey,
                pending: true
            }));
        } catch (e) {
            sendError(res, e.message, 500);
        }
    });
}

// Verify email
function handleVerify(req, res, query) {
    const apiKey = query.key;
    const token = query.token;

    if (!apiKey || !token) {
        return sendError(res, 'Missing key or token', 400);
    }

    const result = apiKeyStore.verifyAndActivate(apiKey, token);

    if (result.success) {
        console.log(`[VERIFY] API key verified: ${apiKey}`);
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
                handleRegister(req, res);
                break;

            case '/verify':
                handleVerify(req, res, query);
                break;

            case '/status/memcache':
                const stats = entrezCache.getStats();
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(stats));
                break;

            case '/status/trending':
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(trendingData, null, 2));
                break;

            case '/status/keys':
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

// HTTP helper for inter-server communication
function httpRequest(host, port, path, method = 'GET', data = null) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: host,
            port: port,
            path: path,
            method: method,
            headers: {
                'Content-Type': 'application/json',
                'User-Agent': 'EntrezJS/2.0-Distributed'
            }
        };

        const req = http.request(options, (res) => {
            let responseData = '';
            res.on('data', chunk => responseData += chunk);
            res.on('end', () => {
                try {
                    resolve(JSON.parse(responseData));
                } catch (e) {
                    resolve(responseData);
                }
            });
        });

        req.on('error', reject);
        if (data) {
            req.write(JSON.stringify(data));
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
            }
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

// Queen collects data from all bees
async function collectFromBees() {
    if (CONFIG.serverRole !== 'queen') return;

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
        // Get API keys from first queen
        const firstQueenData = await httpRequest(
            CONFIG.firstQueenHost,
            CONFIG.firstQueenPort,
            `/internal/queen-handover?newQueen=${CONFIG.serverCodename}`
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
        beeRegistry.register(codename, host, port);
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
                const data = JSON.parse(body);
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

                // Return consolidated data
                res.writeHead(200, { 'Content-Type': 'application/json' });

                // Get all API keys and blocked IPs to share
                const allApiKeys = [];
                for (const [key, value] of apiKeyStore.keys) {
                    allApiKeys.push({ key, email: value.email, toolId: value.toolId });
                }

                res.end(JSON.stringify({
                    apiKeys: allApiKeys,
                    blockedIps: Array.from(ipBanManager.bannedIps)
                }));
            } catch (e) {
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
                const data = JSON.parse(body);
                currentQueen.host = data.host;
                currentQueen.port = data.port;
                console.log(`[BEE] Queen updated to ${data.host}:${data.port}`);
                res.writeHead(200);
                res.end(JSON.stringify({ success: true }));
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

// Create and start server
const server = http.createServer(handleRequest);

server.listen(CONFIG.port, () => {
    const apiKeyInfo = CONFIG.entrezApiKeys.length > 0
        ? `API key rotation: ${CONFIG.entrezApiKeys.length} keys`
        : 'API key rotation: disabled';

    const codenameLine = CONFIG.serverCodename
        ? `║  [${CONFIG.serverCodename}]                                         ║`
        : '';

    const titleLine = CONFIG.serverCodename
        ? `║                   EntrezAJAX 2 JS Server [${CONFIG.serverCodename}]`
        : `║                   EntrezAJAX 2 JS Server                    ║`;

    // Role display
    let roleInfo = '';
    if (CONFIG.serverRole === 'queen') {
        roleInfo = `║  Role: QUEEN (backup, trending, IP blocking)             ║`;
    } else if (CONFIG.serverRole === 'bee') {
        roleInfo = `║  Role: BEE -> queen: ${CONFIG.queenHost}:${CONFIG.queenPort}           ║`;
    }

    console.log(`
╔═══════════════════════════════════════════════════════════╗
${titleLine}${' '.repeat(60 - titleLine.length)}║
║                       (EntrezJS)                          ║
╠═══════════════════════════════════════════════════════════╗
║  Server running at http://localhost:${CONFIG.port}                  ║
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
║  ${apiKeyInfo.padEnd(59)}║
║    - Slack webhook: ${process.env.SLACK_WEBHOOK_URL ? 'enabled' : 'disabled'.padEnd(36)}║
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
