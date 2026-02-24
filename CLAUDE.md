# EntrezJS

## Version History

| Version | Status | Description |
|---------|--------|-------------|
| 2.0.0 | EntrezAJAX | Original Python version on Google App Engine |
| 3.0.1 | EntrezJS | Node.js rewrite in production |

## About

A bioinformatics web service that provides JSON/AJAX endpoints for NCBI's Entrez Programming Utilities (E-utilities). The original Python version was hosted on Google App Engine. This Node.js version provides the same functionality with easier deployment.

Allows web browsers to directly access NCBI's Entrez databases (PubMed, GenBank, Protein, Nucleotide, etc.) via AJAX without being blocked by the Same-Origin Policy. Supports JSONP for cross-origin requests.

Published in: Loman, N. and M. Pallen (2010). "EntrezJS: direct web browser access to the Entrez Programming Utilities." *Source Code for Biology and Medicine* 5(1): 6.

## Project Structure

```
entrezjs/
├ ─server.js               # Node.js server implementation (EntrezJS)
├ package.json            # NPM package configuration
├ ─test.js                 # Test suite
├ ─.env                    # Sensitive config (Queen: API keys, SMTP, webhook)
├ ─.env.example            # Example config template
├ ─api_keys.json           # Registered API keys (runtime generated)
├ ─api_keys_pending.json   # Pending email verifications
├ ─api_keys.backup.*.json  # Backups with timestamps
├ ─trending.json           # Daily trending data
├ ─bees.json               # Registered bee servers (queen only)
├ ─shared_cache.json       # Shared cache metadata
└ ─banned_ips.json         # Blocked IPs
```

## Node.js Implementation (EntrezJS)

The Node.js version (`server.js`) provides all the functionality of the original Python version.

### Features

| Feature | Description |
|---------|-------------|
| **Zero Dependencies** | No npm install required - uses only Node.js built-in modules |
| **Caching** | 24-hour in-memory cache with GZIP compression |
| **Memory Management** | Auto cleanup when memory > 80% |
| **Email Verification** | Registration requires email verification |
| **API Key Rotation** | Adaptive - only use keys when > 5 req/s |
| **JSONP** | Supports `callback` parameter |
| **Persistence** | API keys persist to JSON files |
| **Backup** | Auto backup every 24h + on shutdown/crash |
| **Rate Limiting** | 30 requests/minute per IP |
| **Security Headers** | XSS, frame protection enabled |
| **Daily Trending** | Top 10 search terms + PMIDs, Slack webhook |
| **Distributed Mode** | Queen/Bee architecture for multi-server |
| **Encryption** | AES-256-GCM for Queen-Bee communication |
| **SSRF Protection** | Blocks private/internal IPs on bee registration |

### Quick Start (No npm install required!)

```bash
# Just copy the files and run - no dependencies needed!
node server.js
```

### Running the Node.js Server

```bash
# Standalone/Bee mode (uses defaults)
node server.js

# Bee with custom port
PORT=8081 node server.js

# Bee connecting to queen
PORT=8083 SERVER_CODE_NAME=bee02 SERVER_ROLE=bee QUEEN_HOST=192.168.1.100 QUEEN_PORT=8080 node server.js

# Queen (requires unique codename)
SERVER_CODE_NAME=queen01 SERVER_ROLE=queen node server.js

# Run tests
node test.js
```

### .env File (Queen only)

Sensitive configuration for Queen servers:

```bash
# Copy example
cp .env.example .env

# Edit .env with your values
SERVER_CODE_NAME=queen01
SERVER_ROLE=queen
ENTREZ_API_KEYS=key1,key2,key3
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
SMTP_HOST=smtp.example.com
SMTP_USER=user@example.com
SMTP_PASS=your-password
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | 8080 |
| `SERVER_CODE_NAME` | Server codename (a-z0-9, no spaces) | bee01 (standalone/bee) |
| `SERVER_ROLE` | Role: `queen`, `bee`, or `standalone` | standalone |
| `QUEEN_HOST` | Queen server IP (for bee) | - |
| `QUEEN_PORT` | Queen server port (for bee) | 8080 |
| `FIRST_QUEEN_HOST` | First queen IP (for second queen) | - |
| `FIRST_QUEEN_PORT` | First queen port (for second queen) | 8080 |
| `SERVER_URL` | Server URL for email verification | https://localhost:8080 |
| `ENTREZ_API_KEYS` | Comma-separated API keys for rotation (Queen) | - |
| `SLACK_WEBHOOK_URL` | Slack webhook for daily trending (Queen) | - |
| `SMTP_HOST` | SMTP server for email (Queen) | - |
| `SMTP_PORT` | SMTP port | 587 |
| `SMTP_USER` | SMTP username (Queen) | - |
| `SMTP_PASS` | SMTP password (Queen) | - |

**Note**: Queen role requires unique `SERVER_CODE_NAME`. Bee/Standalone can use default `bee01`.

## Distributed System (Queen/Bee)

### Architecture

```
┌─────────────┐
│   QUEEN    │ - API key backup
│            │ - Blocked IP aggregation
│            │ - Daily trending generation
│            │ - Cache coordination
└─────┬───────┘
      │ sync hourly
      ▼
┌─────────────┐
│    BEE      │ - Serve API requests
│    BEE      │ - Report new API keys
│    BEE      │ - Report blocked IPs
└─────────────┘
```

### Queen Configuration

```bash
# Queen requires unique codename + sensitive config via .env
# Copy .env.example to .env and fill in values

# Or use environment variables:
export SERVER_ROLE=queen
export SERVER_CODE_NAME=queen01
export ENTREZ_API_KEYS="key1,key2,key3"
export SLACK_WEBHOOK_URL="https://hooks.slack.com/..."
```

### Bee Configuration

```bash
# Bee can use defaults (bee01)
# Just specify queen address:
export SERVER_ROLE=bee
export QUEEN_HOST=192.168.1.100
export QUEEN_PORT=8080

# Or run without any config:
node server.js  # Uses bee01, standalone mode
```

### Second Queen (Failover)

```bash
# Second queen joins by specifying first queen
export SERVER_ROLE=queen
export SERVER_CODE_NAME=queen02
export FIRST_QUEEN_HOST=192.168.1.100
export FIRST_QUEEN_PORT=8080
```

On second queen join:
1. First queen broadcasts new queen address to all bees
2. First queen becomes bee
3. Second queen imports API keys and takes over

### Internal Endpoints

| Endpoint | Description |
|----------|-------------|
| `/internal/register` | Bee registers with queen |
| `/internal/sync` | Bee syncs API keys, blocked IPs (encrypted) |
| `/internal/status` | Bee status for queen collection |
| `/internal/queen-update` | Broadcast new queen address (encrypted) |
| `/internal/queen-handover` | First queen hands over to second (unencrypted) |

**Note**: `/internal/sync` and `/internal/queen-update` use AES-256-GCM encryption with `SERVER_CODE_NAME` as the key. The initial `/internal/queen-handover` is unencrypted since the two queens have different codenames.

## Available Endpoints

| Route | Description |
|-------|-------------|
| `/espell` | Spelling suggestions |
| `/einfo` | Database information |
| `/esearch` | Search NCBI databases |
| `/esummary` | Get document summaries |
| `/efetch` | Fetch records |
| `/elink` | Find linked records |
| `/esearch+esummary` | Combined search + summary |
| `/esearch+efetch` | Combined search + fetch |
| `/esearch+elink` | Combined search + link |
| `/elink+esummary` | Combined link + summary |
| `/elink+efetch` | Combined link + fetch |
| `/register` | Developer registration (requires email) |
| `/verify` | Email verification link |
| `/status/memcache` | Cache and memory status |
| `/status/trending` | Daily trending data |
| `/status/keys` | API key usage statistics |
| `/status/security` | IP ban status & violations |

## Registration Flow

1. **Register**: `POST /register` with contact_name, website_url, email, tool_id
2. **Verify**: Click link in verification email `/verify?key=xxx&token=xxx`
3. **Use API**: Use verified API key for all endpoints

## JSON Output Format

Matches Python EntrezAJAX format:

```javascript
// esummary: [{ "Id": "12345", "Title": "...", "Authors": "..." }, ...]
// elink: [{ "pubmed": ["123", "456"] }, { "protein": ["789"] }]
// einfo: { "DbList": [...], "DbInfo": { "pubmed": {...} } }
// espell: { "pubmed": "cancer" }
```

## Configuration

- **Cache Time**: 24 hours (default)
- **Max Cache Entries**: 5,000
- **Max Cache Size**: 512MB
- **Compression**: GZIP for entries > 1KB
- **Eviction**: LRU with size-based + entry-count limits
- **Memory Threshold**: Auto cleanup at >80% heap usage
- **Rate Limit**: 30 requests/minute per IP
- **API Key Threshold**: Use keys when > 5 req/s
- **Sync Interval**: 1 hour (bee→queen)
- **Backup**: Every 24h + on SIGINT/SIGTERM/uncaughtException

## API Usage

```bash
# 1. Register to get API key
curl -X POST https://localhost:8080/register \
  -d "contact_name=Test&website_url=https://example.com&email=test@example.com&tool_id=mytool"

# 2. Check email for verification link
# Click the /verify?key=xxx&token=xxx link

# 3. Use the API
curl "https://localhost:8080/esearch?apikey=YOUR_API_KEY&db=pubmed&term=cancer"

# 4. JSONP callback
curl "https://localhost:8080/esearch?apikey=YOUR_API_KEY&db=pubterm=cancer&callback=myFunc"

# 5. Check status
curl https://localhost:8080/status/memcache
curl https://localhost:8080/status/trending
curl https://localhost:8080/status/keys
curl https://localhost:8080/status/security
```

## Security

- API key required for all main endpoints
- Email verification required for new registrations
- Rate limiting (30 req/min per IP)
- IP banning (5+ violations = ban single IP)
- Subnet banning (10+ violations in /24 or /48 = ban subnet)
- Security headers (X-Content-Type-Options, X-Frame-Options, X-XSS-Protection)
- WHATWG URL API (no deprecated url.parse)
- **Queen-Bee Encryption**: AES-256-GCM encryption for internal communication using `SERVER_CODE_NAME` as key
- **SSRF Protection**: Blocks private/internal IPs (IPv4/IPv6/NAT64) on bee registration
