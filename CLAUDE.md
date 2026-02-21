# EntrezAJAX 2 JS (EntrezJS)

A bioinformatics web service that provides JSON/AJAX endpoints for NCBI's Entrez Programming Utilities (E-utilities). The original Python version was hosted on Google App Engine. This Node.js version provides the same functionality with easier deployment.

Allows web browsers to directly access NCBI's Entrez databases (PubMed, GenBank, Protein, Nucleotide, etc.) via AJAX without being blocked by the Same-Origin Policy. Supports JSONP for cross-origin requests.

Published in: Loman, N. and M. Pallen (2010). "EntrezAJAX: direct web browser access to the Entrez Programming Utilities." *Source Code for Biology and Medicine* 5(1): 6.

## Project Structure

```
entrezjs/
├── main.py              # Original Python WSGI entry point (GAE)
├── settings.py          # Django settings (GAE-specific)
├── urls.py              # URL routing configuration
├── app.yaml             # Google App Engine configuration
├── server.js            # Node.js server implementation (EntrezJS)
├── package.json         # NPM package configuration
├── test.js              # Test suite
├── api_keys.json        # Registered API keys (runtime generated)
├── api_keys_pending.json # Pending email verifications
├── api_keys.backup.*.json # Backups with timestamps
├── trending.json        # Daily trending data
├── Bio/                 # Biopython library (Entrez module)
├── json_endpoint/       # Core API endpoint module (Python)
├── devdb/               # Developer registration module
├── hmmer/               # HMMER protein domain search module
├── infopages/           # Info pages and examples
├── templates/           # HTML templates
├── static/              # Static files
└── appengine_django/    # Django-Google App Engine adapter
```

## Node.js Implementation (EntrezJS)

The Node.js version (`server.js`) provides all the functionality of the original Python version.

### Features

| Feature | Description |
|---------|-------------|
| **Caching** | 24-hour in-memory cache with LRU eviction |
| **Memory Management** | Auto cleanup when memory > 80% |
| **Email Verification** | Registration requires email verification |
| **API Key Rotation** | Support multiple NCBI API keys |
| **JSONP** | Supports `callback` parameter |
| **Persistence** | API keys persist to JSON files |
| **Backup** | Auto backup every 24h + on shutdown/crash |
| **Rate Limiting** | 30 requests/minute per IP |
| **Security Headers** | XSS, frame protection enabled |
| **Daily Trending** | Top 10 search terms + PMIDs, Slack webhook |

### Running the Node.js Server

```bash
# Start server
node server.js

# Run tests
node test.js
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | 8080 |
| `SERVER_URL` | Server URL for email verification | http://localhost:8080 |
| `ENTREZ_API_KEYS` | Comma-separated API keys for rotation | (none) |
| `SLACK_WEBHOOK_URL` | Slack webhook for daily trending | (none) |
| `SMTP_HOST` | SMTP server for email | (none) |
| `SMTP_PORT` | SMTP port | 587 |
| `SMTP_USER` | SMTP username | (none) |
| `SMTP_PASS` | SMTP password | (none) |

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

## Registration Flow

1. **Register**: `POST /register` with contact_name, website_url, email, tool_id
2. **Verify**: Click link in verification email `/verify?key=xxx&token=xxx`
3. **Use API**: Use verified API key for all endpoints

## JSON Output Format

Optimized dictionary format (not arrays):

```javascript
// esummary: { "12345": { "Title": "...", "Authors": "..." } }
// elink: { "pubmed": ["123", "456"], "protein": ["789"] }
// einfo: { "DbList": [...], "DbInfo": { "pubmed": {...} } }
// espell: { "pubmed": "cancer" }
```

## Configuration

- **Cache Time**: 24 hours (default)
- **Max Cache Entries**: 10,000
- **Memory Threshold**: Auto cleanup at >80% heap usage
- **Rate Limit**: 30 requests/minute per IP
- **Backup**: Every 24h + on SIGINT/SIGTERM/uncaughtException

## API Usage

```bash
# 1. Register to get API key
curl -X POST http://localhost:8080/register \
  -d "contact_name=Test&website_url=http://example.com&email=test@example.com&tool_id=mytool"

# 2. Check email for verification link
# Click the /verify?key=xxx&token=xxx link

# 3. Use the API
curl "http://localhost:8080/esearch?apikey=YOUR_API_KEY&db=pubmed&term=cancer"

# 4. JSONP callback
curl "http://localhost:8080/esearch?apikey=YOUR_API_KEY&db=pubterm=cancer&callback=myFunc"

# 5. Check status
curl http://localhost:8080/status/memcache
curl http://localhost:8080/status/trending
curl http://localhost:8080/status/keys
```

## Security

- API key required for all main endpoints
- Email verification required for new registrations
- Rate limiting (30 req/min per IP)
- Security headers (X-Content-Type-Options, X-Frame-Options, X-XSS-Protection)
- WHATWG URL API (no deprecated url.parse)
