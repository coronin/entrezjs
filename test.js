'use strict';

const http = require('http');
const querystring = require('querystring');

// Configuration
const SERVER_HOST = 'localhost';
const SERVER_PORT = 8080;

let apiKey = null;

// Helper to make requests
function makeRequest(path, method = 'GET', postData = null) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: SERVER_HOST,
            port: SERVER_PORT,
            path: path,
            method: method,
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
        };

        const req = http.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    resolve(JSON.parse(data));
                } catch (e) {
                    resolve(data);
                }
            });
        });

        req.on('error', reject);

        if (postData) {
            req.write(postData);
        }

        req.end();
    });
}

async function runTests() {
    console.log('╔═══════════════════════════════════════════════════════════╗');
    console.log('║                    EntrezJS Test Suite                    ║');
    console.log('╚═══════════════════════════════════════════════════════════╝\n');

    // Test 1: Register a developer
    console.log('[Test 1] Registering developer...');
    try {
        const registerData = querystring.stringify({
            contact_name: 'Test User',
            website_url: 'http://example.com',
            email: 'test@example.com',
            tool_id: 'test_tool_' + Date.now()
        });

        const regResult = await makeRequest('/register', 'POST', registerData);
        apiKey = regResult.apiKey;
        console.log('  ✓ Registration successful, API Key:', apiKey.substring(0, 8) + '...\n');
    } catch (e) {
        console.log('  ✗ Registration failed:', e.message, '\n');
        return;
    }

    // Test 2: espell endpoint
    console.log('[Test 2] Testing /espell...');
    try {
        const result = await makeRequest(`/espell?apikey=${apiKey}&db=pubmed&term=brest+caner`);
        console.log('  ✓ espell result:', JSON.stringify(result).substring(0, 100) + '...\n');
    } catch (e) {
        console.log('  ✗ espell failed:', e.message, '\n');
    }

    // Test 3: einfo endpoint
    console.log('[Test 3] Testing /einfo...');
    try {
        const result = await makeRequest(`/einfo?apikey=${apiKey}&db=pubmed`);
        console.log('  ✓ einfo result:', JSON.stringify(result).substring(0, 100) + '...\n');
    } catch (e) {
        console.log('  ✗ einfo failed:', e.message, '\n');
    }

    // Test 4: esearch endpoint
    console.log('[Test 4] Testing /esearch...');
    try {
        const result = await makeRequest(`/esearch?apikey=${apiKey}&db=pubmed&term=cancer&retmax=5`);
        console.log('  ✓ esearch result: Count =', result.result?.Count, ', IdList:', result.result?.IdList, '\n');
    } catch (e) {
        console.log('  ✗ esearch failed:', e.message, '\n');
    }

    // Test 5: esearch+esummary endpoint
    console.log('[Test 5] Testing /esearch+esummary...');
    try {
        const result = await makeRequest(`/esearch+esummary?apikey=${apiKey}&db=pubmed&term=BRCA1&retmax=3`);
        console.log('  ✓ esearch+esummary result: First ID =', result.result?.[0]?.Id, '\n');
    } catch (e) {
        console.log('  ✗ esearch+esummary failed:', e.message, '\n');
    }

    // Test 6: Test cache (second request should be faster)
    console.log('[Test 6] Testing cache (second esearch should hit cache)...');
    try {
        const start = Date.now();
        await makeRequest(`/esearch?apikey=${apiKey}&db=pubmed&term=cancer&retmax=5`);
        const time1 = Date.now() - start;

        const start2 = Date.now();
        await makeRequest(`/esearch?apikey=${apiKey}&db=pubmed&term=cancer&retmax=5`);
        const time2 = Date.now() - start2;

        console.log(`  ✓ First request: ${time1}ms, Second request (cached): ${time2}ms\n`);
    } catch (e) {
        console.log('  ✗ Cache test failed:', e.message, '\n');
    }

    // Test 7: Test JSONP
    console.log('[Test 7] Testing JSONP callback...');
    try {
        const result = await makeRequest(`/espell?apikey=${apiKey}&db=pubmed&term=brest+caner&callback=myCallback`);
        console.log('  ✓ JSONP result starts with:', result.substring(0, 30), '\n');
    } catch (e) {
        console.log('  ✗ JSONP test failed:', e.message, '\n');
    }

    // Test 8: Test without API key (should fail)
    console.log('[Test 8] Testing without API key (should fail)...');
    try {
        const result = await makeRequest('/espell?db=pubmed&term=test');
        console.log('  ✗ Should have failed but got:', result, '\n');
    } catch (e) {
        console.log('  ✓ Correctly rejected request without API key\n');
    }

    // Test 9: Test memcache status
    console.log('[Test 9] Checking cache status...');
    try {
        const result = await makeRequest('/status/memcache');
        console.log('  ✓ Cache stats:', result, '\n');
    } catch (e) {
        console.log('  ✗ Memcache status failed:', e.message, '\n');
    }

    console.log('╔═══════════════════════════════════════════════════════════╗');
    console.log('║                    Tests Complete!                        ║');
    console.log('╚═══════════════════════════════════════════════════════════╝');
}

runTests().catch(console.error);
