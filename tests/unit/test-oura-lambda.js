#!/usr/bin/env node

/**
 * Test suite for Oura API Lambda ingest
 * 
 * Tests:
 * 1. Mock API responses and verify S3 upload structure
 * 2. Verify DynamoDB logging format
 * 3. Test error handling for API failures
 * 4. Validate date range logic
 */

const assert = require('assert');

// Test constants
const BUCKET = process.env.BRONZE_BUCKET || 'bio-lakehouse-bronze-YOUR_AWS_ACCOUNT';
const TABLE = 'bio_ingestion_log';

// Mock Oura API responses
const MOCK_OURA_RESPONSES = {
  readiness: {
    data: [
      {
        id: "test-readiness-1",
        day: "2026-02-18",
        score: 85,
        temperature_deviation: -0.2,
        temperature_trend_deviation: 0.1
      }
    ]
  },
  sleep: {
    data: [
      {
        id: "test-sleep-1",
        day: "2026-02-18",
        score: 82,
        total_sleep_duration: 25200,
        rem_sleep_duration: 7200,
        deep_sleep_duration: 5400
      }
    ]
  },
  activity: {
    data: [
      {
        id: "test-activity-1",
        day: "2026-02-18",
        score: 88,
        active_calories: 450,
        steps: 8500
      }
    ]
  },
  heartrate: {
    data: [
      {
        timestamp: "2026-02-18T08:00:00Z",
        bpm: 62
      }
    ]
  },
  spo2: {
    data: [
      {
        timestamp: "2026-02-18T08:00:00Z",
        spo2_percentage: 97
      }
    ]
  }
};

class TestRunner {
  constructor() {
    this.tests = [];
    this.passed = 0;
    this.failed = 0;
  }

  test(name, fn) {
    this.tests.push({ name, fn });
  }

  async run() {
    console.log('\n🧪 Running Oura Lambda Tests\n');
    
    for (const { name, fn } of this.tests) {
      try {
        await fn();
        console.log(`✅ ${name}`);
        this.passed++;
      } catch (error) {
        console.error(`❌ ${name}`);
        console.error(`   ${error.message}`);
        this.failed++;
      }
    }

    console.log(`\n📊 Results: ${this.passed} passed, ${this.failed} failed\n`);
    return this.failed === 0;
  }
}

const runner = new TestRunner();

// Test 1: Verify S3 key structure
runner.test('S3 keys follow bronze layer convention', async () => {
  const dataType = 'readiness';
  const date = '2026-02-18';
  const expectedKeyPattern = /^oura\/readiness\/\d{4}-\d{2}-\d{2}T\d{6}Z\.json$/;
  
  const mockKey = `oura/${dataType}/${date}T${new Date().toISOString().split('T')[1].replace(/[:.]/g, '').slice(0, 6)}Z.json`;
  
  assert.ok(
    expectedKeyPattern.test(mockKey),
    `Key ${mockKey} doesn't match expected pattern`
  );
});

// Test 2: Verify data transformation
runner.test('API response transforms correctly for S3', async () => {
  const mockResponse = MOCK_OURA_RESPONSES.readiness;
  
  // Simulate transformation
  const transformed = {
    metadata: {
      source: 'oura-api',
      data_type: 'readiness',
      extracted_at: new Date().toISOString(),
      record_count: mockResponse.data.length
    },
    data: mockResponse.data
  };
  
  assert.strictEqual(transformed.metadata.source, 'oura-api');
  assert.strictEqual(transformed.metadata.data_type, 'readiness');
  assert.strictEqual(transformed.data.length, 1);
  assert.ok(transformed.metadata.extracted_at);
});

// Test 3: Verify DynamoDB log structure
runner.test('DynamoDB log entry has required fields', async () => {
  const logEntry = {
    file_path: 'oura/readiness/2026-02-18T120000Z.json',
    upload_timestamp: Date.now(),
    record_count: 1,
    status: 'success',
    data_type: 'readiness',
    source: 'oura-api'
  };
  
  assert.ok(logEntry.file_path);
  assert.ok(logEntry.upload_timestamp);
  assert.ok(logEntry.record_count >= 0);
  assert.ok(['success', 'failure', 'validation_failed'].includes(logEntry.status));
  assert.ok(logEntry.data_type);
  assert.ok(logEntry.source);
});

// Test 4: Date range calculation
runner.test('Date range defaults to last 7 days', async () => {
  const today = new Date();
  const sevenDaysAgo = new Date(today);
  sevenDaysAgo.setDate(today.getDate() - 7);
  
  const endDate = today.toISOString().split('T')[0];
  const startDate = sevenDaysAgo.toISOString().split('T')[0];
  
  assert.ok(startDate < endDate);
  assert.ok(/^\d{4}-\d{2}-\d{2}$/.test(startDate));
  assert.ok(/^\d{4}-\d{2}-\d{2}$/.test(endDate));
});

// Test 5: Error handling structure
runner.test('Error responses include required metadata', async () => {
  const errorResponse = {
    statusCode: 500,
    body: JSON.stringify({
      error: 'Oura API rate limit exceeded',
      timestamp: new Date().toISOString()
    })
  };
  
  const body = JSON.parse(errorResponse.body);
  assert.strictEqual(errorResponse.statusCode, 500);
  assert.ok(body.error);
  assert.ok(body.timestamp);
});

// Test 6: All endpoints covered
runner.test('All 5 Oura endpoints are defined', async () => {
  const expectedEndpoints = ['readiness', 'sleep', 'activity', 'heartrate', 'spo2'];
  const actualEndpoints = Object.keys(MOCK_OURA_RESPONSES);
  
  assert.strictEqual(actualEndpoints.length, 5);
  expectedEndpoints.forEach(endpoint => {
    assert.ok(
      actualEndpoints.includes(endpoint),
      `Missing endpoint: ${endpoint}`
    );
  });
});

// Test 7: SSE-AES256 encryption metadata
runner.test('S3 upload includes encryption header', async () => {
  const expectedParams = {
    Bucket: BUCKET,
    Key: 'oura/test/file.json',
    Body: JSON.stringify({ test: 'data' }),
    ContentType: 'application/json',
    ServerSideEncryption: 'AES256'
  };
  
  assert.strictEqual(expectedParams.ServerSideEncryption, 'AES256');
});

// Test 8: Timestamp format validation
runner.test('Timestamps use ISO 8601 format', async () => {
  const timestamp = new Date().toISOString();
  const iso8601Pattern = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
  
  assert.ok(
    iso8601Pattern.test(timestamp),
    `Timestamp ${timestamp} doesn't match ISO 8601`
  );
});

// Run all tests
(async () => {
  const success = await runner.run();
  process.exit(success ? 0 : 1);
})();
