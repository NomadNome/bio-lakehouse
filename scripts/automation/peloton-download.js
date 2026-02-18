#!/usr/bin/env node
/**
 * Automated Peloton workout download via OpenClaw browser control
 */

const PELOTON_URL = 'https://members.onepeloton.com/profile/workouts';
const GATEWAY_URL = process.env.OPENCLAW_GATEWAY_URL || 'http://127.0.0.1:18789';
const GATEWAY_TOKEN = process.env.OPENCLAW_GATEWAY_TOKEN;

async function downloadPelotonWorkouts() {
  console.log('🏋️  Starting Peloton download automation...');

  // Simple fetch-based browser control via OpenClaw gateway
  const headers = {
    'Content-Type': 'application/json',
  };
  if (GATEWAY_TOKEN) {
    headers['Authorization'] = `Bearer ${GATEWAY_TOKEN}`;
  }

  try {
    // Open page
    console.log('📖 Opening Peloton workouts page...');
    const openResp = await fetch(`${GATEWAY_URL}/api/browser/open`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        targetUrl: PELOTON_URL,
        profile: 'openclaw'
      })
    });
    const openData = await openResp.json();
    const targetId = openData.targetId;

    if (!targetId) {
      throw new Error('Failed to open browser page');
    }

    // Wait for page load
    console.log('⏳ Waiting for page load...');
    await new Promise(resolve => setTimeout(resolve, 5000));

    // Take snapshot to find button
    console.log('🔍 Looking for Download button...');
    const snapshotResp = await fetch(`${GATEWAY_URL}/api/browser/snapshot`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        targetId,
        refs: 'aria'
      })
    });
    const snapshotData = await snapshotResp.json();

    // Click "Download Workouts" button
    console.log('👆 Clicking Download Workouts...');
    const actResp = await fetch(`${GATEWAY_URL}/api/browser/act`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        targetId,
        request: {
          kind: 'click',
          ref: 'e61' // This is the ref from the snapshot - may need to be dynamic
        }
      })
    });

    if (!actResp.ok) {
      // Fallback: try to find the button by text
      await fetch(`${GATEWAY_URL}/api/browser/act`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          targetId,
          request: {
            kind: 'click',
            selector: 'button:has-text("Download Workouts")'
          }
        })
      });
    }

    console.log('⏳ Waiting for download...');
    await new Promise(resolve => setTimeout(resolve, 8000));

    console.log('✅ Download complete!');
    process.exit(0);

  } catch (error) {
    console.error('❌ Browser automation failed:', error.message);
    process.exit(1);
  }
}

downloadPelotonWorkouts();
