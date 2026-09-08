/**
 * TraderX — Dashboard JavaScript
 * ════════════════════════════════════════════════════════════
 * PAPER TRADING MODE — this frontend NEVER calls any order-placement
 * endpoint. It only reads state from the FastAPI backend via WebSocket
 * and calls read-only REST endpoints.
 * ════════════════════════════════════════════════════════════
 *
 * Responsibilities:
 *   1. Maintain a WebSocket connection to /ws for real-time state pushes.
 *   2. Render 3 position cards from server state (no local calculation).
 *   3. Drive the countdown timer + SVG ring.
 *   4. Handle stock input submission.
 *   5. Auth status polling on load.
 *   6. Stats panel with All / Actual toggle.
 *   7. Flash animations on exit events.
 */

'use strict';

// ─── Constants ───────────────────────────────────────────────
const WS_URL         = `ws://${location.host}/ws`;
const WS_PING_MS     = 25_000;   // send ping every 25s to keep alive
const WS_RETRY_BASE  = 2_000;    // initial reconnect delay ms
const WS_RETRY_MAX   = 30_000;   // max reconnect delay ms
const RING_CIRCUMFERENCE = 2 * Math.PI * 38;  // SVG ring (r=34)
const SESSION_TOTAL_SECS = 17 * 60;  // 9:15 → 9:32 = 17 minutes

// ─── State ───────────────────────────────────────────────────
let ws              = null;
let wsRetryDelay    = WS_RETRY_BASE;
let wsPingInterval  = null;
let statsMode       = 'all';     // 'all' | 'actual'
let lastState       = null;      // latest full state from server
let prevStatuses    = {};        // instrument_key → previous status (for flash detection)
let isAuthenticated = false;

// ─── DOM refs (cached once) ──────────────────────────────────
const $ = id => document.getElementById(id);
const elCountdown    = $('countdown-display');
const elCurrentTime  = $('current-time-display');
const elSessionLabel = $('session-status-label');
const elFeedBanner   = $('feed-banner');
const elRingFill     = $('ring-fill');
const elAuthDot      = $('auth-dot');
const elAuthLabel    = $('auth-label');
const elBtnLogin     = $('btn-login');
const elBtnSubmit    = $('btn-submit');
const elBtnText      = $('btn-submit-text');
const elBtnSpinner   = $('btn-submit-spinner');
const elSubmitError  = $('submit-error');
const elFlashOverlay = $('exit-flash-overlay');
const elCardSlots    = [$('card-slot-0'), $('card-slot-1'), $('card-slot-2')];

// Stats elements
const elStatTotal      = $('stat-total');
const elStatWinrate    = $('stat-winrate');
const elStatAvgwin     = $('stat-avgwin');
const elStatAvgloss    = $('stat-avgloss');
const elStatExpectancy = $('stat-expectancy');


// ════════════════════════════════════════════════════════════
// WebSocket Management
// ════════════════════════════════════════════════════════════

function connectWS() {
  if (ws && ws.readyState === WebSocket.OPEN) return;

  ws = new WebSocket(WS_URL);

  ws.addEventListener('open', () => {
    console.log('[WS] Connected');
    wsRetryDelay = WS_RETRY_BASE;

    // Start keepalive pings
    clearInterval(wsPingInterval);
    wsPingInterval = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send('ping');
      }
    }, WS_PING_MS);
  });

  ws.addEventListener('message', (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'pong') return;
      if (msg.type === 'state_update') {
        handleStateUpdate(msg);
      }
    } catch (e) {
      console.warn('[WS] Failed to parse message:', e);
    }
  });

  ws.addEventListener('close', () => {
    console.warn('[WS] Disconnected — retrying in', wsRetryDelay, 'ms');
    clearInterval(wsPingInterval);

    // Show disconnected banner (only if we had positions)
    if (lastState && lastState.positions && lastState.positions.length > 0) {
      elFeedBanner.classList.add('visible');
    }

    setTimeout(() => {
      wsRetryDelay = Math.min(wsRetryDelay * 1.5, WS_RETRY_MAX);
      connectWS();
    }, wsRetryDelay);
  });

  ws.addEventListener('error', (e) => {
    console.error('[WS] Error:', e);
  });
}


// ════════════════════════════════════════════════════════════
// State Rendering
// ════════════════════════════════════════════════════════════

function handleStateUpdate(state) {
  lastState = state;

  // Feed connectivity
  const feedOk = state.feed_connected;
  if (feedOk) {
    elFeedBanner.classList.remove('visible');
  }
  // Only show disconnect banner if we have active positions
  if (!feedOk && state.positions && state.positions.some(p => p.status === 'OPEN')) {
    elFeedBanner.classList.add('visible');
  }

  // Countdown
  renderCountdown(state.countdown_seconds, state.hard_exit_time, state.current_time);

  // Positions
  renderPositions(state.positions || []);

  // Stats
  renderStats(statsMode === 'all' ? state.stats_all : state.stats_actual);
}

function renderCountdown(secondsLeft, hardExitTime, currentTime) {
  // Current time display
  if (elCurrentTime) elCurrentTime.textContent = currentTime || '--:--:--';

  // Countdown digits
  if (secondsLeft <= 0) {
    elCountdown.textContent = '00:00';
    elCountdown.classList.remove('urgent');
    elCountdown.classList.add('expired');
    elRingFill.style.strokeDashoffset = RING_CIRCUMFERENCE;
    elRingFill.classList.add('expired');
    elRingFill.classList.remove('urgent');
    if (elSessionLabel) {
      elSessionLabel.textContent = 'Session ended';
      elSessionLabel.className = 'session-status expired';
    }
    return;
  }

  const mins = Math.floor(secondsLeft / 60);
  const secs = secondsLeft % 60;
  elCountdown.textContent = `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;

  const isUrgent = secondsLeft <= 120; // last 2 minutes
  elCountdown.classList.toggle('urgent', isUrgent);
  elCountdown.classList.remove('expired');

  // SVG ring — proportion of 17-minute session remaining
  const fraction = Math.min(secondsLeft / SESSION_TOTAL_SECS, 1);
  const offset   = RING_CIRCUMFERENCE * (1 - fraction);
  elRingFill.style.strokeDashoffset = offset;
  elRingFill.classList.toggle('urgent', isUrgent);
  elRingFill.classList.remove('expired');

  if (elSessionLabel) {
    elSessionLabel.textContent = lastState && lastState.positions && lastState.positions.length > 0
      ? 'Session active'
      : 'Waiting for positions';
    elSessionLabel.className = 'session-status' + (lastState?.positions?.length > 0 ? ' active' : '');
  }
}

function renderPositions(positions) {
  // Detect new exits for flash effects
  positions.forEach(pos => {
    const prevStatus = prevStatuses[pos.instrument_key];
    const currStatus = pos.status;
    if (prevStatus === 'OPEN' && currStatus !== 'OPEN') {
      triggerExitFlash(currStatus, pos.instrument_key);
    }
    prevStatuses[pos.instrument_key] = currStatus;
  });

  // Render each card slot
  for (let i = 0; i < 3; i++) {
    const slot = elCardSlots[i];
    if (!slot) continue;

    if (i < positions.length) {
      renderCard(slot, positions[i], i);
    } else {
      // Empty slot
      slot.className = 'position-card empty';
      slot.innerHTML = `<span class="empty-label">📊 Awaiting position ${i + 1}</span>`;
    }
  }
}

function renderCard(el, pos, idx) {
  const statusClass   = statusToClass(pos.status);
  const pnlClass      = pos.pnl_pct > 0 ? 'positive' : pos.pnl_pct < 0 ? 'negative' : 'neutral';
  const pnlSign       = pos.pnl_pct >= 0 ? '+' : '';
  const gaugePos      = computeGaugePosition(pos);
  const isActual      = pos.is_actual_pick;
  const isClosed      = pos.status !== 'OPEN';
  const badgeLabel    = statusBadgeLabel(pos.status);

  el.className = `position-card status-${pos.status}${isActual ? ' is-actual-pick' : ''}`;

  el.innerHTML = `
    <div class="card-slot-num">${idx + 1}</div>

    <div class="card-header">
      <div>
        <div class="card-symbol">${escHtml(pos.stock)}</div>
        <div class="card-symbol-sub">${escHtml(pos.instrument_key)}</div>
      </div>
      <span class="status-badge ${statusClass}">${badgeLabel}</span>
    </div>

    <div class="pnl-display">
      <div class="pnl-value ${pnlClass}">${pnlSign}${pos.pnl_pct.toFixed(2)}%</div>
      <div class="pnl-label">Live P&amp;L</div>
    </div>

    <div class="gauge-container">
      <div class="gauge-labels">
        <span class="gauge-sl">SL ₹${pos.stoploss.toFixed(2)}</span>
        <span class="gauge-target">TGT ₹${pos.target.toFixed(2)}</span>
      </div>
      <div class="gauge-track" id="gauge-track-${idx}">
        <div class="gauge-fill-negative" style="width:${gaugePos.negFill}%"></div>
        <div class="gauge-center-line"></div>
        <div class="gauge-fill-positive" style="width:${gaugePos.posFill}%"></div>
        <div class="gauge-marker ${gaugePos.markerClass}${pos.status === 'OPEN' ? ' live' : ''}"
             style="left:${gaugePos.markerPct}%"></div>
      </div>
    </div>

    <div class="price-row">
      <span class="price-label">Entry</span>
      <span class="price-value">₹${pos.entry_price.toFixed(2)}</span>
    </div>
    <div class="price-row">
      <span class="price-label">LTP</span>
      <span class="price-value ltp">₹${pos.current_ltp.toFixed(2)}</span>
    </div>

    ${isClosed ? renderExitInfo(pos) : ''}

    <div class="card-separator"></div>
    <div class="actual-pick-row">
      <span class="actual-pick-label">My actual pick</span>
      <label class="toggle" title="Mark as actual trade">
        <input type="checkbox"
               id="toggle-actual-${pos.trade_id}"
               ${isActual ? 'checked' : ''}
               onchange="toggleActualPick(${pos.trade_id}, this.checked)"
               aria-label="Mark as actual pick" />
        <span class="toggle-slider"></span>
      </label>
    </div>

    <div class="entry-time">Entered: ${formatTime(pos.entry_time)}</div>
  `;
}

function renderExitInfo(pos) {
  return `
    <div class="exit-info">
      <div class="exit-row">
        <span class="exit-label">Exit Price</span>
        <span class="exit-val">₹${pos.exit_price ? pos.exit_price.toFixed(2) : '—'}</span>
      </div>
      <div class="exit-row">
        <span class="exit-label">Exit Time</span>
        <span class="exit-val">${pos.exit_time ? formatTime(pos.exit_time) : '—'}</span>
      </div>
      <div class="exit-row">
        <span class="exit-label">Reason</span>
        <span class="exit-val">${escHtml(pos.exit_reason || '—')}</span>
      </div>
    </div>
  `;
}

function renderStats(stats) {
  if (!stats) return;

  elStatTotal.textContent    = stats.total_trades || '0';
  elStatWinrate.textContent  = stats.total_trades ? `${stats.win_rate}%` : '—';
  elStatAvgwin.textContent   = stats.wins ? `+${stats.avg_win_pct}%` : '—';
  elStatAvgloss.textContent  = stats.losses ? `${stats.avg_loss_pct}%` : '—';

  const exp = stats.expectancy;
  elStatExpectancy.textContent = stats.total_trades ? `${exp >= 0 ? '+' : ''}${exp}%` : '—';
  elStatExpectancy.className = `stat-value ${exp >= 0 ? 'positive' : 'negative'}`;
  elStatAvgwin.className  = 'stat-value positive';
  elStatAvgloss.className = 'stat-value negative';
  elStatWinrate.className = `stat-value ${stats.win_rate >= 50 ? 'positive' : ''}`;
}


// ════════════════════════════════════════════════════════════
// Gauge Position Calculation
// ════════════════════════════════════════════════════════════

function computeGaugePosition(pos) {
  // Range: SL (0%) ──── entry (50%) ──── target (100%)
  const { entry_price, stoploss, target, current_ltp } = pos;
  const totalRange = target - stoploss;

  let markerPct = 50; // default: at entry
  if (totalRange > 0) {
    const raw = ((current_ltp - stoploss) / totalRange) * 100;
    markerPct = Math.max(2, Math.min(98, raw));
  }

  const isPosive = current_ltp >= entry_price;
  const markerClass = isPosive ? 'positive' : 'negative';

  // Fill widths (from center outward)
  const centerPct = ((entry_price - stoploss) / totalRange) * 100;
  let posFill = 0, negFill = 0;
  if (isPosive) {
    posFill = Math.max(0, ((current_ltp - entry_price) / (target - entry_price)) * 50);
  } else {
    negFill = Math.max(0, ((entry_price - current_ltp) / (entry_price - stoploss)) * 50);
  }

  return { markerPct, markerClass, posFill, negFill };
}


// ════════════════════════════════════════════════════════════
// Flash / Alert Effects
// ════════════════════════════════════════════════════════════

function triggerExitFlash(status, instrKey) {
  let flashClass = '';
  if (status === 'TARGET')    flashClass = 'flash-target';
  else if (status === 'SL')   flashClass = 'flash-sl';
  else if (status === 'TIME_EXIT') flashClass = 'flash-time';

  if (!flashClass) return;

  // Card-level flash
  const positions = lastState?.positions || [];
  const idx = positions.findIndex(p => p.instrument_key === instrKey);
  if (idx >= 0 && elCardSlots[idx]) {
    const card = elCardSlots[idx];
    card.classList.add(flashClass);
    setTimeout(() => card.classList.remove(flashClass), 800);
  }

  // Full-screen overlay flash
  elFlashOverlay.className = `exit-flash-overlay ${flashClass}`;
  setTimeout(() => { elFlashOverlay.className = 'exit-flash-overlay'; }, 900);
}


// ════════════════════════════════════════════════════════════
// Stock Submission
// ════════════════════════════════════════════════════════════

async function submitStocks() {
  const stocks = [
    $('stock-1').value.trim(),
    $('stock-2').value.trim(),
    $('stock-3').value.trim(),
  ].filter(Boolean);

  if (stocks.length === 0) {
    showSubmitError('Please enter at least one stock name or instrument key.');
    return;
  }

  // Loading state
  elBtnText.style.display    = 'none';
  elBtnSpinner.style.display = 'inline-block';
  elBtnSubmit.disabled       = true;
  hideSubmitError();

  try {
    const resp = await fetch('/api/submit-stocks', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ stocks }),
    });

    const data = await resp.json();

    if (!resp.ok) {
      showSubmitError(data.error || `Error ${resp.status}`);
      return;
    }

    // Clear inputs on success
    $('stock-1').value = '';
    $('stock-2').value = '';
    $('stock-3').value = '';

    // Check for per-stock errors
    const errors = (data.results || []).filter(r => r.error);
    if (errors.length > 0) {
      showSubmitError(errors.map(e => `${e.stock}: ${e.error}`).join(' | '));
    }

  } catch (e) {
    showSubmitError(`Network error: ${e.message}`);
  } finally {
    elBtnText.style.display    = 'inline';
    elBtnSpinner.style.display = 'none';
    elBtnSubmit.disabled       = false;
  }
}

function showSubmitError(msg) {
  elSubmitError.textContent  = msg;
  elSubmitError.style.display = 'block';
}

function hideSubmitError() {
  elSubmitError.style.display = 'none';
}


// ════════════════════════════════════════════════════════════
// Actual Pick Toggle
// ════════════════════════════════════════════════════════════

async function toggleActualPick(tradeId, checked) {
  // Optimistically update the server
  try {
    const resp = await fetch(`/api/mark-actual/${tradeId}`, { method: 'POST' });
    if (!resp.ok) {
      console.error('Failed to toggle actual pick:', await resp.text());
    }
  } catch (e) {
    console.error('Network error toggling actual pick:', e);
  }
}


// ════════════════════════════════════════════════════════════
// Stats Filter
// ════════════════════════════════════════════════════════════

function switchStats(mode) {
  statsMode = mode;

  $('btn-stats-all').classList.toggle('active', mode === 'all');
  $('btn-stats-actual').classList.toggle('active', mode === 'actual');
  $('btn-stats-all').setAttribute('aria-pressed', mode === 'all');
  $('btn-stats-actual').setAttribute('aria-pressed', mode === 'actual');

  if (lastState) {
    renderStats(mode === 'all' ? lastState.stats_all : lastState.stats_actual);
  }
}


// ════════════════════════════════════════════════════════════
// Auth Status
// ════════════════════════════════════════════════════════════

async function checkAuthStatus() {
  try {
    const resp = await fetch('/api/auth-status');
    const data = await resp.json();
    isAuthenticated = data.authenticated;

    if (isAuthenticated) {
      elAuthDot.className   = 'dot connected';
      elAuthLabel.textContent = 'Connected to Upstox';
      elBtnLogin.textContent = 'Re-login';
    } else {
      elAuthDot.className   = 'dot disconnected';
      elAuthLabel.textContent = 'Not authenticated';
      elBtnLogin.textContent = 'Login to Upstox';
    }
  } catch (e) {
    console.warn('Auth status check failed:', e);
  }
}


// ════════════════════════════════════════════════════════════
// Utility Helpers
// ════════════════════════════════════════════════════════════

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatTime(isoStr) {
  if (!isoStr) return '—';
  try {
    // Handles ISO strings like "2026-09-08T09:17:05.123+05:30"
    const d = new Date(isoStr);
    return d.toLocaleTimeString('en-IN', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
      timeZone: 'Asia/Kolkata',
    });
  } catch {
    return isoStr;
  }
}

function statusToClass(status) {
  const map = {
    'OPEN':      'open',
    'TARGET':    'target',
    'SL':        'sl',
    'TIME_EXIT': 'time-exit',
  };
  return map[status] || 'open';
}

function statusBadgeLabel(status) {
  const map = {
    'OPEN':      '● OPEN',
    'TARGET':    '✓ TARGET HIT',
    'SL':        '✕ SL HIT',
    'TIME_EXIT': '⏰ TIME EXIT',
  };
  return map[status] || status;
}


// ════════════════════════════════════════════════════════════
// Enter key on inputs → submit
// ════════════════════════════════════════════════════════════

['stock-1', 'stock-2', 'stock-3'].forEach(id => {
  const el = $(id);
  if (el) {
    el.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') submitStocks();
    });
  }
});


// ════════════════════════════════════════════════════════════
// Init
// ════════════════════════════════════════════════════════════

(async function init() {
  console.log('TraderX Dashboard — PAPER TRADING MODE, no live orders');

  await checkAuthStatus();
  connectWS();

  // Fetch initial state via REST as fallback (in case WS is slow)
  try {
    const resp = await fetch('/api/state');
    const state = await resp.json();
    state.type = 'state_update';
    handleStateUpdate(state);
  } catch (e) {
    console.warn('Initial state fetch failed (WS will provide it):', e);
  }
})();
