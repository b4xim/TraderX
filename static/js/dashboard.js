/**
 * TraderX — Dashboard JavaScript (v2 — 3-tab)
 * ════════════════════════════════════════════════════════════
 * PAPER TRADING MODE — this frontend NEVER calls any order-placement
 * endpoint. It only reads state from the FastAPI backend via WebSocket
 * and calls read-only / paper-trading REST endpoints.
 * ════════════════════════════════════════════════════════════
 *
 * Responsibilities:
 *   1. Tab bar switching (Dashboard / Stock Picker / Backtester).
 *   2. Maintain WebSocket connection to /ws for real-time state pushes.
 *   3. Render 3 live position cards (Tab 1).
 *   4. Drive countdown timer + SVG ring (Tab 1).
 *   5. Handle stock input submission (Tab 1).
 *   6. Auth status polling on load.
 *   7. Stats panel with All / Actual toggle (Tab 1).
 *   8. Stock Picker — run Stage 1+2 and Stage 3 scans (Tab 2).
 *   9. Backtester — load session by date, render P&L chart (Tab 3).
 */

'use strict';

// ─── Constants ────────────────────────────────────────────────
const WS_URL          = `ws://${location.host}/ws`;
const WS_PING_MS      = 25_000;
const WS_RETRY_BASE   = 2_000;
const WS_RETRY_MAX    = 30_000;
const RING_CIRCUMFERENCE = 2 * Math.PI * 38;
const SESSION_TOTAL_SECS = 17 * 60;   // 9:15 → 9:32 = 17 minutes

// Palette constants (match CSS design tokens)
const COLORS = {
  emerald: '#10B981',
  rose:    '#F43F5E',
  cyan:    '#22D3EE',
  amber:   '#F59E0B',
  fg:      '#EEF0FF',
  fg2:     '#9096B8',
  fg4:     '#363C60',
  border:  'rgba(255,255,255,0.07)',
  surface: '#12152A',
};

// ─── State ────────────────────────────────────────────────────
let ws              = null;
let wsRetryDelay    = WS_RETRY_BASE;
let wsPingInterval  = null;
let statsMode       = 'all';
let lastState       = null;
let prevStatuses    = {};
let isAuthenticated = false;
let lastScanResult  = null;
let btChartCtx      = null;
let activeTab       = 'dashboard';

// ─── DOM refs ────────────────────────────────────────────────
const $  = id => document.getElementById(id);
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
const elStatTotal    = $('stat-total');
const elStatWinrate  = $('stat-winrate');
const elStatAvgwin   = $('stat-avgwin');
const elStatAvgloss  = $('stat-avgloss');
const elStatExpect   = $('stat-expectancy');


// ════════════════════════════════════════════════════════════
// TAB SWITCHING
// ════════════════════════════════════════════════════════════

function switchTab(tabId) {
  const tabs = ['dashboard', 'picker', 'backtester'];
  tabs.forEach(t => {
    const panel = $(`tab-${t}`);
    const btn   = $(`tab-btn-${t}`);
    const isActive = t === tabId;
    if (panel) panel.style.display = isActive ? '' : 'none';
    if (btn)   {
      btn.classList.toggle('active', isActive);
      btn.setAttribute('aria-selected', isActive);
    }
  });
  activeTab = tabId;

  // Lazy-load on first switch
  if (tabId === 'picker' && !lastScanResult) {
    loadPickerConfig();
    fetchScanState();
  }
  if (tabId === 'backtester') {
    fetchHistoryDates();
    // Set date input to today
    const di = $('bt-date-input');
    if (di && !di.value) {
      di.value = new Date().toISOString().slice(0, 10);
    }
    // Initialize canvas context
    const canvas = $('bt-pnl-chart');
    if (canvas && !btChartCtx) {
      btChartCtx = canvas.getContext('2d');
    }
  }
}


// ════════════════════════════════════════════════════════════
// WebSocket Management
// ════════════════════════════════════════════════════════════

function connectWS() {
  if (ws && ws.readyState === WebSocket.OPEN) return;

  ws = new WebSocket(WS_URL);

  ws.addEventListener('open', () => {
    console.log('[WS] Connected');
    wsRetryDelay = WS_RETRY_BASE;
    clearInterval(wsPingInterval);
    wsPingInterval = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) ws.send('ping');
    }, WS_PING_MS);
  });

  ws.addEventListener('message', (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'pong') return;
      if (msg.type === 'state_update') handleStateUpdate(msg);
      if (msg.type === 'scan_update')  handleScanUpdate(msg);
    } catch (e) {
      console.warn('[WS] Failed to parse message:', e);
    }
  });

  ws.addEventListener('close', () => {
    console.warn('[WS] Disconnected — retrying in', wsRetryDelay, 'ms');
    clearInterval(wsPingInterval);
    if (lastState && lastState.positions && lastState.positions.length > 0) {
      elFeedBanner.classList.add('visible');
    }
    setTimeout(() => {
      wsRetryDelay = Math.min(wsRetryDelay * 1.5, WS_RETRY_MAX);
      connectWS();
    }, wsRetryDelay);
  });

  ws.addEventListener('error', (e) => console.error('[WS] Error:', e));
}


// ════════════════════════════════════════════════════════════
// STATE RENDERING (Tab 1 — Live Trading)
// ════════════════════════════════════════════════════════════

function handleStateUpdate(state) {
  lastState = state;
  const feedOk = state.feed_connected;
  if (feedOk) elFeedBanner.classList.remove('visible');
  if (!feedOk && state.positions && state.positions.some(p => p.status === 'OPEN')) {
    elFeedBanner.classList.add('visible');
  }
  renderCountdown(state.countdown_seconds, state.hard_exit_time, state.current_time);
  renderPositions(state.positions || []);
  renderStats(statsMode === 'all' ? state.stats_all : state.stats_actual);
}

function renderCountdown(secondsLeft, hardExitTime, currentTime) {
  if (elCurrentTime) elCurrentTime.textContent = currentTime || '--:--:--';

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

  const isUrgent = secondsLeft <= 120;
  elCountdown.classList.toggle('urgent', isUrgent);
  elCountdown.classList.remove('expired');

  const fraction = Math.min(secondsLeft / SESSION_TOTAL_SECS, 1);
  elRingFill.style.strokeDashoffset = RING_CIRCUMFERENCE * (1 - fraction);
  elRingFill.classList.toggle('urgent', isUrgent);
  elRingFill.classList.remove('expired');

  if (elSessionLabel) {
    elSessionLabel.textContent = lastState?.positions?.length > 0 ? 'Session active' : 'Waiting for positions';
    elSessionLabel.className = 'session-status' + (lastState?.positions?.length > 0 ? ' active' : '');
  }
}

function renderPositions(positions) {
  positions.forEach(pos => {
    const prev = prevStatuses[pos.instrument_key];
    if (prev === 'OPEN' && pos.status !== 'OPEN') triggerExitFlash(pos.status, pos.instrument_key);
    prevStatuses[pos.instrument_key] = pos.status;
  });

  for (let i = 0; i < 3; i++) {
    const slot = elCardSlots[i];
    if (!slot) continue;
    if (i < positions.length) renderCard(slot, positions[i], i);
    else {
      slot.className = 'position-card empty';
      slot.innerHTML = `<span class="empty-label">📊 Awaiting position ${i + 1}</span>`;
    }
  }
}

function renderCard(el, pos, idx) {
  const statusClass = statusToClass(pos.status);
  const pnlClass    = pos.pnl_pct > 0 ? 'positive' : pos.pnl_pct < 0 ? 'negative' : 'neutral';
  const pnlSign     = pos.pnl_pct >= 0 ? '+' : '';
  const gaugePos    = computeGaugePosition(pos);
  const isActual    = pos.is_actual_pick;
  const isClosed    = pos.status !== 'OPEN';
  const badgeLabel  = statusBadgeLabel(pos.status);

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
        <input type="checkbox" id="toggle-actual-${pos.trade_id}"
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
    </div>`;
}

function renderStats(stats) {
  if (!stats) return;
  elStatTotal.textContent   = stats.total_trades || '0';
  elStatWinrate.textContent = stats.total_trades ? `${stats.win_rate}%` : '—';
  elStatAvgwin.textContent  = stats.wins ? `+${stats.avg_win_pct}%` : '—';
  elStatAvgloss.textContent = stats.losses ? `${stats.avg_loss_pct}%` : '—';
  const exp = stats.expectancy;
  elStatExpect.textContent = stats.total_trades ? `${exp >= 0 ? '+' : ''}${exp}%` : '—';
  elStatExpect.className   = `stat-value ${exp >= 0 ? 'positive' : 'negative'}`;
  elStatAvgwin.className   = 'stat-value positive';
  elStatAvgloss.className  = 'stat-value negative';
  elStatWinrate.className  = `stat-value ${stats.win_rate >= 50 ? 'positive' : ''}`;
}

function computeGaugePosition(pos) {
  const { entry_price, stoploss, target, current_ltp } = pos;
  const totalRange = target - stoploss;
  let markerPct = 50;
  if (totalRange > 0) {
    const raw = ((current_ltp - stoploss) / totalRange) * 100;
    markerPct = Math.max(2, Math.min(98, raw));
  }
  const isPos = current_ltp >= entry_price;
  const markerClass = isPos ? 'positive' : 'negative';
  let posFill = 0, negFill = 0;
  if (isPos) {
    posFill = Math.max(0, ((current_ltp - entry_price) / (target - entry_price)) * 50);
  } else {
    negFill = Math.max(0, ((entry_price - current_ltp) / (entry_price - stoploss)) * 50);
  }
  return { markerPct, markerClass, posFill, negFill };
}

function triggerExitFlash(status, instrKey) {
  let flashClass = '';
  if (status === 'TARGET')    flashClass = 'flash-target';
  else if (status === 'SL')   flashClass = 'flash-sl';
  else if (status === 'TIME_EXIT') flashClass = 'flash-time';
  if (!flashClass) return;

  const positions = lastState?.positions || [];
  const idx = positions.findIndex(p => p.instrument_key === instrKey);
  if (idx >= 0 && elCardSlots[idx]) {
    const card = elCardSlots[idx];
    card.classList.add(flashClass);
    setTimeout(() => card.classList.remove(flashClass), 800);
  }
  elFlashOverlay.className = `exit-flash-overlay ${flashClass}`;
  setTimeout(() => { elFlashOverlay.className = 'exit-flash-overlay'; }, 900);
}


// ════════════════════════════════════════════════════════════
// STOCK SUBMISSION (Tab 1)
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
    if (!resp.ok) { showSubmitError(data.error || `Error ${resp.status}`); return; }
    $('stock-1').value = '';
    $('stock-2').value = '';
    $('stock-3').value = '';
    const errors = (data.results || []).filter(r => r.error);
    if (errors.length > 0) showSubmitError(errors.map(e => `${e.stock}: ${e.error}`).join(' | '));
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

async function toggleActualPick(tradeId) {
  try {
    const resp = await fetch(`/api/mark-actual/${tradeId}`, { method: 'POST' });
    if (!resp.ok) console.error('Failed to toggle actual pick:', await resp.text());
  } catch (e) {
    console.error('Network error toggling actual pick:', e);
  }
}

function switchStats(mode) {
  statsMode = mode;
  $('btn-stats-all').classList.toggle('active', mode === 'all');
  $('btn-stats-actual').classList.toggle('active', mode === 'actual');
  $('btn-stats-all').setAttribute('aria-pressed', mode === 'all');
  $('btn-stats-actual').setAttribute('aria-pressed', mode === 'actual');
  if (lastState) renderStats(mode === 'all' ? lastState.stats_all : lastState.stats_actual);
}


// ════════════════════════════════════════════════════════════
// STOCK PICKER — Tab 2
// ════════════════════════════════════════════════════════════

async function loadPickerConfig() {
  try {
    const resp = await fetch('/api/config');
    if (!resp.ok) return;
    const data = await resp.json();
    const s = data.scanner || {};
    const setText = (id, val) => { const el = $(id); if (el) el.textContent = val; };
    setText('cfg-pool-size',  s.pool_size ?? '—');
    setText('cfg-oi-tier',    s.min_avg_oi_tier ?? '—');
    setText('cfg-gap-band',   s.gap_min_pct != null ? `${s.gap_min_pct}% – ${s.gap_max_pct}%` : '—');
    setText('cfg-flips',      s.max_preopen_flips ?? '—');
    setText('cfg-top-n',      s.top_n_losers_at_open ?? '—');
    if (s.demo_mode) {
      const badge = $('picker-demo-badge');
      if (badge) badge.style.display = '';
    }
  } catch (e) {
    console.warn('Config load failed:', e);
  }
}

async function fetchScanState() {
  try {
    const resp = await fetch('/api/scan/state');
    if (!resp.ok) return;
    const data = await resp.json();
    if (data.stage) {
      lastScanResult = data;
      renderScanResult(data);
    }
  } catch (e) {
    console.warn('Scan state fetch failed:', e);
  }
}

async function runScanStage12() {
  setScanBtnLoading('s12', true);
  showPickerError(null);

  try {
    const resp = await fetch('/api/scan/stage1-2', { method: 'POST' });
    const data = await resp.json();
    if (!resp.ok) { showPickerError(data.error || `Error ${resp.status}`); return; }
    lastScanResult = data;
    renderScanResult(data);
    // Enable Stage 3 button
    const s3btn = $('btn-scan-s3');
    if (s3btn) s3btn.disabled = (data.selected || []).length === 0;
  } catch (e) {
    showPickerError(`Network error: ${e.message}`);
  } finally {
    setScanBtnLoading('s12', false);
  }
}

async function runScanStage3() {
  setScanBtnLoading('s3', true);
  showPickerError(null);

  try {
    const resp = await fetch('/api/scan/stage3', { method: 'POST' });
    const data = await resp.json();
    if (!resp.ok) { showPickerError(data.error || `Error ${resp.status}`); return; }
    lastScanResult = data;
    renderScanResult(data);
  } catch (e) {
    showPickerError(`Network error: ${e.message}`);
  } finally {
    setScanBtnLoading('s3', false);
  }
}

function handleScanUpdate(msg) {
  lastScanResult = msg;
  // Update Tab 2 badge
  const count = (msg.selected || []).length;
  const badge = $('picker-badge');
  if (badge) {
    badge.textContent = count;
    badge.style.display = count > 0 ? '' : 'none';
  }
  // Re-render only if on picker tab
  if (activeTab === 'picker') renderScanResult(msg);
}

function setScanBtnLoading(which, loading) {
  const btn = which === 's12' ? $('btn-scan-s12') : $('btn-scan-s3');
  const txt = $(which === 's12' ? 'scan-s12-text' : 'scan-s3-text');
  const sp  = $(which === 's12' ? 'scan-s12-spinner' : 'scan-s3-spinner');
  if (btn) btn.disabled = loading;
  if (txt) txt.style.display = loading ? 'none' : '';
  if (sp)  sp.style.display  = loading ? 'inline-block' : 'none';
}

function showPickerError(msg) {
  const el = $('picker-error');
  if (!el) return;
  if (msg) { el.textContent = msg; el.style.display = ''; }
  else { el.style.display = 'none'; }
}

function renderScanResult(result) {
  if (!result) return;

  // Status line
  const stageLabel = $('scan-stage-label');
  const tsLabel    = $('scan-ts-label');
  const stageText  = result.stage === 'STAGE1_2'
    ? `Stage 1+2 complete — ${(result.selected || []).length} candidate(s) passed`
    : `Stage 3 complete — ${(result.selected || []).length} final pick(s)`;
  if (stageLabel) stageLabel.textContent = stageText;
  if (tsLabel)    tsLabel.textContent = result.run_id ? new Date(result.run_id).toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' }) : '';

  // Stage 3 button state
  const s3btn = $('btn-scan-s3');
  if (s3btn) s3btn.disabled = !result.selected || result.selected.length === 0 || result.stage === 'STAGE3';

  // Badge update
  const count = (result.selected || []).length;
  const badge = $('picker-badge');
  if (badge) { badge.textContent = count; badge.style.display = count > 0 ? '' : 'none'; }

  // Warnings
  renderPickerWarnings(result.warnings || []);

  // Selected
  renderPickerSelected(result.selected || [], result.stage);

  // Excluded
  renderPickerExcluded(result.excluded || []);
}

function renderPickerWarnings(warnings) {
  const el = $('picker-warnings');
  if (!el) return;
  if (warnings.length === 0) { el.style.display = 'none'; return; }
  el.innerHTML = warnings.map(w => `<div>${escHtml(w)}</div>`).join('');
  el.style.display = '';
}

function renderPickerSelected(selected, stage) {
  const section = $('picker-selected-section');
  const cards   = $('picker-selected-cards');
  const count   = $('picker-selected-count');
  if (!section || !cards) return;

  count.textContent = selected.length;

  if (selected.length === 0) {
    section.style.display = 'none';
    return;
  }
  section.style.display = '';

  const isStage3 = stage === 'STAGE3';

  cards.innerHTML = selected.map((s, idx) => {
    const distClass = s.distance_from_prev_low_pct > 0 ? 'positive' : s.distance_from_prev_low_pct < 0 ? 'negative' : 'neutral';
    const distLabel = s.distance_from_prev_low_pct != null
      ? `${s.distance_from_prev_low_pct > 0 ? '-' : '+'}${Math.abs(s.distance_from_prev_low_pct).toFixed(2)}% from prev low`
      : '—';

    const sectorGapTxt = s.sector_gap_pct != null
      ? `${s.sector_gap_pct > 0 ? '+' : ''}${s.sector_gap_pct.toFixed(2)}%`
      : '—';
    const sectorGapClass = s.sector_gap_pct != null && s.sector_gap_pct < 0 ? 'negative' : 'neutral';

    const atm = isStage3 && s.atm_pe_key
      ? `<div class="picker-card-sep"></div>
         <div class="picker-pe-row">
           <span class="picker-pe-key">ATM PE: ${escHtml(s.atm_pe_key)}</span>
           <span class="picker-pe-ltp">₹${s.atm_pe_ltp != null ? s.atm_pe_ltp.toFixed(2) : '—'}</span>
         </div>
         <button class="btn-push-dashboard" onclick="pushToDashboard('${escHtml(s.symbol)}','${escHtml(s.atm_pe_key || s.symbol)}')">
           ▶ Push to Live Trading Tab
         </button>`
      : (isStage3 ? '' :
         `<button class="btn-push-dashboard"
            onclick="pushToDashboard('${escHtml(s.symbol)}','')">
            ▶ Use this stock in Live Trading
          </button>`);

    const corpFlag = s.has_corp_action
      ? `<div class="corp-action-flag">⚠ Corp Action: ${escHtml(s.corp_action_note)}</div>`
      : '';

    const s3info = isStage3 ? `
      <div class="picker-data-item">
        <div class="picker-data-label">Live Change</div>
        <div class="picker-data-val negative">${s.live_change_pct != null ? s.live_change_pct.toFixed(2) + '%' : '—'}</div>
      </div>
      <div class="picker-data-item">
        <div class="picker-data-label">Candle</div>
        <div class="picker-data-val ${s.is_red_candle ? 'negative' : 'positive'}">${s.is_red_candle ? '🔴 RED' : '🟢 GREEN'}</div>
      </div>` : '';

    return `
      <div class="picker-card" id="picker-card-${s.symbol}">
        <div class="picker-card-header">
          <div>
            <div style="display:flex;align-items:baseline;gap:8px">
              <span class="picker-card-rank">#${idx + 1}</span>
              <span class="picker-card-symbol">${escHtml(s.symbol)}</span>
            </div>
            <div class="picker-card-sector">${escHtml(s.sector)} · OI: ${escHtml(s.avg_oi_tier)}</div>
          </div>
          <span class="picker-gap-badge">${s.gap_pct != null ? s.gap_pct.toFixed(2) + '%' : '—'}</span>
        </div>
        <div class="picker-data-grid">
          <div class="picker-data-item">
            <div class="picker-data-label">Prev Close</div>
            <div class="picker-data-val">₹${s.prev_close != null ? s.prev_close.toFixed(2) : '—'}</div>
          </div>
          <div class="picker-data-item">
            <div class="picker-data-label">Pre-open LTP</div>
            <div class="picker-data-val">₹${s.pre_open_ltp != null ? s.pre_open_ltp.toFixed(2) : '—'}</div>
          </div>
          <div class="picker-data-item">
            <div class="picker-data-label">Dist from Prev Low</div>
            <div class="picker-data-val ${distClass}">${distLabel}</div>
          </div>
          <div class="picker-data-item">
            <div class="picker-data-label">Pre-open Flips</div>
            <div class="picker-data-val ${s.preopen_flip_count > 0 ? 'negative' : 'neutral'}">${s.preopen_flip_count ?? '—'}</div>
          </div>
          <div class="picker-data-item">
            <div class="picker-data-label">Sector Gap</div>
            <div class="picker-data-val ${sectorGapClass}">${sectorGapTxt}</div>
          </div>
          <div class="picker-data-item">
            <div class="picker-data-label">Prev Low</div>
            <div class="picker-data-val">₹${s.prev_low ? s.prev_low.toFixed(2) : '—'}</div>
          </div>
          ${s3info}
        </div>
        ${corpFlag}
        ${atm}
      </div>`;
  }).join('');
}

function renderPickerExcluded(excluded) {
  const section = $('picker-excluded-section');
  const tbody   = $('exclusion-tbody');
  const count   = $('picker-excluded-count');
  if (!section || !tbody) return;

  count.textContent = excluded.length;

  if (excluded.length === 0) { section.style.display = 'none'; return; }
  section.style.display = '';

  tbody.innerHTML = excluded.map(e => `
    <tr>
      <td class="excl-symbol">${escHtml(e.symbol)}</td>
      <td class="excl-stage">${escHtml(e.stage)}</td>
      <td><span class="excl-rule">${escHtml(e.rule)}</span></td>
      <td class="excl-detail">${escHtml(e.detail)}</td>
    </tr>
  `).join('');
}

function toggleExcluded() {
  const wrap    = $('picker-excluded-table-wrap');
  const chevron = $('excluded-chevron');
  const toggle  = $('excluded-toggle');
  if (!wrap) return;
  const isOpen = wrap.style.display !== 'none';
  wrap.style.display = isOpen ? 'none' : '';
  chevron?.classList.toggle('rotated', !isOpen);
  toggle?.setAttribute('aria-expanded', !isOpen);
}

// Push a selected stock to Tab 1 input fields
function pushToDashboard(symbol, instrumentKey) {
  const val = instrumentKey && instrumentKey !== symbol ? instrumentKey : symbol;

  // Find first empty input field
  for (let i = 1; i <= 3; i++) {
    const input = $(`stock-${i}`);
    if (input && !input.value.trim()) {
      input.value = val;
      input.focus();
      // Animate the input briefly
      input.style.borderColor = 'var(--emerald)';
      input.style.boxShadow = '0 0 0 3px var(--emerald-dim)';
      setTimeout(() => {
        input.style.borderColor = '';
        input.style.boxShadow = '';
      }, 1500);
      break;
    }
  }

  // Switch to Dashboard tab
  switchTab('dashboard');
}


// ════════════════════════════════════════════════════════════
// BACKTESTER — Tab 3
// ════════════════════════════════════════════════════════════

let currentBtUniverse = 'HIGH';

function setBtUniverse(univ) {
  currentBtUniverse = univ;
  const highBtn = $('univ-btn-high');
  const highMedBtn = $('univ-btn-highmed');
  if (highBtn) highBtn.classList.toggle('active', univ === 'HIGH');
  if (highMedBtn) highMedBtn.classList.toggle('active', univ === 'HIGH+MED');
}

async function fetchHistoryDates() {
  try {
    const resp = await fetch('/api/history-dates');
    const hintsEl = $('bt-date-hints');
    if (!hintsEl) return;

    let dates = [];
    if (resp.ok) {
      const data = await resp.json();
      dates = data.dates || [];
    }

    // Always provide quick suggestion chips
    const today = new Date();
    const suggestions = [];
    // Last 3 weekdays
    let check = new Date(today);
    while (suggestions.length < 3) {
      check.setDate(check.getDate() - 1);
      const day = check.getDay();
      if (day !== 0 && day !== 6) { // Skip Sat/Sun
        const yyyy = check.getFullYear();
        const mm = String(check.getMonth() + 1).padStart(2, '0');
        const dd = String(check.getDate()).padStart(2, '0');
        suggestions.push(`${yyyy}-${mm}-${dd}`);
      }
    }

    const allChips = Array.from(new Set([...dates, ...suggestions])).slice(0, 4);

    hintsEl.innerHTML = allChips.map(d =>
      `<button type="button" class="bt-date-hint-chip" onclick="selectBtDate('${d}')" aria-label="Test date ${d}">${d}</button>`
    ).join('');

    // Default the date input if empty
    const di = $('bt-date-input');
    if (di && !di.value && allChips.length > 0) {
      di.value = allChips[0];
    }
  } catch (e) {
    console.warn('History dates fetch failed:', e);
  }
}

function selectBtDate(dateStr) {
  const di = $('bt-date-input');
  if (di) di.value = dateStr;
  loadBacktest();
}

async function loadBacktest() {
  const dateInput = $('bt-date-input');
  const dateStr   = dateInput?.value;
  if (!dateStr) {
    showBtError('Please select a date.');
    return;
  }

  setBtBtnLoading(true);
  showBtError(null);

  // Hide all output sections
  for (const id of ['bt-summary', 'bt-chart-section', 'bt-trades-section', 'bt-empty']) {
    const el = $(id); if (el) el.style.display = 'none';
  }
  const demoBadge = $('bt-demo-badge');
  if (demoBadge) demoBadge.style.display = 'none';

  try {
    const resp = await fetch('/api/backtest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        date: dateStr,
        top_n: 5,
        universe: currentBtUniverse,
      })
    });

    if (!resp.ok) {
      const errData = await resp.json().catch(() => ({}));
      showBtError(errData.error || `Error ${resp.status}`);
      return;
    }

    const data = await resp.json();
    const trades = data.trades || [];

    if (trades.length === 0) {
      const empty = $('bt-empty');
      if (empty) empty.style.display = '';
      return;
    }

    if (data.is_demo && demoBadge) {
      demoBadge.style.display = '';
    }

    const metaEl = $('bt-header-meta');
    if (metaEl) {
      metaEl.textContent = `${data.date} · Universe: ${data.universe} · Leverage: ${data.pe_leverage_factor}x`;
    }

    renderBtSummary(data.summary);
    renderBtChart(trades);
    renderBtTradeCards(trades, data.pe_leverage_factor);
  } catch (e) {
    showBtError(`Network error: ${e.message}`);
  } finally {
    setBtBtnLoading(false);
  }
}

function renderBtSummary(summary) {
  const section = $('bt-summary');
  if (!section || !summary) return;
  section.style.display = '';

  const set = (id, val, cls='') => {
    const el = $(id);
    if (!el) return;
    el.textContent = val;
    if (cls) el.className = `bt-stat-value ${cls}`;
  };

  set('bt-stat-total',      summary.total_trades || 0);
  set('bt-stat-wins',       summary.targets_hit || 0, 'positive');
  set('bt-stat-losses',     summary.sl_hit || 0, 'negative');
  set('bt-stat-time-exits', summary.time_exits || 0);
  set('bt-stat-winrate',    `${summary.win_rate || 0}%`, (summary.win_rate || 0) >= 50 ? 'positive' : 'negative');
  
  const totalPnl = summary.total_pe_pnl_pct != null ? summary.total_pe_pnl_pct : 0;
  set('bt-stat-total-pnl',  `${totalPnl >= 0 ? '+' : ''}${totalPnl.toFixed(2)}%`, totalPnl >= 0 ? 'positive' : 'negative');

  const avgPnl = summary.avg_pe_pnl_pct != null ? summary.avg_pe_pnl_pct : 0;
  set('bt-stat-avg-pnl',    `${avgPnl >= 0 ? '+' : ''}${avgPnl.toFixed(2)}%`, avgPnl >= 0 ? 'positive' : 'negative');
}

function renderBtChart(trades) {
  const section = $('bt-chart-section');
  const canvas  = $('bt-pnl-chart');
  if (!section || !canvas) return;
  section.style.display = '';

  if (!trades || trades.length === 0) {
    section.style.display = 'none';
    return;
  }

  // Resize canvas
  const wrap = canvas.parentElement;
  canvas.width  = wrap.offsetWidth || 700;
  canvas.height = 220;

  if (!btChartCtx) btChartCtx = canvas.getContext('2d');
  const ctx = btChartCtx;
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  const W = canvas.width;
  const H = canvas.height;
  const PAD = { top: 20, right: 25, bottom: 36, left: 55 };
  const iW = W - PAD.left - PAD.right;
  const iH = H - PAD.top  - PAD.bottom;

  // Colors for up to 5 top losers
  const stockPalette = [COLORS.emerald, COLORS.cyan, COLORS.amber, '#A78BFA', '#F472B6'];

  // Determine global min and max PE PnL across all trades
  let minPnl = -22.0;
  let maxPnl = 22.0;
  trades.forEach(t => {
    (t.candles || []).forEach(c => {
      if (c.pe_pnl_pct < minPnl) minPnl = c.pe_pnl_pct;
      if (c.pe_pnl_pct > maxPnl) maxPnl = c.pe_pnl_pct;
    });
  });
  minPnl = Math.floor(minPnl - 2);
  maxPnl = Math.ceil(maxPnl + 2);
  const pnlRange = maxPnl - minPnl || 1;

  // Grid lines
  ctx.strokeStyle = COLORS.border;
  ctx.lineWidth   = 1;
  const gridLines = 4;
  for (let i = 0; i <= gridLines; i++) {
    const y = PAD.top + (iH / gridLines) * i;
    ctx.beginPath();
    ctx.moveTo(PAD.left, y);
    ctx.lineTo(W - PAD.right, y);
    ctx.stroke();

    const val = maxPnl - (pnlRange / gridLines) * i;
    ctx.fillStyle = COLORS.fg2;
    ctx.font = `10px "DM Mono", monospace`;
    ctx.textAlign = 'right';
    ctx.fillText(`${val >= 0 ? '+' : ''}${val.toFixed(0)}%`, PAD.left - 6, y + 4);
  }

  // Zero reference line
  const zeroY = PAD.top + ((maxPnl - 0) / pnlRange) * iH;
  ctx.strokeStyle = 'rgba(255,255,255,0.25)';
  ctx.lineWidth   = 1.5;
  ctx.setLineDash([4, 4]);
  ctx.beginPath();
  ctx.moveTo(PAD.left, zeroY);
  ctx.lineTo(W - PAD.right, zeroY);
  ctx.stroke();
  ctx.setLineDash([]);

  // Time Axis (09:16 to 09:32)
  const allTimes = (trades[0]?.candles || []).map(c => c.time);
  const numSteps = Math.max(allTimes.length - 1, 1);

  // Legend
  const legendEl = $('bt-chart-legend');
  if (legendEl) legendEl.innerHTML = '';

  // Plot each stock trajectory
  trades.forEach((t, ti) => {
    const color = stockPalette[ti % stockPalette.length];
    const candles = t.candles || [];
    if (candles.length === 0) return;

    const xs = candles.map((_, i) => PAD.left + (i / numSteps) * iW);
    const ys = candles.map(c => PAD.top + ((maxPnl - c.pe_pnl_pct) / pnlRange) * iH);

    // Line
    ctx.beginPath();
    xs.forEach((x, i) => { if (i === 0) ctx.moveTo(x, ys[i]); else ctx.lineTo(x, ys[i]); });
    ctx.strokeStyle = color;
    ctx.lineWidth   = 2.2;
    ctx.lineJoin    = 'round';
    ctx.stroke();

    // Exit marker
    const exitIdx = candles.findIndex(c => c.time === t.exit_time);
    const mIdx = exitIdx >= 0 ? exitIdx : candles.length - 1;
    ctx.beginPath();
    ctx.arc(xs[mIdx], ys[mIdx], 4.5, 0, Math.PI * 2);
    ctx.fillStyle = t.is_win ? COLORS.emerald : COLORS.rose;
    ctx.fill();
    ctx.strokeStyle = '#06090e';
    ctx.lineWidth = 2;
    ctx.stroke();

    // Legend item
    if (legendEl) {
      legendEl.innerHTML += `
        <div class="bt-legend-item">
          <span class="bt-legend-dot" style="background:${color}"></span>
          #${t.rank} ${escHtml(t.symbol)} (${t.pe_pnl_pct >= 0 ? '+' : ''}${t.pe_pnl_pct.toFixed(1)}%)
        </div>`;
    }
  });

  // X-axis timestamps
  ctx.fillStyle  = COLORS.fg4;
  ctx.font       = `10px "DM Mono", monospace`;
  ctx.textAlign  = 'center';
  const labelInterval = Math.max(1, Math.floor(allTimes.length / 5));
  allTimes.forEach((timeStr, i) => {
    if (i % labelInterval === 0 || i === allTimes.length - 1) {
      const x = PAD.left + (i / numSteps) * iW;
      ctx.fillText(timeStr, x, H - PAD.bottom + 16);
    }
  });
}

function generateSparklineSvg(candles, entryPrice, exitTime, exitReason, isWin) {
  if (!candles || candles.length < 2) return '';

  const w = 320;
  const h = 58;
  const pad = { top: 8, right: 12, bottom: 12, left: 12 };
  const innerW = w - pad.left - pad.right;
  const innerH = h - pad.top - pad.bottom;

  const prices = candles.map(c => c.price);
  const minP = Math.min(...prices, entryPrice);
  const maxP = Math.max(...prices, entryPrice);
  const range = maxP - minP || 1;

  const getX = (idx) => pad.left + (idx / (candles.length - 1)) * innerW;
  const getY = (p) => pad.top + ((maxP - p) / range) * innerH;

  const strokeColor = isWin ? '#34d399' : '#f43f5e';
  const fillColor   = isWin ? 'rgba(52, 211, 153, 0.12)' : 'rgba(244, 63, 94, 0.12)';

  const points = candles.map((c, i) => `${getX(i).toFixed(1)},${getY(c.price).toFixed(1)}`).join(' ');
  const entryY = getY(entryPrice).toFixed(1);

  const firstX = getX(0).toFixed(1);
  const lastX = getX(candles.length - 1).toFixed(1);
  const areaPath = `M ${firstX} ${getY(candles[0].price).toFixed(1)} ` +
                   candles.map((c, i) => `L ${getX(i).toFixed(1)} ${getY(c.price).toFixed(1)}`).join(' ') +
                   ` L ${lastX} ${h - pad.bottom} L ${firstX} ${h - pad.bottom} Z`;

  const exitIdx = candles.findIndex(c => c.time === exitTime);
  const markIdx = exitIdx >= 0 ? exitIdx : candles.length - 1;
  const exitX = getX(markIdx).toFixed(1);
  const exitY = getY(candles[markIdx].price).toFixed(1);

  return `
    <div class="bt-sparkline-wrap">
      <div class="bt-sparkline-header">
        <span>09:16 Entry: ₹${entryPrice.toFixed(2)}</span>
        <span style="color:${strokeColor}">${exitTime} Exit (${exitReason}): ₹${candles[markIdx].price.toFixed(2)}</span>
      </div>
      <svg class="bt-sparkline-svg" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
        <line x1="${pad.left}" y1="${entryY}" x2="${w - pad.right}" y2="${entryY}"
              stroke="rgba(255,255,255,0.2)" stroke-dasharray="3,3" stroke-width="1" />
        <path d="${areaPath}" fill="${fillColor}" />
        <polyline fill="none" stroke="${strokeColor}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" points="${points}" />
        <circle cx="${firstX}" cy="${entryY}" r="3" fill="#22d3ee" stroke="#06090e" stroke-width="1.5" />
        <circle cx="${exitX}" cy="${exitY}" r="4" fill="${strokeColor}" stroke="#06090e" stroke-width="1.5" />
      </svg>
    </div>`;
}

function renderBtTradeCards(trades, leverageFactor = 2.0) {
  const section = $('bt-trades-section');
  const container = $('bt-trade-cards');
  const count = $('bt-trade-count');
  if (!section || !container) return;

  count.textContent = trades.length;
  section.style.display = '';

  container.innerHTML = trades.map(t => {
    const pnl       = t.pe_pnl_pct;
    const pnlClass  = pnl == null ? 'neutral' : pnl > 0 ? 'positive' : 'negative';
    const pnlText   = pnl == null ? '—' : `${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}%`;
    const outcome   = t.exit_reason || 'TIME_EXIT';
    const badgeMap  = { TARGET: '✓ TARGET HIT', STOPLOSS: '✕ SL HIT', TIME_EXIT: '⏰ 9:32 TIME EXIT' };
    const badge     = badgeMap[outcome] || outcome;
    const strikeText= t.option_strike_approx ? `${t.option_strike_approx} PE` : 'ATM PE';

    const sparklineHtml = generateSparklineSvg(t.candles, t.entry_price, t.exit_time, outcome, t.is_win);

    return `
      <div class="bt-trade-card outcome-${outcome}">
        <div class="bt-card-header">
          <div>
            <div class="bt-stock-title">
              <span class="bt-card-rank">#${t.rank}</span>
              <span class="bt-card-symbol">${escHtml(t.symbol)}</span>
            </div>
            <div class="bt-card-sub">${escHtml(t.sector)} · ${escHtml(strikeText)} · ${escHtml(t.avg_oi_tier)} OI</div>
          </div>
          <span class="status-badge ${statusToClass(outcome)}">${badge}</span>
        </div>

        <div class="bt-pnl-display">
          <div class="bt-pnl-value ${pnlClass}">${pnlText}</div>
          <div class="bt-pnl-label">Simulated PE Return (${t.stock_move_pct >= 0 ? '+' : ''}${t.stock_move_pct.toFixed(2)}% Stock Move)</div>
        </div>

        <div class="bt-trade-details">
          <div class="bt-detail-row">
            <span class="bt-detail-label">Prev Close</span>
            <span class="bt-detail-val">₹${t.prev_close.toFixed(2)}</span>
          </div>
          <div class="bt-detail-row">
            <span class="bt-detail-label">9:16 Entry</span>
            <span class="bt-detail-val">₹${t.price_at_916.toFixed(2)} (${t.stock_gap_pct >= 0 ? '+' : ''}${t.stock_gap_pct.toFixed(2)}% gap)</span>
          </div>
          <div class="bt-detail-row">
            <span class="bt-detail-label">Exit Details</span>
            <span class="bt-detail-val">₹${t.exit_price.toFixed(2)} · ${t.exit_time} (${outcome})</span>
          </div>
        </div>

        ${sparklineHtml}
      </div>`;
  }).join('');
}

function setBtBtnLoading(loading) {
  const btn = $('btn-bt-load');
  const txt = $('bt-load-text');
  const sp  = $('bt-load-spinner');
  if (btn) btn.disabled = loading;
  if (txt) txt.style.display = loading ? 'none' : '';
  if (sp)  sp.style.display  = loading ? 'inline-block' : 'none';
}

function showBtError(msg) {
  const el = $('bt-error');
  if (!el) return;
  if (msg) { el.textContent = msg; el.style.display = ''; }
  else { el.style.display = 'none'; }
}


// ════════════════════════════════════════════════════════════
// AUTH STATUS
// ════════════════════════════════════════════════════════════

async function checkAuthStatus() {
  try {
    const resp = await fetch('/api/auth-status');
    const data = await resp.json();
    isAuthenticated = data.authenticated;

    if (isAuthenticated) {
      elAuthDot.className     = 'dot connected';
      elAuthLabel.textContent = 'Connected to Upstox';
      elBtnLogin.textContent  = 'Re-login';
    } else {
      elAuthDot.className     = 'dot disconnected';
      elAuthLabel.textContent = 'Not authenticated';
      elBtnLogin.textContent  = 'Login to Upstox';
    }
  } catch (e) {
    console.warn('Auth status check failed:', e);
  }
}


// ════════════════════════════════════════════════════════════
// UTILITY HELPERS
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
    const d = new Date(isoStr);
    return d.toLocaleTimeString('en-IN', {
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false, timeZone: 'Asia/Kolkata',
    });
  } catch { return isoStr; }
}

function statusToClass(status) {
  const map = { 'OPEN': 'open', 'TARGET': 'target', 'SL': 'sl', 'TIME_EXIT': 'time-exit' };
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
// KEYBOARD SHORTCUTS
// ════════════════════════════════════════════════════════════

['stock-1', 'stock-2', 'stock-3'].forEach(id => {
  const el = $(id);
  if (el) el.addEventListener('keydown', (e) => { if (e.key === 'Enter') submitStocks(); });
});

const btDateInput = $('bt-date-input');
if (btDateInput) btDateInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') loadBacktest(); });


// ════════════════════════════════════════════════════════════
// INIT
// ════════════════════════════════════════════════════════════

(async function init() {
  console.log('TraderX Dashboard v2 — PAPER TRADING MODE, no live orders');

  await checkAuthStatus();
  connectWS();

  // Load config for picker tab (even before switching to it)
  loadPickerConfig();

  // Fetch initial dashboard state
  try {
    const resp = await fetch('/api/state');
    const state = await resp.json();
    state.type = 'state_update';
    handleStateUpdate(state);
  } catch (e) {
    console.warn('Initial state fetch failed (WS will provide it):', e);
  }
})();
