/* Deriv Market Analyzer — Over 1 / Under 8
   Runs fully in the browser. Uses Deriv's PUBLIC WebSocket API for market data.
   No login, no password, no API token. No trades are ever placed. */

'use strict';

/* ---------- config ---------- */
const APP_ID = 1089;                 // public demo app id for market data
const WS_URL = 'wss://ws.derivws.com/websockets/v3?app_id=' + APP_ID;
const MARKETS = [
  { sym: 'R_10',  name: 'Volatility 10'  },
  { sym: 'R_25',  name: 'Volatility 25'  },
  { sym: 'R_50',  name: 'Volatility 50'  },
  { sym: 'R_75',  name: 'Volatility 75'  },
  { sym: 'R_100', name: 'Volatility 100' }
];

/* Over 1  => wins when last digit is 2..9 (loses on 0,1)
   Under 8 => wins when last digit is 0..7 (loses on 8,9) */
function overWins(d){ return d >= 2; }
function underWins(d){ return d <= 7; }

/* ---------- state ---------- */
let ws = null;
let reqId = 1;
const pending = {};          // req_id -> {resolve, reject}
const tickSubs = {};         // symbol -> subscription id
const decimalsMap = {};      // symbol -> number of decimal places
const live = {};             // symbol -> {ticks, over, under, lastPrice, lastDigit}
let liveOn = false;

/* ---------- dom ---------- */
const $ = (id) => document.getElementById(id);
const connDot = $('connDot'), connText = $('connText');

/* ---------- websocket plumbing ---------- */
function connect(){
  ws = new WebSocket(WS_URL);
  ws.onopen = () => {
    setConn('on', 'Connected to Deriv');
    loadDecimals();
  };
  ws.onclose = () => { setConn('off', 'Disconnected — retrying…'); setTimeout(connect, 2500); };
  ws.onerror = () => setConn('off', 'Connection error');
  ws.onmessage = (ev) => onMessage(JSON.parse(ev.data));
}

function setConn(cls, text){
  connDot.className = 'conn-dot ' + cls;
  connText.textContent = text;
}

function send(payload){
  return new Promise((resolve, reject) => {
    const id = reqId++;
    payload.req_id = id;
    pending[id] = { resolve, reject };
    ws.send(JSON.stringify(payload));
    // safety timeout
    setTimeout(() => {
      if (pending[id]) { delete pending[id]; reject(new Error('timeout')); }
    }, 20000);
  });
}

function onMessage(msg){
  // route live ticks to the live handler (they share req_id with the sub request)
  if (msg.msg_type === 'tick' && msg.tick){
    handleTick(msg.tick, msg.subscription && msg.subscription.id);
  }
  const id = msg.req_id;
  if (id && pending[id]){
    const { resolve, reject } = pending[id];
    delete pending[id];
    if (msg.error) reject(new Error(msg.error.message));
    else resolve(msg);
  }
}
/* ---------- decimals: get display precision per symbol ---------- */
async function loadDecimals(){
  try {
    const res = await send({ active_symbols: 'brief', product_type: 'basic' });
    (res.active_symbols || []).forEach(s => {
      // pip = 0.01 -> 2 decimals, 0.001 -> 3 decimals, etc.
      if (s.pip) decimalsMap[s.symbol] = Math.round(-Math.log10(s.pip));
    });
  } catch (e) {
    // sensible fallbacks for the volatility indices
    Object.assign(decimalsMap, { R_10:3, R_25:3, R_50:4, R_75:4, R_100:2 });
  }
}

function lastDigit(price, sym){
  const d = decimalsMap[sym] != null ? decimalsMap[sym] : 2;
  const fixed = Number(price).toFixed(d);
  return parseInt(fixed[fixed.length - 1], 10);
}

/* ---------- historical test ---------- */
async function getHistory(sym, count){
  const res = await send({
    ticks_history: sym,
    count: count,
    end: 'latest',
    style: 'ticks'
  });
  return (res.history && res.history.prices) ? res.history.prices : [];
}

/* Try to get a real payout from the proposal endpoint (no auth needed).
   Falls back to a theoretical estimate if the broker rejects it. */
async function getPayout(sym, type, barrier, stake){
  try {
    const res = await send({
      proposal: 1, amount: stake, basis: 'stake', contract_type: type,
      currency: 'USD', duration: 1, duration_unit: 't', symbol: sym, barrier: String(barrier)
    });
    if (res.proposal && res.proposal.payout) return res.proposal.payout;
  } catch (e) { /* ignore, use fallback */ }
  // theoretical fallback: fair odds + ~2% house edge
  const p = type === 'DIGITOVER' ? 0.8 : 0.8; // over1=8/10, under8=8/10
  return stake * (1 / p) * 0.98;
}

function plFromRate(wins, losses, payout, stake){
  return wins * (payout - stake) - losses * stake;
}

async function runHistorical(){
  if (!ws || ws.readyState !== 1){ $('status').textContent = 'Not connected yet — please wait a moment.'; return; }
  const count = Math.max(100, Math.min(5000, parseInt($('tickCount').value) || 3000));
  const stake = parseFloat($('stake').value) || 1;
  $('runBtn').disabled = true;
  $('status').textContent = 'Downloading tick history and testing ' + MARKETS.length + ' markets…';
  $('resultBody').innerHTML = '';
  $('summary').innerHTML = '';

  const rows = [];
  for (const m of MARKETS){
    try {
      const prices = await getHistory(m.sym, count);
      let oWin = 0, uWin = 0, total = prices.length;
      for (const p of prices){
        const d = lastDigit(p, m.sym);
        if (overWins(d)) oWin++;
        if (underWins(d)) uWin++;
      }
      const oLose = total - oWin, uLose = total - uWin;
      const oPayout = await getPayout(m.sym, 'DIGITOVER', 1, stake);
      const uPayout = await getPayout(m.sym, 'DIGITUNDER', 8, stake);
      const oPL = plFromRate(oWin, oLose, oPayout, stake);
      const uPL = plFromRate(uWin, uLose, uPayout, stake);
      rows.push({
        sym: m.sym, name: m.name, total,
        oRate: total ? oWin/total*100 : 0,
        uRate: total ? uWin/total*100 : 0,
        oPL, uPL
      });
    } catch (e){
      rows.push({ sym: m.sym, name: m.name, total: 0, oRate: 0, uRate: 0, oPL: 0, uPL: 0, err: e.message });
    }
  }
  renderResults(rows);
  $('status').textContent = 'Tested ' + count + ' recent ticks per market. Done.';
  $('runBtn').disabled = false;
}

function fmtPL(v){
  const cls = v >= 0 ? 'pos' : 'neg';
  const sign = v >= 0 ? '+' : '';
  return '<span class="' + cls + '">' + sign + v.toFixed(2) + '</span>';
}

function renderResults(rows){
  const body = $('resultBody');
  body.innerHTML = '';
  rows.forEach(r => {
    const bestContract = r.oPL >= r.uPL
      ? '<span class="badge over">Over 1</span>'
      : '<span class="badge under">Under 8</span>';
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td class="market">' + r.name + '</td>' +
      '<td>' + r.total + '</td>' +
      '<td>' + r.oRate.toFixed(2) + '%</td>' +
      '<td>' + fmtPL(r.oPL) + '</td>' +
      '<td>' + r.uRate.toFixed(2) + '%</td>' +
      '<td>' + fmtPL(r.uPL) + '</td>' +
      '<td>' + (r.total ? bestContract : '<span class="muted">n/a</span>') + '</td>';
    body.appendChild(tr);
  });

  // best / worst by the best P/L available in each market
  const scored = rows.filter(r => r.total).map(r => ({
    name: r.name,
    best: Math.max(r.oPL, r.uPL),
    contract: r.oPL >= r.uPL ? 'Over 1' : 'Under 8',
    rate: r.oPL >= r.uPL ? r.oRate : r.uRate
  }));
  if (!scored.length){ return; }
  scored.sort((a,b) => b.best - a.best);
  const best = scored[0], worst = scored[scored.length - 1];
  $('summary').innerHTML =
    '<div class="box"><h3>🏆 Best-performing market</h3>' +
      '<div class="big">' + best.name + '</div>' +
      '<div class="muted">' + best.contract + ' &middot; ' + best.rate.toFixed(2) + '% win &middot; ' + fmtPL(best.best) + ' est. P/L</div></div>' +
    '<div class="box"><h3>⚠️ Worst-performing market</h3>' +
      '<div class="big">' + worst.name + '</div>' +
      '<div class="muted">' + worst.contract + ' &middot; ' + worst.rate.toFixed(2) + '% win &middot; ' + fmtPL(worst.best) + ' est. P/L</div></div>';
}
/* ---------- live analysis ---------- */
async function startLive(){
  if (!ws || ws.readyState !== 1) return;
  liveOn = true;
  $('liveBtn').textContent = 'Stop Live Analysis';
  $('liveBtn').classList.add('live-on');
  $('liveStatus').textContent = 'Streaming live ticks… win rates update as each new tick arrives.';
  $('liveBody').innerHTML = '';
  MARKETS.forEach(m => {
    live[m.sym] = { ticks: 0, over: 0, under: 0, lastPrice: '-', lastDigit: '-' };
    const tr = document.createElement('tr');
    tr.id = 'live-' + m.sym;
    tr.innerHTML = '<td class="market">' + m.name + '</td><td>-</td><td>-</td><td>0</td><td>-</td><td>-</td>';
    $('liveBody').appendChild(tr);
  });
  for (const m of MARKETS){
    try {
      const res = await send({ ticks: m.sym, subscribe: 1 });
      if (res.subscription) tickSubs[m.sym] = res.subscription.id;
    } catch(e){ /* ignore */ }
  }
}

function stopLive(){
  liveOn = false;
  $('liveBtn').textContent = 'Start Live Analysis';
  $('liveBtn').classList.remove('live-on');
  $('liveStatus').textContent = 'Live analysis stopped.';
  Object.keys(tickSubs).forEach(sym => {
    const id = tickSubs[sym];
    if (id) { try { ws.send(JSON.stringify({ forget: id })); } catch(e){} }
    delete tickSubs[sym];
  });
}

function handleTick(tick, subId){
  if (!liveOn) return;
  const sym = tick.symbol;
  const st = live[sym];
  if (!st) return;
  const d = lastDigit(tick.quote, sym);
  st.ticks++;
  if (overWins(d)) st.over++;
  if (underWins(d)) st.under++;
  st.lastPrice = Number(tick.quote).toFixed(decimalsMap[sym] != null ? decimalsMap[sym] : 2);
  st.lastDigit = d;
  const tr = $('live-' + sym);
  if (!tr) return;
  const oRate = (st.over / st.ticks * 100).toFixed(1);
  const uRate = (st.under / st.ticks * 100).toFixed(1);
  const chipCls = overWins(d) ? 'digit-win' : 'digit-lose';
  tr.children[1].textContent = st.lastPrice;
  tr.children[2].innerHTML = '<span class="digit-chip ' + chipCls + '">' + d + '</span>';
  tr.children[3].textContent = st.ticks;
  tr.children[4].textContent = oRate + '%';
  tr.children[5].textContent = uRate + '%';
}

/* ---------- bot XML parsing ---------- */
function parseBotXml(text){
  const info = {};
  let doc;
  try { doc = new DOMParser().parseFromString(text, 'text/xml'); }
  catch(e){ return { error: 'Could not read the XML file.' }; }
  if (doc.querySelector('parsererror')) return { error: 'This does not look like a valid Deriv Bot XML file.' };

  // Deriv Bot (Blockly) stores settings as <field name="...">value</field>
  const fields = {};
  doc.querySelectorAll('field').forEach(f => {
    const n = f.getAttribute('name');
    if (n && f.textContent) fields[n] = f.textContent.trim();
  });

  const pick = (...keys) => { for (const k of keys){ if (fields[k]) return fields[k]; } return null; };

  info.market     = pick('SYMBOL_LIST', 'MARKET_LIST', 'SUBMARKET_LIST');
  info.tradeType  = pick('TRADETYPE_LIST', 'TRADETYPECAT_LIST');
  info.contract   = pick('TYPE_LIST', 'CONTRACT_TYPE');
  info.prediction = pick('PREDICTION', 'BARRIER', 'LASTDIGITPREDICTION');
  info.stake      = pick('AMOUNT');
  info.duration   = pick('DURATION');
  info.durationU  = pick('DURATIONTYPE_LIST');
  info.currency   = pick('CURRENCY_LIST');

  // count block types for a quick structure read
  const blocks = {};
  doc.querySelectorAll('block').forEach(b => {
    const t = b.getAttribute('type');
    if (t) blocks[t] = (blocks[t] || 0) + 1;
  });
  info.blockCount = Object.values(blocks).reduce((a,b)=>a+b,0);
  return info;
}

function renderBotInfo(info){
  const el = $('botInfo');
  if (info.error){ el.className = 'bot-info'; el.innerHTML = '<span class="neg">' + info.error + '</span>'; return; }
  const row = (k, v) => v ? '<tr><td>' + k + '</td><td>' + v + '</td></tr>' : '';
  el.className = 'bot-info';
  el.innerHTML =
    '<table>' +
      row('Market',        info.market) +
      row('Trade type',    info.tradeType) +
      row('Contract',      info.contract) +
      row('Prediction / barrier', info.prediction) +
      row('Stake',         info.stake) +
      row('Duration',      (info.duration ? info.duration + ' ' + (info.durationU || '') : null)) +
      row('Currency',      info.currency) +
      row('Total blocks',  info.blockCount) +
    '</table>' +
    '<p class="note">Your bot rules were read for reference. Below, every market is tested against both Over 1 and Under 8 so you can see which fits best.</p>';
}

/* ---------- wire up ---------- */
$('botFile').addEventListener('change', (e) => {
  const file = e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => renderBotInfo(parseBotXml(reader.result));
  reader.readAsText(file);
});
$('runBtn').addEventListener('click', runHistorical);
$('liveBtn').addEventListener('click', () => { liveOn ? stopLive() : startLive(); });

connect();
