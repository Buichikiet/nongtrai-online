'use strict';
/* Nông Trại Vui Vẻ – server: Google/Discord, lưu data, WS, role, admin, bot */
const fs = require('fs'), path = require('path'), http = require('http'), crypto = require('crypto');
const { CUTE_TITLES, CLOTHES, SHOES, BAGS, CROP_IDS, FURN_IDS, GOOD_IDS, ROLE_LV } = require('./catalog');

try {
  for (const l of fs.readFileSync(path.join(__dirname, '.env'), 'utf8').split(/\r?\n/)) {
    const m = l.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (m && !(m[1] in process.env)) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
} catch (e) { /* không có .env */ }
const E = process.env;
const PORT = +E.PORT || 3000;
const GCID = E.GOOGLE_CLIENT_ID || '';
const DEV = E.DEV_LOGIN === '1';
const OWNERS = (E.OWNER_EMAILS || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
const INVITE = E.DISCORD_INVITE || 'https://discord.gg/TZRfuhbqJA';
const DISCORD_CID = E.DISCORD_CLIENT_ID || '';
const DISCORD_SEC = E.DISCORD_CLIENT_SECRET || '';
const PUBLIC_URL = (E.PUBLIC_URL || '').replace(/\/$/, '');
const DATA = E.DATA_DIR || path.join(__dirname, 'data');
fs.mkdirSync(DATA, { recursive: true });

const express = require('express');
const { WebSocketServer } = require('ws');
let oa = null;
if (GCID) { const { OAuth2Client } = require('google-auth-library'); oa = new OAuth2Client(GCID); }

const ITEM_KIND = {
  seed: ['seeds', CROP_IDS], crop: ['crops', CROP_IDS], furn: ['furn', FURN_IDS], good: ['goods', GOOD_IDS],
  cloth: ['clothes', CLOTHES], shoe: ['shoes', SHOES], bag: ['bags', BAGS]
};

const DBF = path.join(DATA, 'db.json');
let db = { users: {}, sessions: {} };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DBF, 'utf8'))); } catch (e) { /* mới */ }
const byCode = {};
Object.values(db.users).forEach(u => { byCode[u.code] = u; });
let dirty = false;
const mark = () => { dirty = true; };
function flush() {
  if (!dirty) return;
  dirty = false;
  try { fs.writeFileSync(DBF + '.tmp', JSON.stringify(db)); fs.renameSync(DBF + '.tmp', DBF); }
  catch (e) { console.error('Lỗi lưu db', e); dirty = true; }
}
setInterval(flush, 4000);
for (const s of ['SIGINT', 'SIGTERM']) process.on(s, () => { flush(); process.exit(0); });
const SESSION_MS = 30 * 864e5;
for (const t of Object.keys(db.sessions)) if (Date.now() - db.sessions[t].ts > SESSION_MS) delete db.sessions[t];

const newCode = () => { for (;;) { const c = crypto.randomBytes(4).toString('hex').slice(0, 6).toUpperCase(); if (!byCode[c]) return c; } };
const clean = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, n);
const isOwnerId = u => OWNERS.includes((u.email || '').toLowerCase()) || OWNERS.includes((u.sub || '').toLowerCase());
const ymd = () => new Date().toISOString().slice(0, 10);

function getOrCreate(sub, email, name) {
  let u = db.users[sub];
  if (!u) {
    u = db.users[sub] = {
      sub, email: email || '', code: newCode(), name: clean(name, 14) || 'Nông dân', role: 'player', title: '',
      save: null, look: null, friends: [], reqIn: [], pending: [], banned: false, mutedUntil: 0,
      discordId: sub.startsWith('d:') ? sub.slice(2) : null, linkCode: null, linkExp: 0,
      lastCheckin: '', createdAt: Date.now()
    };
    byCode[u.code] = u;
  }
  if (isOwnerId(u) && u.role !== 'owner') u.role = 'owner';
  mark();
  return u;
}
const pub = u => ({
  code: u.code, name: u.name, role: u.role, title: u.title || '', save: u.save, look: u.look,
  lastCheckin: u.lastCheckin || '', discordId: !!u.discordId
});
function sessionUser(token) {
  const s = db.sessions[token];
  if (!s || Date.now() - s.ts > SESSION_MS) return null;
  return db.users[s.sub] || null;
}

const conns = new Map();
const sendWs = (ws, o) => { try { if (ws.readyState === 1) ws.send(JSON.stringify(o)); } catch (e) { /* bỏ */ } };
const sendTo = (code, o) => { const c = conns.get(code); if (c) sendWs(c.ws, o); };
const bcast = (o, except) => { for (const [code, c] of conns) if (code !== except) sendWs(c.ws, o); };
let bridge = null;

function friendsList(u) {
  return u.friends.map(c => {
    const f = byCode[c];
    return { code: c, name: f ? f.name : '?', role: f ? f.role : 'player', title: f ? f.title : '', online: conns.has(c), lv: f && f.save ? f.save.lv || 1 : 1 };
  });
}
function reqList(u) { return u.reqIn.map(c => ({ code: c, name: byCode[c] ? byCode[c].name : '?' })); }
function pushFriends(u) { sendTo(u.code, { t: 'friends', list: friendsList(u), reqIn: reqList(u) }); }
function addPending(u, item) {
  item.id = crypto.randomBytes(6).toString('hex');
  item.ts = Date.now();
  u.pending.push(item);
  if (u.pending.length > 200) u.pending.shift();
  mark();
  sendTo(u.code, { t: 'pending', items: [item] });
}
function adminLog(line) {
  try { fs.appendFileSync(path.join(DATA, 'admin.log'), new Date().toISOString() + ' ' + line + '\n'); } catch (e) { /* bỏ */ }
}

function doCheckin(u, source) {
  const day = ymd();
  if (u.lastCheckin === day) return { ok: false, msg: 'Hôm nay bạn đã điểm danh rồi. Quay lại vào ngày mai nhé!' };
  u.lastCheckin = day;
  const coins = 80 + (u.save && u.save.lv ? u.save.lv * 12 : 12);
  addPending(u, {
    title: 'Điểm danh hằng ngày 🎁',
    body: `Điểm danh ${source} — nhận ${coins} xu và 1 đôi giày điểm danh!`,
    gift: { coins, shoes: { s_checkin: 1 } }, auto: true, from: 'Hệ thống'
  });
  sendTo(u.code, { t: 'announce', text: 'Điểm danh thành công! Nhận xu + giày trong hòm thư.' });
  mark();
  if (bridge && bridge.checkin) bridge.checkin(u, 'điểm danh thành công (+' + coins + ' xu + giày)');
  return { ok: true, msg: `Điểm danh OK! +${coins} xu và giày điểm danh đã gửi vào game.` };
}

function issueSession(u) {
  const token = crypto.randomBytes(24).toString('hex');
  db.sessions[token] = { sub: u.sub, ts: Date.now() };
  mark();
  return token;
}

function adminDo(actor, cmd, a) {
  a = a || {};
  const needLv = { info: 2, kick: 2, mute: 2, announce: 3, coins: 3, item: 3, mail: 3, ban: 3, unban: 3, settitle: 3, setrole: 4 }[cmd];
  if (!needLv) return { ok: false, msg: 'Lệnh không tồn tại' };
  if ((ROLE_LV[actor.role] || 0) < needLv) return { ok: false, msg: 'Bạn không đủ quyền cho lệnh này' };
  if (cmd === 'announce') {
    const text = clean(a.text, 200);
    if (!text) return { ok: false, msg: 'Thiếu nội dung' };
    bcast({ t: 'announce', text });
    if (bridge) bridge.announce(text);
    adminLog(`${actor.label} announce: ${text}`);
    return { ok: true, msg: 'Đã gửi thông báo toàn server' };
  }
  const target = String(a.target || '').trim().toUpperCase();
  const u = byCode[target];
  if (!u) return { ok: false, msg: 'Không tìm thấy người chơi mã ' + (target || '(trống)') };
  if (cmd !== 'info' && actor.role !== 'owner' && (ROLE_LV[u.role] || 0) >= (ROLE_LV[actor.role] || 0))
    return { ok: false, msg: 'Không thể tác động lên người có quyền ngang hoặc cao hơn' };
  let res;
  switch (cmd) {
    case 'info': {
      const sv = u.save || {};
      res = { ok: true, msg: `${u.name} [${u.code}] role=${u.role} title=${u.title || '-'} Lv.${sv.lv || 1} xu=${Math.floor(sv.coins || 0)} ${conns.has(u.code) ? 'ONLINE' : 'offline'}${u.banned ? ' BỊ BAN' : ''}${u.discordId ? ' discord=' + u.discordId : ''}` };
      break;
    }
    case 'coins': {
      const n = Math.trunc(+a.amount);
      if (!n || Math.abs(n) > 1e9) return { ok: false, msg: 'Số xu không hợp lệ (±1 tỷ)' };
      addPending(u, { title: n > 0 ? 'Admin cộng xu 🪙' : 'Admin trừ xu', body: `${actor.label} ${n > 0 ? 'đã cộng' : 'đã trừ'} ${Math.abs(n).toLocaleString('vi')} xu`, gift: { coins: n }, auto: true, from: 'Admin' });
      res = { ok: true, msg: `Đã ${n > 0 ? 'cộng' : 'trừ'} ${Math.abs(n)} xu cho ${u.name} [${u.code}]` };
      break;
    }
    case 'item': {
      const [kind, id] = String(a.item || '').split(':');
      const k = ITEM_KIND[kind];
      const n = Math.trunc(+a.amount);
      if (!k || !k[1].includes(id)) return { ok: false, msg: 'Vật phẩm không hợp lệ' };
      if (!(n >= 1 && n <= 9999)) return { ok: false, msg: 'Số lượng 1–9999' };
      addPending(u, { title: 'Admin tặng vật phẩm 🎁', body: `${actor.label} tặng bạn ${n} × ${id}`, gift: { [k[0]]: { [id]: n } }, auto: true, from: 'Admin' });
      res = { ok: true, msg: `Đã gửi ${n} × ${id} cho ${u.name}` };
      break;
    }
    case 'mail': {
      const title = clean(a.title, 60) || 'Thư từ Admin', body = clean(a.text, 300);
      const coins = Math.trunc(+a.amount) || 0;
      addPending(u, { title, body, gift: coins ? { coins } : null, auto: false, from: 'Admin' });
      res = { ok: true, msg: 'Đã gửi thư cho ' + u.name };
      break;
    }
    case 'kick':
      sendTo(u.code, { t: 'kick', reason: 'Bạn bị đưa ra khỏi game bởi quản trị viên' });
      { const c = conns.get(u.code); if (c) c.ws.close(4003); }
      res = { ok: true, msg: 'Đã kick ' + u.name };
      break;
    case 'ban':
      u.banned = true;
      for (const t of Object.keys(db.sessions)) if (db.sessions[t].sub === u.sub) delete db.sessions[t];
      sendTo(u.code, { t: 'kick', reason: 'Tài khoản đã bị khóa' });
      { const c = conns.get(u.code); if (c) c.ws.close(4003); }
      res = { ok: true, msg: 'Đã khóa tài khoản ' + u.name };
      break;
    case 'unban': u.banned = false; res = { ok: true, msg: 'Đã mở khóa ' + u.name }; break;
    case 'mute': {
      const m = Math.max(0, Math.min(10080, Math.trunc(+a.minutes) || 0));
      u.mutedUntil = m ? Date.now() + m * 60000 : 0;
      res = { ok: true, msg: m ? `Đã cấm chat ${u.name} ${m} phút` : 'Đã bỏ cấm chat ' + u.name };
      break;
    }
    case 'setrole': {
      const r = String(a.role || '');
      if (!(r in ROLE_LV)) return { ok: false, msg: 'Role không hợp lệ (player/vip/mod/admin/owner)' };
      if (OWNERS.length && isOwnerId(u) && r !== 'owner') return { ok: false, msg: 'Không thể hạ role của chủ game đặt trong .env' };
      u.role = r;
      sendTo(u.code, { t: 'role', role: r, title: u.title || '' });
      res = { ok: true, msg: `Đã đặt role ${r} cho ${u.name} [${u.code}]` };
      break;
    }
    case 'settitle': {
      const t = String(a.title || '');
      if (t && !(t in CUTE_TITLES)) return { ok: false, msg: 'Danh hiệu không hợp lệ' };
      u.title = t;
      sendTo(u.code, { t: 'role', role: u.role, title: t });
      res = { ok: true, msg: t ? `Đã cấp ${CUTE_TITLES[t].n} cho ${u.name}` : `Đã gỡ danh hiệu của ${u.name}` };
      break;
    }
  }
  mark();
  adminLog(`${actor.label} ${cmd} -> ${u.code} ${JSON.stringify(a)}`);
  return res;
}

const app = express();
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
app.use(express.json({ limit: '100kb' }));
app.get('/config.js', (req, res) => {
  res.type('js').send('window.CFG=' + JSON.stringify({
    googleClientId: GCID, server: '', dev: DEV, discord: !!DISCORD_CID,
    invite: INVITE, donate: { bank: 'MB', acc: '00180599999998', name: 'BUI CHI KIET' },
    titles: CUTE_TITLES
  }) + ';');
});
app.get('/api/health', (req, res) => res.json({ ok: true, online: conns.size }));

const hits = new Map();
setInterval(() => hits.clear(), 60000);
function rate(req, res) {
  const ip = req.ip;
  hits.set(ip, (hits.get(ip) || 0) + 1);
  if (hits.get(ip) > 40) { res.status(429).json({ error: 'Thử lại sau ít phút' }); return true; }
  return false;
}

function finishLogin(res, u, method) {
  if (u.banned) return res.status(403).json({ error: 'Tài khoản đã bị khóa' });
  const token = issueSession(u);
  if (bridge && bridge.login) try { bridge.login(u, method); } catch (e) { /* */ }
  return res.json({ token, user: pub(u) });
}

app.post('/api/login', async (req, res) => {
  if (rate(req, res)) return;
  try {
    const { credential, dev } = req.body || {};
    let sub, email, name;
    if (credential) {
      if (!oa) return res.status(400).json({ error: 'Server chưa cấu hình GOOGLE_CLIENT_ID' });
      const t = await oa.verifyIdToken({ idToken: String(credential), audience: GCID });
      const p = t.getPayload();
      sub = 'g:' + p.sub; email = p.email; name = p.name;
    } else if (dev && DEV) {
      const nm = clean(dev, 14).replace(/[^\p{L}\p{N} _-]/gu, '');
      if (!nm) return res.status(400).json({ error: 'Tên không hợp lệ' });
      sub = 'dev:' + nm.toLowerCase(); email = ''; name = nm;
    } else return res.status(400).json({ error: 'Thiếu thông tin đăng nhập' });
    finishLogin(res, getOrCreate(sub, email, name), credential ? 'Google' : 'Thử nghiệm');
  } catch (e) {
    console.error('login', e.message);
    res.status(401).json({ error: 'Đăng nhập thất bại' });
  }
});

function redirectUri(req) {
  if (E.DISCORD_REDIRECT_URI) return E.DISCORD_REDIRECT_URI;
  if (PUBLIC_URL) return PUBLIC_URL + '/auth/discord/callback';
  return req.protocol + '://' + req.get('host') + '/auth/discord/callback';
}
app.get('/auth/discord', (req, res) => {
  if (!DISCORD_CID) return res.status(400).send('Chưa cấu hình DISCORD_CLIENT_ID');
  const url = 'https://discord.com/api/oauth2/authorize?client_id=' + encodeURIComponent(DISCORD_CID)
    + '&redirect_uri=' + encodeURIComponent(redirectUri(req))
    + '&response_type=code&scope=identify%20email';
  res.redirect(url);
});
app.get('/auth/discord/callback', async (req, res) => {
  try {
    const code = String(req.query.code || '');
    if (!code || !DISCORD_CID || !DISCORD_SEC) return res.redirect('/?err=discord');
    const body = new URLSearchParams({
      client_id: DISCORD_CID, client_secret: DISCORD_SEC, grant_type: 'authorization_code',
      code, redirect_uri: redirectUri(req)
    });
    const tok = await fetch('https://discord.com/api/oauth2/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body
    }).then(r => r.json());
    if (!tok.access_token) return res.redirect('/?err=discord');
    const me = await fetch('https://discord.com/api/users/@me', { headers: { Authorization: 'Bearer ' + tok.access_token } }).then(r => r.json());
    const u = getOrCreate('d:' + me.id, me.email || '', me.global_name || me.username);
    u.discordId = me.id; mark();
    if (u.banned) return res.redirect('/?err=banned');
    const token = issueSession(u);
    if (bridge && bridge.login) try { bridge.login(u, 'Discord'); } catch (e) { /* */ }
    res.redirect('/?token=' + encodeURIComponent(token));
  } catch (e) {
    console.error('discord oauth', e.message);
    res.redirect('/?err=discord');
  }
});

app.get('/api/me', (req, res) => {
  const u = sessionUser((req.headers.authorization || '').replace('Bearer ', ''));
  if (!u || u.banned) return res.status(401).json({ error: 'Phiên hết hạn' });
  res.json({ user: pub(u) });
});
app.post('/api/checkin', (req, res) => {
  const u = sessionUser((req.headers.authorization || '').replace('Bearer ', ''));
  if (!u || u.banned) return res.status(401).json({ error: 'Phiên hết hạn' });
  res.json(doCheckin(u, 'trong game'));
});
app.use(express.static(path.join(__dirname, 'public'), { setHeaders: r => r.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate') }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2 * 1024 * 1024 });
const num = (v, lo, hi) => Math.max(lo, Math.min(hi, +v || 0));
const COLOR = /^#[0-9a-fA-F]{6}$/;

wss.on('connection', (ws, req) => {
  const token = new URL(req.url, 'http://x').searchParams.get('token') || '';
  const u = sessionUser(token);
  if (!u || u.banned) { ws.close(4001); return; }
  const old = conns.get(u.code);
  if (old) { sendWs(old.ws, { t: 'kick', reason: 'Tài khoản đăng nhập ở nơi khác' }); old.ws.close(4003); }
  const c = { ws, u, map: 1, lv: 1, lastChat: 0, lastPos: 0 };
  conns.set(u.code, c);
  sendWs(ws, { t: 'init', role: u.role, title: u.title || '', friends: friendsList(u), reqIn: reqList(u), online: conns.size, lastCheckin: u.lastCheckin || '', code: u.code });
  if (u.pending.length) sendWs(ws, { t: 'pending', items: u.pending });
  u.friends.forEach(fc => { const f = byCode[fc]; if (f) pushFriends(f); });

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    const now = Date.now();
    switch (m.t) {
      case 'pos': {
        if (now - c.lastPos < 90) return;
        c.lastPos = now; c.map = num(m.map, 1, 200) | 0; c.lv = num(m.lv, 1, 99) | 0;
        bcast({ t: 'pos', id: u.code, name: u.name, role: u.role, title: u.title, look: u.look, map: c.map, x: num(m.x, 0, 5000), y: num(m.y, 0, 5000), dir: ['u', 'd', 'l', 'r'].includes(m.dir) ? m.dir : 'd', veh: num(m.veh, 0, 15) | 0, led: !!m.led, lv: c.lv }, u.code);
        break;
      }
      case 'profile': {
        const nm = clean(m.name, 14);
        if (nm) u.name = nm;
        const l = m.look || {};
        if (COLOR.test(l.skin) && COLOR.test(l.hc) && COLOR.test(l.oc)) {
          u.look = {
            skin: l.skin, hc: l.hc, oc: l.oc,
            hair: num(l.hair, 0, 8) | 0,
            outfit: CLOTHES.includes(l.outfit) ? l.outfit : (l.outfit || ''),
            shoes: SHOES.includes(l.shoes) ? l.shoes : (l.shoes || ''),
            bag: BAGS.includes(l.bag) ? l.bag : (l.bag || ''),
            led: !!l.led
          };
        }
        mark();
        break;
      }
      case 'save': {
        if (!m.data || typeof m.data !== 'object') return;
        if (JSON.stringify(m.data).length > 1.5e6) return;
        u.save = m.data; u.save.id = u.code;
        mark();
        break;
      }
      case 'chat': {
        const text = clean(m.text, 120);
        if (!text || now - c.lastChat < 800) return;
        if (u.mutedUntil > now) { sendWs(ws, { t: 'adm_res', msg: 'Bạn đang bị cấm chat' }); return; }
        c.lastChat = now;
        bcast({ t: 'chat', name: u.name, role: u.role, title: u.title, lv: c.lv, text }, u.code);
        if (bridge) bridge.chat(u.name, u.role, text);
        break;
      }
      case 'fr_req': {
        const t = byCode[String(m.to || '').trim().toUpperCase()];
        if (!t || t === u) return sendWs(ws, { t: 'adm_res', msg: 'Không tìm thấy mã người chơi' });
        if (u.friends.includes(t.code)) return sendWs(ws, { t: 'adm_res', msg: 'Đã là bạn rồi' });
        if (u.reqIn.includes(t.code)) { u.reqIn = u.reqIn.filter(x => x !== t.code); u.friends.push(t.code); t.friends.push(u.code); pushFriends(u); pushFriends(t); sendWs(ws, { t: 'adm_res', msg: '💕 Đã kết bạn với ' + t.name }); mark(); return; }
        if (!t.reqIn.includes(u.code)) t.reqIn.push(u.code);
        pushFriends(t); mark();
        sendWs(ws, { t: 'adm_res', msg: '📨 Đã gửi lời mời tới ' + t.name });
        break;
      }
      case 'fr_acc': {
        const id = String(m.id);
        const o = byCode[id];
        if (!o || !u.reqIn.includes(id)) return;
        u.reqIn = u.reqIn.filter(x => x !== id);
        if (!u.friends.includes(id)) u.friends.push(id);
        if (!o.friends.includes(u.code)) o.friends.push(u.code);
        pushFriends(u); pushFriends(o); mark();
        sendTo(o.code, { t: 'adm_res', msg: '💕 ' + u.name + ' đã chấp nhận kết bạn' });
        break;
      }
      case 'fr_rej': u.reqIn = u.reqIn.filter(x => x !== String(m.id)); pushFriends(u); mark(); break;
      case 'fr_del': {
        const id = String(m.id), o = byCode[id];
        u.friends = u.friends.filter(x => x !== id);
        if (o) { o.friends = o.friends.filter(x => x !== u.code); pushFriends(o); }
        pushFriends(u); mark();
        break;
      }
      case 'gift': {
        const t = byCode[String(m.to)], n = Math.trunc(+m.coins);
        if (!t || !u.friends.includes(t.code) || !(n >= 1 && n <= 1000)) return;
        addPending(t, { title: 'Quà từ ' + u.name + ' 🎁', body: u.name + ' tặng bạn ' + n + ' xu!', gift: { coins: n }, auto: true, from: u.name });
        break;
      }
      case 'ack': {
        if (!Array.isArray(m.ids)) return;
        const ids = new Set(m.ids.map(String));
        u.pending = u.pending.filter(p => !ids.has(p.id));
        mark();
        break;
      }
      case 'link': {
        u.linkCode = String(u.code);
        u.linkExp = now + 24 * 3600000;
        mark();
        sendWs(ws, { t: 'link_code', code: u.code });
        break;
      }
      case 'checkin': {
        const r = doCheckin(u, 'trong game');
        sendWs(ws, { t: 'adm_res', msg: r.msg, lastCheckin: u.lastCheckin });
        break;
      }
      case 'copy_code':
        sendWs(ws, { t: 'link_code', code: u.code });
        break;
      case 'adm': {
        const r = adminDo({ role: u.role, label: u.name + '(' + u.code + ')' }, String(m.cmd), m.args);
        sendWs(ws, { t: 'adm_res', msg: r.msg });
        break;
      }
    }
  });
  ws.on('close', () => {
    if (conns.get(u.code) && conns.get(u.code).ws === ws) {
      conns.delete(u.code);
      bcast({ t: 'left', id: u.code });
      u.friends.forEach(fc => { const f = byCode[fc]; if (f) pushFriends(f); });
    }
  });
  ws.on('error', () => { });
});

const ctx = {
  DATA, adminDo,
  findByCode: c => byCode[String(c || '').trim().toUpperCase()] || null,
  roleOfDiscord: id => { const u = Object.values(db.users).find(x => x.discordId === id); return u ? u.role : 'player'; },
  linkDiscord(code, discordId) {
    const raw = String(code || '').trim();
    let u = Object.values(db.users).find(x => x.linkCode && x.linkCode === raw && x.linkExp > Date.now());
    if (!u) u = byCode[raw.toUpperCase()];
    if (!u) return null;
    u.discordId = discordId; u.linkCode = null; mark();
    return u;
  },
  checkinByCode(code, discordId) {
    const u = byCode[String(code || '').trim().toUpperCase()];
    if (!u) return { ok: false, msg: 'Không tìm thấy mã người chơi. Copy mã trong sảnh/game rồi thử lại.' };
    if (u.discordId && u.discordId !== discordId) return { ok: false, msg: 'Mã này đã liên kết Discord khác.' };
    if (!u.discordId) { u.discordId = discordId; mark(); }
    return doCheckin(u, 'Discord');
  },
  setTitleByDiscord(discordId, title) {
    const u = Object.values(db.users).find(x => x.discordId === discordId);
    if (!u || !(title in CUTE_TITLES)) return null;
    u.title = title; mark();
    sendTo(u.code, { t: 'role', role: u.role, title });
    return u;
  },
  online: () => [...conns.values()].map(c => ({ name: c.u.name, code: c.u.code, role: c.u.role, lv: c.lv })),
  top: n => Object.values(db.users).filter(u => u.save).sort((a, b) => (b.save.lv || 1) - (a.save.lv || 1) || (b.save.coins || 0) - (a.save.coins || 0)).slice(0, n).map(u => ({ name: u.name, code: u.code, lv: u.save.lv || 1, coins: Math.floor(u.save.coins || 0), role: u.role })),
  chatFromDiscord: (name, text) => bcast({ t: 'chat', name: clean(name, 20), role: 'discord', lv: 0, text: clean(text, 120) }),
  setBridge: b => { bridge = b; }
};
if (E.DISCORD_TOKEN) {
  try { require('./discord')(ctx); } catch (e) { console.error('Không khởi động được bot Discord:', e.message); }
} else console.log('ℹ️  Chưa có DISCORD_TOKEN – bỏ qua bot Discord');

server.listen(PORT, () => {
  console.log(`🌱 Nông Trại http://localhost:${PORT}  Google:${GCID ? 'on' : 'off'} DiscordOAuth:${DISCORD_CID ? 'on' : 'off'} Dev:${DEV ? 'on' : 'off'}`);
});
module.exports = { adminDo };
