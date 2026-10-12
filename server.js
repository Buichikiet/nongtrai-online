'use strict';
const fs = require('fs'), path = require('path'), http = require('http'), crypto = require('crypto');
const axios = require('axios');

try {
  for (const l of fs.readFileSync(path.join(__dirname, '.env'), 'utf8').split(/\r?\n/)) {
    const m = l.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (m && !(m[1] in process.env)) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
} catch (e) {}
const E = process.env;
const PORT = +E.PORT || 3000;
const GCID = E.GOOGLE_CLIENT_ID || '';
const DEV = E.DEV_LOGIN === '1';
const DATA = E.DATA_DIR || path.join(__dirname, 'data');
fs.mkdirSync(DATA, { recursive: true });
const MONGO_URL = E.MONGO_URL || '';

const express = require('express');
const { WebSocketServer } = require('ws');
const { MongoClient } = require('mongodb');
let oa = null;
if (GCID) { const { OAuth2Client } = require('google-auth-library'); oa = new OAuth2Client(GCID); }

// ====== MONGO SETUP ======
let mongoClient = null, mongoDb = null, usersCol = null;
async function initMongo() {
  if (!MONGO_URL) { console.log('⚠️ Không có MONGO_URL — dùng file local'); return; }
  mongoClient = new MongoClient(MONGO_URL);
  await mongoClient.connect();
  mongoDb = mongoClient.db('nongtrai');
  usersCol = mongoDb.collection('users');
  console.log('✅ Đã kết nối MongoDB Atlas');
}
initMongo().catch(e => console.error('❌ Lỗi MongoDB:', e.message));

// ====== DB LOCAL (FALLBACK) ======
const DBF = path.join(DATA, 'db.json');
let db = { users: {}, sessions: {} };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DBF, 'utf8'))); } catch (e) {}
const byCode = {};
Object.values(db.users).forEach(u => { byCode[u.code] = u; });
let dirty = false;
const mark = () => { dirty = true; };
function flush() {
  if (!dirty) return;
  dirty = false;
  try { fs.writeFileSync(DBF + '.tmp', JSON.stringify(db)); fs.renameSync(DBF + '.tmp', DBF); }
  catch (e) { dirty = true; }
}
setInterval(flush, 4000);

// ====== SYNC MONGO ======
async function flushMongo() {
  if (!usersCol) return;
  const ops = Object.values(db.users).map(u => ({
    updateOne: { filter: { sub: u.sub }, update: { $set: u }, upsert: true }
  }));
  if (ops.length) await usersCol.bulkWrite(ops);
}
setInterval(() => flushMongo().catch(()=>{}), 5000);

async function loadFromMongo() {
  if (!usersCol) return;
  const all = await usersCol.find({}).toArray();
  all.forEach(u => { db.users[u.sub] = u; byCode[u.code] = u; });
  console.log('✅ Đã load', all.length, 'users từ MongoDB');
}
setTimeout(() => loadFromMongo().catch(e => console.error('Load Mongo lỗi:', e.message)), 3000);

// ====== UTILS ======
const newCode = () => { for (;;) { const c = crypto.randomBytes(4).toString('hex').slice(0,6).toUpperCase(); if (!byCode[c]) return c; } };
const clean = (s,n) => String(s==null?'':s).replace(/[\u0000-\u001f<>]/g,'').trim().slice(0,n);

function getOrCreate(sub, email, name, discordId=null) {
  let u = db.users[sub];
  if (!u) {
    let c = discordId ? discordId.slice(-6).toUpperCase() : newCode();
    if (byCode[c]) c = newCode();
    u = db.users[sub] = {
      sub, email: email||'', code: c, name: clean(name,14)||'Nông dân', role:'player', save:null, look:null,
      friends: [], reqIn: [], pending: [], banned:false, mutedUntil:0, discordId, createdAt:Date.now()
    };
    byCode[u.code] = u;
  }
  if (discordId && !u.discordId) u.discordId = discordId;
  mark(); flush(); flushMongo().catch(()=>{});
  return u;
}
const pub = u => ({ code:u.code, name:u.name, role:u.role, save:u.save, look:u.look });
function sessionUser(token) { const s = db.sessions[token]; if (!s) return null; return db.users[s.sub] || null; }

const conns = new Map();
const sendWs = (ws,o) => { try { if (ws.readyState===1) ws.send(JSON.stringify(o)); } catch(e){} };
const sendTo = (code,o) => { const c = conns.get(code); if (c) sendWs(c.ws,o); };
const bcast = (o, except) => { for (const [code,c] of conns) if (code !== except) sendWs(c.ws,o); };
let bridge = null;

function adminDo(actor, cmd, a) {
  a = a||{};
  const target = String(a.target||'').trim().toUpperCase();
  let u = byCode[target] || Object.values(db.users).find(x => x.discordId === target || x.discordId === a.target);
  if (!u) return { ok:false, msg:'Không tìm thấy người chơi mã hoặc Discord ID '+target };
  let res;
  switch(cmd) {
    case 'info': res = { ok:true, msg:`${u.name} [${u.code}] role=${u.role} Lv.${u.save?.lv||1}` }; break;
    case 'coins':
      const n = Math.trunc(+a.amount);
      u.save = u.save || { coins:0, lv:1, xp:0, map:1, inv:{seeds:{carrot:8},crops:{},goods:{},furn:{},clothes:[]}, sel:'carrot', houseLv:0, veh:0, vehOwned:[0], pet:-1, petOwned:[], maps:{}, friends:[], mail:[], lastDaily:'', led:true, settings:{music:.5,sfx:.7} };
      u.save.coins = Math.max(0, (u.save.coins||0) + n);
      sendTo(u.code, { t:'update_coins', coins: u.save.coins, delta: n });
      res = { ok:true, msg:`Đã cộng ${n} xu cho ${u.name}` };
      break;
    case 'setrole':
      const r = String(a.role||'');
      u.role = r;
      sendTo(u.code, { t:'role', role: r });
      res = { ok:true, msg:`Đã cấp role ${r} cho ${u.name}` };
      break;
    case 'announce':
      bcast({ t:'announce', text:clean(a.text,200) });
      res = { ok:true, msg:'Đã gửi thông báo' };
      break;
    default: res = { ok:true, msg:'Thực thi lệnh thành công' };
  }
  mark(); flush(); flushMongo().catch(()=>{});
  return res;
}

const app = express();
app.use((req,res,next) => {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Headers','Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  if (req.method==='OPTIONS') return res.sendStatus(204);
  next();
});
app.use(express.json({ limit:'100kb' }));
app.get('/config.js', (req,res) => res.type('js').send('window.CFG='+JSON.stringify({ googleClientId:GCID, server:'', dev:DEV })+';'));

// ĐĂNG NHẬP DISCORD
app.get('/api/auth/discord/callback', async (req,res) => {
  const code = req.query.code;
  if (!code) return res.status(400).send('Thiếu mã xác thực Discord!');
  try {
    const tokenResponse = await axios.post('https://discord.com/api/oauth2/token', new URLSearchParams({
      client_id: process.env.DISCORD_CLIENT_ID,
      client_secret: process.env.DISCORD_CLIENT_SECRET,
      grant_type: 'authorization_code',
      code: code,
      redirect_uri: process.env.DISCORD_REDIRECT_URI,
    }), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });

    const user = (await axios.get('https://discord.com/api/users/@me', {
      headers: { Authorization: `Bearer ${tokenResponse.data.access_token}` }
    })).data;

    const sub = 'discord:' + user.id;
    let u = Object.values(db.users).find(x => x.discordId === user.id) || db.users[sub];
    if (!u) u = getOrCreate(sub, user.email||'', user.username, user.id);
    else { u.discordId = user.id; if (user.username) u.name = clean(user.username,14); mark(); }

    if (bridge && typeof bridge.notifyLogin === 'function') bridge.notifyLogin(user.username, user.id);
    res.redirect(`/?discordId=${user.id}&username=${encodeURIComponent(user.username)}&userCode=${u.code}`);
  } catch (e) {
    console.error('Discord login error:', e.message);
    res.status(500).send('Đăng nhập Discord thất bại!');
  }
});

// ĐĂNG NHẬP GOOGLE
app.get('/api/auth/google/callback', async (req, res) => {
  const code = req.query.code;
  if (!code) return res.status(400).send('Thiếu mã xác thực Google!');
  try {
    const tokenResponse = await axios.post('https://oauth2.googleapis.com/token', new URLSearchParams({
      client_id: process.env.GOOGLE_CLIENT_ID,
      client_secret: process.env.GOOGLE_CLIENT_SECRET,
      grant_type: 'authorization_code',
      code: code,
      redirect_uri: 'https://nongtrai-online.onrender.com/api/auth/google/callback',
    }), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });

    const userInfo = (await axios.get('https://www.googleapis.com/oauth2/v2/userinfo', {
      headers: { Authorization: `Bearer ${tokenResponse.data.access_token}` }
    })).data;

    const sub = 'google:' + userInfo.id;
    let u = Object.values(db.users).find(x => x.email === userInfo.email) || db.users[sub];
    if (!u) u = getOrCreate(sub, userInfo.email, userInfo.name, null);
    else { if (userInfo.name) u.name = clean(userInfo.name, 14); mark(); }

    res.redirect(`/?discordId=${userInfo.id}&username=${encodeURIComponent(userInfo.name)}&userCode=${u.code}&provider=google`);
  } catch (e) {
    console.error('Google login error:', e.message);
    res.status(500).send('Đăng nhập Google thất bại!');
  }
});

// LƯU GAME
app.post('/api/save-game', (req,res) => {
  const { discordId, farmData } = req.body;
  if (!discordId) return res.status(400).json({ success:false });
  const u = Object.values(db.users).find(x => x.discordId === discordId || x.code === discordId);
  if (u) {
    u.save = farmData; u.save.id = u.code; mark(); flush();
    flushMongo().catch(()=>{});
  } else {
    fs.writeFileSync(path.join(DATA, `${discordId}.json`), JSON.stringify(farmData,null,2));
  }
  res.json({ success:true });
});

app.get('/api/load-game/:discordId', (req,res) => {
  const u = Object.values(db.users).find(x => x.discordId === req.params.discordId || x.code === req.params.discordId);
  if (u && u.save) return res.json({ success:true, farmData: u.save });
  const p = path.join(DATA, `${req.params.discordId}.json`);
  if (fs.existsSync(p)) return res.json({ success:true, farmData: JSON.parse(fs.readFileSync(p,'utf8')) });
  res.json({ success:false });
});

// API ADMIN
app.post('/api/admin/setrole', (req,res) => {
  const { target, role } = req.body;
  const r = adminDo({ role:'owner' }, 'setrole', { target, role });
  res.json(r);
});
app.post('/api/admin/coins', (req,res) => {
  const { target, amount } = req.body;
  const r = adminDo({ role:'owner' }, 'coins', { target, amount });
  res.json(r);
});

// KẾT BẠN
app.post('/api/friend/request', (req,res) => {
  const { from, target, name } = req.body;
  const u = byCode[target] || Object.values(db.users).find(x=>x.discordId===target);
  if (!u) return res.json({ ok:false, msg:'Không tìm thấy người chơi!' });
  if (!u.friends) u.friends = [];
  if (!u.friends.find(f=>f.id===from)) {
    u.friends.push({ id: from, name });
    sendTo(u.code, { t:'friend_request', from, name });
    mark(); flush(); flushMongo().catch(()=>{});
  }
  res.json({ ok:true });
});

app.post('/api/login', async (req,res) => {
  try {
    const { credential, dev } = req.body || {};
    let sub, email, name;
    if (dev && DEV) { sub = 'dev:'+dev.toLowerCase(); name = dev; }
    else return res.status(400).json({ error:'Yêu cầu đăng nhập hợp lệ' });
    const u = getOrCreate(sub, email, name);
    const token = crypto.randomBytes(24).toString('hex');
    db.sessions[token] = { sub: u.sub, ts: Date.now() };
    mark();
    res.json({ token, user: pub(u) });
  } catch(e) { res.status(401).json({ error:'Đăng nhập thất bại' }); }
});

app.get('/api/me', (req,res) => {
  const auth = req.headers.authorization || '';
  const token = auth.replace('Bearer ','');
  const u = sessionUser(token);
  if (!u) return res.status(401).json({ error:'Chưa đăng nhập' });
  res.json({ user: pub(u) });
});

app.use(express.static(path.join(__dirname, 'public')));
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path:'/ws' });

wss.on('connection', (ws, req) => {
  const token = new URL(req.url,'http://x').searchParams.get('token') || '';
  const u = sessionUser(token) || Object.values(db.users)[0];
  if (!u) { ws.close(4001); return; }
  const c = { ws, u, map:1, lv:1, lastChat:0, lastPos:0 };
  conns.set(u.code, c);
  sendWs(ws, { t:'init', role:u.role, online:conns.size });

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch(e){ return; }
    if (m.t==='pos') {
      bcast({ t:'pos', id:u.code, name:u.name, role:u.role, look:u.look, map:m.map, x:m.x, y:m.y, dir:m.dir, veh:m.veh, lv:m.lv }, u.code);
    } else if (m.t==='save') {
      u.save = m.data; u.save.id = u.code; mark(); flush(); flushMongo().catch(()=>{});
    } else if (m.t==='chat') {
      bcast({ t:'chat', name:u.name, role:u.role, lv:c.lv, text:clean(m.text,120) }, u.code);
    } else if (m.t==='friend_request') {
      const target = byCode[m.target];
      if (target) {
        if (!target.friends) target.friends = [];
        if (!target.friends.find(f=>f.id===u.code)) {
          target.friends.push({ id: u.code, name: u.name });
          sendTo(target.code, { t:'friend_request', from:u.code, name:u.name });
          mark(); flush(); flushMongo().catch(()=>{});
        }
      }
    } else if (m.t==='invite') {
      sendTo(m.target, { t:'invite', from:u.code, name:u.name, map:m.map });
    }
  });
  ws.on('close', () => {
    conns.delete(u.code);
    bcast({ t:'left', id:u.code }, u.code);
  });
});

const ctx = {
  adminDo,
  findByCode: c => byCode[String(c||'').trim().toUpperCase()] || null,
  roleOfDiscord: id => { const u = Object.values(db.users).find(x=>x.discordId===id); return u ? u.role : 'player'; },
  online: () => [...conns.values()].map(c => ({ name:c.u.name, code:c.u.code, role:c.u.role, lv:c.lv })),
  top: n => Object.values(db.users).filter(u=>u.save).sort((a,b)=>(b.save.lv||1)-(a.save.lv||1)).slice(0,n).map(u=>({ name:u.name, code:u.code, lv:u.save.lv||1, coins:u.save.coins||0, role:u.role })),
  setBridge: b => { bridge = b; }
};

if (E.DISCORD_TOKEN) {
  try { require('./discord')(ctx); } catch(e) { console.error('Lỗi bot Discord:', e.message); }
}

server.listen(PORT, () => console.log(`🌱 Server Nông Trại VIP chạy tại http://localhost:${PORT}`));
module.exports = { adminDo };
