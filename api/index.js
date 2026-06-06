// ============================================================
//  ZN STORE — api/index.js
//  Vercel Serverless Function
//  يعوّض api.php بالكامل — Upstash Redis للبيانات
// ============================================================

import { Redis } from '@upstash/redis';
import { put, del } from '@vercel/blob';
import crypto from 'crypto';
import sharp from 'sharp';

// ─── Redis Client ────────────────────────────────────────────
const redis = new Redis({
  url:   process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// ─── ثوابت ───────────────────────────────────────────────────
const ADMIN_PASSWORD    = process.env.ADMIN_PASSWORD   || 'Zabzabikk@29';
const ADMIN_BOT_TOKEN   = process.env.ADMIN_BOT_TOKEN  || '';
const USER_BOT_TOKEN    = process.env.USER_BOT_TOKEN   || '';
const ADMIN_CHAT_ID     = process.env.ADMIN_CHAT_ID    || '';
const B2_KEY_ID         = process.env.B2_KEY_ID        || '';
const B2_APP_KEY        = process.env.B2_APP_KEY       || '';
const B2_BUCKET         = process.env.B2_BUCKET        || 'zn-store-products';
const B2_ENDPOINT       = process.env.B2_ENDPOINT      || 's3.us-east-005.backblazeb2.com';
const B2_PUBLIC_URL     = process.env.B2_PUBLIC_URL    || 'https://f005.backblazeb2.com/file/zn-store-products';

const REWARD_BRONZE     = 400;
const REWARD_GOLD       = 450;
const REWARD_DIAMOND    = 500;
const LEVEL_BRONZE_MIN  = 4000;
const LEVEL_GOLD_MIN    = 40000;
const LEVEL_DIAMOND_MIN = 100000;
const MIN_WITHDRAWAL    = 4000;
const SESSION_LIFETIME  = 2592000; // 30 يوم (ثانية)

const CATEGORIES = [
  'أكسسوارات','أكسسوارات السيارات والدراجات','أكسسوارات الكومبيوتر',
  'المطبخ','إلكترونيات','المنزل','الديكور','أدوات الأشغال اليدوية',
  'حقائب الظهر','العناية و الجمال','أدوات رياضية','التخييم والتنزه',
  'لعبة وألعاب','متنوع','الأمومة','électroménager','المدرسة',
  'Vacance & Loisir','أطفال'
];

// ─── Redis Keys ───────────────────────────────────────────────
const K = {
  users:          () => 'zn:users',
  user:           (id) => `zn:user:${id}`,
  products:       () => 'zn:products',
  product:        (id) => `zn:product:${id}`,
  productIds:     () => 'zn:product:ids',
  orders:         () => 'zn:orders',
  order:          (id) => `zn:order:${id}`,
  orderIds:       () => 'zn:order:ids',
  token:          (t)  => `zn:token:${t}`,
  notifications:  (uid)=> `zn:notif:${uid}`,
  withdrawals:    () => 'zn:withdrawals',
  withdrawal:     (id) => `zn:withdrawal:${id}`,
  withdrawalIds:  () => 'zn:withdrawal:ids`,
  shipping:       () => 'zn:shipping',
  rateLimits:     (ip) => `zn:rl:${ip}`,
};

// ─── أدوات مساعدة ─────────────────────────────────────────────
function generateID(prefix = '') {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
}

function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.createHmac('sha256', salt).update(password).digest('hex');
  return `${salt}:${hash}`;
}

function verifyPassword(password, stored) {
  const [salt, hash] = stored.split(':');
  const testHash = crypto.createHmac('sha256', salt).update(password).digest('hex');
  return testHash === hash;
}

function validatePhone(phone) {
  return /^(05|06|07)[0-9]{8}$/.test(phone);
}

function sanitize(str) {
  return String(str || '').trim().replace(/<[^>]*>/g, '');
}

function ok(data = {}, message = '') {
  return { success: true, message, data };
}

function fail(message = 'خطأ') {
  return { success: false, message, data: {} };
}

function getUserLevel(totalEarned) {
  if (totalEarned >= LEVEL_DIAMOND_MIN)
    return { level:'diamond', name:'ألماسي', icon:'💎', reward:REWARD_DIAMOND, next_level:null, next_min:null };
  if (totalEarned >= LEVEL_GOLD_MIN)
    return { level:'gold',    name:'ذهبي',   icon:'🥇', reward:REWARD_GOLD,    next_level:'diamond', next_min:LEVEL_DIAMOND_MIN };
  if (totalEarned >= LEVEL_BRONZE_MIN)
    return { level:'bronze',  name:'برونزي', icon:'🥉', reward:REWARD_BRONZE,  next_level:'gold',    next_min:LEVEL_GOLD_MIN };
  return   { level:'none',    name:'عادي',   icon:'⭕', reward:REWARD_BRONZE,  next_level:'bronze',  next_min:LEVEL_BRONZE_MIN };
}

// ─── Redis Helpers ─────────────────────────────────────────────

// المستخدمون
async function getUser(uid) {
  return await redis.hgetall(K.user(uid));
}

async function saveUser(uid, user) {
  // Redis HSET لا يدعم nested objects — نحوّل الحقول المعقدة إلى JSON
  const flat = {};
  for (const [k, v] of Object.entries(user)) {
    flat[k] = typeof v === 'object' ? JSON.stringify(v) : String(v ?? '');
  }
  await redis.hset(K.user(uid), flat);
  await redis.sadd(K.users(), uid);
}

async function getAllUsers() {
  const ids = await redis.smembers(K.users());
  if (!ids || ids.length === 0) return {};
  const users = {};
  await Promise.all(ids.map(async (id) => {
    const u = await getUser(id);
    if (u) users[id] = parseUser(u);
  }));
  return users;
}

function parseUser(raw) {
  if (!raw) return null;
  const u = { ...raw };
  if (typeof u.payment_methods === 'string') {
    try { u.payment_methods = JSON.parse(u.payment_methods); } catch { u.payment_methods = []; }
  }
  u.wallet_balance  = parseFloat(u.wallet_balance  || 0);
  u.total_earned    = parseFloat(u.total_earned    || 0);
  u.total_withdrawn = parseFloat(u.total_withdrawn || 0);
  return u;
}

// المنتجات (مخزّنة كـ JSON array واحد لسهولة القراءة)
async function getProducts() {
  const raw = await redis.get(K.products());
  if (!raw) return [];
  try { return JSON.parse(raw); } catch { return []; }
}

async function saveProducts(products) {
  await redis.set(K.products(), JSON.stringify(products));
}

// الطلبات
async function getOrder(id) {
  const raw = await redis.get(K.order(id));
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

async function saveOrder(id, order) {
  await redis.set(K.order(id), JSON.stringify(order));
  await redis.sadd(K.orderIds(), id);
}

async function getAllOrders() {
  const ids = await redis.smembers(K.orderIds());
  if (!ids || ids.length === 0) return {};
  const orders = {};
  await Promise.all(ids.map(async (id) => {
    const o = await getOrder(id);
    if (o) orders[id] = o;
  }));
  return orders;
}

// التوكنات
async function saveToken(userId, token) {
  const expires = Math.floor(Date.now() / 1000) + SESSION_LIFETIME;
  await redis.set(K.token(token), JSON.stringify({ user_id: userId, expires }), { ex: SESSION_LIFETIME });
}

async function validateToken(token) {
  if (!token || token.length !== 64) return null;
  const raw = await redis.get(K.token(token));
  if (!raw) return null;
  try {
    const data = typeof raw === 'string' ? JSON.parse(raw) : raw;
    if (data.expires < Math.floor(Date.now() / 1000)) {
      await redis.del(K.token(token));
      return null;
    }
    // تجديد التوكن إذا بقي أقل من 7 أيام
    const remaining = data.expires - Math.floor(Date.now() / 1000);
    if (remaining < 604800) {
      await redis.expire(K.token(token), SESSION_LIFETIME);
      data.expires = Math.floor(Date.now() / 1000) + SESSION_LIFETIME;
      await redis.set(K.token(token), JSON.stringify(data), { ex: SESSION_LIFETIME });
    }
    return data.user_id;
  } catch { return null; }
}

async function deleteToken(token) {
  if (token) await redis.del(K.token(token));
}

// الإشعارات
async function addNotification(userId, title, message, type = 'info') {
  const key  = K.notifications(userId);
  const raw  = await redis.get(key);
  let notifs = [];
  try { notifs = raw ? JSON.parse(raw) : []; } catch {}
  notifs.unshift({
    id: generateID('notif'), title, message, type,
    read: false, created_at: new Date().toISOString()
  });
  notifs = notifs.slice(0, 50);
  await redis.set(key, JSON.stringify(notifs));
}

// طلبات السحب
async function getWithdrawal(id) {
  const raw = await redis.get(K.withdrawal(id));
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

async function saveWithdrawal(id, data) {
  await redis.set(K.withdrawal(id), JSON.stringify(data));
  await redis.sadd(K.withdrawalIds(), id);
}

async function getAllWithdrawals() {
  const ids = await redis.smembers(K.withdrawalIds());
  if (!ids || ids.length === 0) return {};
  const result = {};
  await Promise.all(ids.map(async (id) => {
    const w = await getWithdrawal(id);
    if (w) result[id] = w;
  }));
  return result;
}

// الشحن (مخزّن مرة واحدة عند أول تشغيل)
async function getShipping() {
  const raw = await redis.get(K.shipping());
  if (raw) {
    try { return JSON.parse(raw); } catch {}
  }
  // القيم الافتراضية للولايات الجزائرية (من shipping.json)
  const defaultShipping = [
    {"id":1,"name":"أدرار","home":1000,"office":600},
    {"id":2,"name":"الشلف","home":600,"office":350},
    {"id":3,"name":"الأغواط","home":750,"office":500},
    {"id":4,"name":"أم البواقي","home":700,"office":400},
    {"id":5,"name":"باتنة","home":650,"office":350},
    {"id":6,"name":"بجاية","home":550,"office":300},
    {"id":7,"name":"بسكرة","home":800,"office":500},
    {"id":8,"name":"بشار","home":1050,"office":700},
    {"id":9,"name":"البليدة","home":250,"office":null},
    {"id":10,"name":"البويرة","home":600,"office":400},
    {"id":11,"name":"تمنراست","home":1200,"office":600},
    {"id":12,"name":"تبسة","home":700,"office":450},
    {"id":13,"name":"تلمسان","home":700,"office":400},
    {"id":14,"name":"تيارت","home":700,"office":400},
    {"id":15,"name":"تيزي وزو","home":550,"office":350},
    {"id":16,"name":"الجزائر","home":250,"office":100},
    {"id":17,"name":"الجلفة","home":750,"office":500},
    {"id":18,"name":"جيجل","home":600,"office":400},
    {"id":19,"name":"سطيف","home":550,"office":350},
    {"id":20,"name":"سعيدة","home":750,"office":450},
    {"id":21,"name":"سكيكدة","home":650,"office":400},
    {"id":22,"name":"سيدي بلعباس","home":650,"office":400},
    {"id":23,"name":"عنابة","home":550,"office":350},
    {"id":24,"name":"قالمة","home":700,"office":400},
    {"id":25,"name":"قسنطينة","home":550,"office":350},
    {"id":26,"name":"المدية","home":600,"office":400},
    {"id":27,"name":"مستغانم","home":600,"office":400},
    {"id":28,"name":"المسيلة","home":650,"office":400},
    {"id":29,"name":"معسكر","home":650,"office":400},
    {"id":30,"name":"ورقلة","home":800,"office":500},
    {"id":31,"name":"وهران","home":550,"office":300},
    {"id":32,"name":"البيض","home":850,"office":550},
    {"id":33,"name":"إيليزي","home":1300,"office":900},
    {"id":34,"name":"برج بوعريريج","home":600,"office":400},
    {"id":35,"name":"بومرداس","home":500,"office":350},
    {"id":36,"name":"الطارف","home":700,"office":400},
    {"id":37,"name":"تندوف","home":1300,"office":900},
    {"id":38,"name":"تيسمسيلت","home":700,"office":400},
    {"id":39,"name":"الوادي","home":800,"office":500},
    {"id":40,"name":"خنشلة","home":700,"office":450},
    {"id":41,"name":"سوق أهراس","home":800,"office":450},
    {"id":42,"name":"تيبازة","home":500,"office":350},
    {"id":43,"name":"ميلة","home":650,"office":400},
    {"id":44,"name":"عين الدفلى","home":600,"office":400},
    {"id":45,"name":"النعامة","home":900,"office":600},
    {"id":46,"name":"عين تموشنت","home":600,"office":450},
    {"id":47,"name":"غرداية","home":850,"office":500},
    {"id":48,"name":"غليزان","home":600,"office":450},
    {"id":49,"name":"تيميمون","home":1200,"office":700},
    {"id":50,"name":"برج باجي مختار","home":1300,"office":900},
    {"id":51,"name":"أولاد جلال","home":750,"office":500},
    {"id":52,"name":"بني عباس","home":1100,"office":700},
    {"id":53,"name":"عين صالح","home":1200,"office":700},
    {"id":54,"name":"عين قزام","home":1300,"office":900},
    {"id":55,"name":"توقرت","home":850,"office":500},
    {"id":56,"name":"جانت","home":1300,"office":900},
    {"id":57,"name":"المغير","home":800,"office":500},
    {"id":58,"name":"المنيعة","home":950,"office":600}
  ];
  await redis.set(K.shipping(), JSON.stringify(defaultShipping));
  return defaultShipping;
}

// ─── تيليجرام ───────────────────────────────────────────────
async function sendTelegram(botToken, chatId, text) {
  if (!chatId || !botToken) return;
  try {
    await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML' })
    });
  } catch {}
}

async function notifyAdmin(text) {
  if (ADMIN_CHAT_ID) await sendTelegram(ADMIN_BOT_TOKEN, ADMIN_CHAT_ID, text);
}

async function notifyUser(userId, text) {
  const user = parseUser(await getUser(userId));
  if (user?.telegram_id) await sendTelegram(USER_BOT_TOKEN, user.telegram_id, text);
}

// ─── Backblaze B2 ─────────────────────────────────────────────
async function uploadToB2(buffer, originalName) {
  // ضغط الصورة باستخدام sharp
  let imageBuffer = buffer;
  try {
    const img = sharp(buffer);
    const meta = await img.metadata();
    if (meta.width > 1200) {
      imageBuffer = await img
        .resize(1200, null, { withoutEnlargement: true })
        .jpeg({ quality: 82 })
        .toBuffer();
    } else {
      imageBuffer = await img.jpeg({ quality: 82 }).toBuffer();
    }
  } catch {}

  const fileName = `img_${Date.now()}_${Math.random().toString(36).slice(2)}.jpg`;
  const uploadUrl = `https://${B2_ENDPOINT}/${B2_BUCKET}/${fileName}`;
  const authorization = 'Basic ' + Buffer.from(`${B2_KEY_ID}:${B2_APP_KEY}`).toString('base64');

  const res = await fetch(uploadUrl, {
    method: 'PUT',
    headers: {
      'Authorization': authorization,
      'Content-Type': 'image/jpeg',
      'Content-Length': String(imageBuffer.length),
      'x-amz-acl': 'public-read',
    },
    body: imageBuffer,
  });

  if (res.ok) return `${B2_PUBLIC_URL}/${fileName}`;
  return false;
}

async function deleteFromB2(imageUrl) {
  if (!imageUrl) return;
  const fileName = imageUrl.split('/').pop();
  const url = `https://${B2_ENDPOINT}/${B2_BUCKET}/${fileName}`;
  const authorization = 'Basic ' + Buffer.from(`${B2_KEY_ID}:${B2_APP_KEY}`).toString('base64');
  try {
    await fetch(url, { method: 'DELETE', headers: { 'Authorization': authorization } });
  } catch {}
}

// ─── Admin session (بسيطة — cookie مُشفّر) ─────────────────────
// في Vercel لا يوجد session server-side، نستخدم JWT بسيط
function createAdminToken() {
  const payload = Buffer.from(JSON.stringify({ admin: true, ts: Date.now() })).toString('base64');
  const sig = crypto.createHmac('sha256', ADMIN_PASSWORD).update(payload).digest('hex');
  return `${payload}.${sig}`;
}

function verifyAdminToken(token) {
  if (!token) return false;
  const [payload, sig] = token.split('.');
  if (!payload || !sig) return false;
  const expected = crypto.createHmac('sha256', ADMIN_PASSWORD).update(payload).digest('hex');
  if (sig !== expected) return false;
  try {
    const data = JSON.parse(Buffer.from(payload, 'base64').toString());
    // صلاحية 24 ساعة
    if (Date.now() - data.ts > 86400000) return false;
    return data.admin === true;
  } catch { return false; }
}

// ─── Request Parser ───────────────────────────────────────────
async function parseRequest(req) {
  const params = {};

  // Query string
  const url = new URL(req.url, 'https://x');
  for (const [k, v] of url.searchParams) params[k] = v;

  const ct = req.headers.get('content-type') || '';

  if (req.method === 'POST') {
    if (ct.includes('application/json')) {
      try {
        const body = await req.json();
        Object.assign(params, body);
      } catch {}
    } else if (ct.includes('multipart/form-data')) {
      try {
        const form = await req.formData();
        for (const [k, v] of form.entries()) {
          if (typeof v === 'string') params[k] = v;
          else params[`__file_${k}`] = v; // File
        }
      } catch {}
    } else if (ct.includes('application/x-www-form-urlencoded')) {
      try {
        const text = await req.text();
        const sp = new URLSearchParams(text);
        for (const [k, v] of sp) params[k] = v;
      } catch {}
    }
  }

  return params;
}

// ─── CORS Headers ─────────────────────────────────────────────
function corsHeaders() {
  return {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, X-Auth-Token, X-Admin-Token',
  };
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: corsHeaders(),
  });
}

// ─── الدالة الرئيسية ──────────────────────────────────────────
export default async function handler(req) {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders() });
  }

  let params;
  try {
    params = await parseRequest(req);
  } catch (e) {
    return jsonResponse(fail('خطأ في تحليل الطلب'));
  }

  const action = sanitize(params.action || '');

  // ─── التحقق من المصادقة ──────────────────────────────────────
  const authToken = req.headers.get('x-auth-token') || params.auth_token || '';
  const adminToken = req.headers.get('x-admin-token') || params.admin_token || '';

  async function getCurrentUserId() {
    if (authToken) {
      const uid = await validateToken(authToken);
      if (uid) return uid;
    }
    return null;
  }

  async function isLoggedIn() {
    return !!(await getCurrentUserId());
  }

  function isAdmin() {
    return verifyAdminToken(adminToken);
  }

  // ─── التوجيه ──────────────────────────────────────────────────
  try {
    switch (action) {

      // ══════════════════════════════════════════════
      //  PING
      // ══════════════════════════════════════════════
      case 'ping':
        return jsonResponse(ok({ pong: true, time: Date.now() }));

      // ══════════════════════════════════════════════
      //  التسجيل
      // ══════════════════════════════════════════════
      case 'register': {
        const name     = sanitize(params.name || '');
        const phone    = sanitize(params.phone || '');
        const password = params.password || '';

        if (!name || !phone || !password)
          return jsonResponse(fail('يرجى ملء جميع الحقول'));
        if (!validatePhone(phone))
          return jsonResponse(fail('رقم الهاتف غير صحيح (يجب أن يبدأ بـ 05/06/07)'));
        if (password.length < 6)
          return jsonResponse(fail('كلمة المرور يجب أن تكون 6 أحرف على الأقل'));

        const allUsers = await getAllUsers();
        for (const u of Object.values(allUsers)) {
          if (u.phone === phone) return jsonResponse(fail('رقم الهاتف مسجل مسبقاً'));
        }

        const uid = generateID('user');
        const user = {
          id: uid, name, phone,
          password: hashPassword(password),
          wallet_balance: 0, total_earned: 0, total_withdrawn: 0,
          level: 'none', payment_methods: [],
          telegram_id: null, telegram_code: null,
          created_at: new Date().toISOString()
        };
        await saveUser(uid, user);

        const token = generateToken();
        await saveToken(uid, token);

        const safe = { ...user };
        delete safe.password; delete safe.telegram_code;
        safe.payment_methods = [];
        return jsonResponse(ok({ user: safe, token }, 'تم إنشاء الحساب بنجاح'));
      }

      // ══════════════════════════════════════════════
      //  تسجيل الدخول
      // ══════════════════════════════════════════════
      case 'login': {
        const phone    = sanitize(params.phone || '');
        const password = params.password || '';
        if (!phone || !password)
          return jsonResponse(fail('أدخل رقم الهاتف وكلمة المرور'));

        const allUsers = await getAllUsers();
        for (const [uid, u] of Object.entries(allUsers)) {
          if (u.phone === phone && verifyPassword(password, u.password)) {
            const token = generateToken();
            await saveToken(uid, token);
            const safe = { ...u };
            delete safe.password; delete safe.telegram_code; delete safe.reset_code;
            return jsonResponse(ok({ user: safe, token }, 'تم تسجيل الدخول'));
          }
        }
        return jsonResponse(fail('رقم الهاتف أو كلمة المرور غير صحيحة'));
      }

      // ══════════════════════════════════════════════
      //  تسجيل الخروج
      // ══════════════════════════════════════════════
      case 'logout': {
        if (authToken) await deleteToken(authToken);
        return jsonResponse(ok({}, 'تم تسجيل الخروج'));
      }

      // ══════════════════════════════════════════════
      //  الدخول بالتوكن
      // ══════════════════════════════════════════════
      case 'loginWithToken': {
        const t = params.auth_token || authToken;
        if (!t) return jsonResponse(fail('لا يوجد توكن'));
        const uid = await validateToken(t);
        if (!uid) return jsonResponse(fail('توكن منتهي أو غير صالح'));
        const raw = await getUser(uid);
        if (!raw) return jsonResponse(fail('المستخدم غير موجود'));
        const u = parseUser(raw);
        const safe = { ...u };
        delete safe.password; delete safe.telegram_code; delete safe.reset_code;
        return jsonResponse(ok({ user: safe }, 'تم الدخول تلقائياً'));
      }

      // ══════════════════════════════════════════════
      //  بيانات المستخدم
      // ══════════════════════════════════════════════
      case 'getUserData': {
        const uid = await getCurrentUserId();
        if (!uid) return jsonResponse(fail('غير مسجل'));
        const raw = await getUser(uid);
        if (!raw) return jsonResponse(fail('المستخدم غير موجود'));
        const u = parseUser(raw);
        const safe = { ...u };
        delete safe.password; delete safe.telegram_code; delete safe.reset_code;
        return jsonResponse(ok({ user: safe }));
      }

      // ══════════════════════════════════════════════
      //  تحديث الملف الشخصي
      // ══════════════════════════════════════════════
      case 'updateUser': {
        const uid = await getCurrentUserId();
        if (!uid) return jsonResponse(fail('يجب تسجيل الدخول'));
        const name = sanitize(params.name || '');
        if (name.length < 2) return jsonResponse(fail('الاسم مطلوب'));
        const raw = await getUser(uid);
        if (!raw) return jsonResponse(fail('المستخدم غير موجود'));
        const u = parseUser(raw);
        u.name = name;
        await saveUser(uid, u);
        return jsonResponse(ok({}, 'تم التحديث'));
      }

      // ══════════════════════════════════════════════
      //  تغيير كلمة المرور
      // ══════════════════════════════════════════════
      case 'changePassword': {
        const uid     = await getCurrentUserId();
        if (!uid) return jsonResponse(fail('يجب تسجيل الدخول'));
        const current = params.current_password || '';
        const nw      = params.new_password || '';
        if (nw.length < 6) return jsonResponse(fail('كلمة المرور الجديدة يجب 6 أحرف على الأقل'));
        const raw = await getUser(uid);
        if (!raw) return jsonResponse(fail('المستخدم غير موجود'));
        const u = parseUser(raw);
        if (!verifyPassword(current, u.password)) return jsonResponse(fail('كلمة المرور الحالية خاطئة'));
        u.password = hashPassword(nw);
        await saveUser(uid, u);
        return jsonResponse(ok({}, 'تم تغيير كلمة المرور'));
      }

      // ══════════════════════════════════════════════
      //  استعادة كلمة المرور
      // ══════════════════════════════════════════════
      case 'requestPasswordReset': {
        const phone = sanitize(params.phone || '');
        if (!validatePhone(phone)) return jsonResponse(fail('رقم الهاتف غير صحيح'));
        const allUsers = await getAllUsers();
        let foundUid = null;
        for (const [uid, u] of Object.entries(allUsers)) {
          if (u.phone === phone) { foundUid = uid; break; }
        }
        if (!foundUid) return jsonResponse(fail('رقم الهاتف غير مسجل'));
        const code = String(Math.floor(Math.random() * 1000000)).padStart(6, '0');
        // نحفظ الكود في Redis مع TTL 10 دقائق
        await redis.set(`zn:reset:${foundUid}`, code, { ex: 600 });
        const u = parseUser(await getUser(foundUid));
        if (u?.telegram_id) {
          await sendTelegram(USER_BOT_TOKEN, u.telegram_id,
            `🔐 رمز إعادة تعيين كلمة المرور: ${code}\nصالح لمدة 10 دقائق.`);
        }
        return jsonResponse(ok({ code_preview: code }, 'تم إرسال الرمز'));
      }

      case 'verifyResetCode': {
        const code  = sanitize(params.code || '');
        const phone = sanitize(params.phone || '');
        // نبحث عن المستخدم بالهاتف
        const allUsers = await getAllUsers();
        let foundUid = null;
        for (const [uid, u] of Object.entries(allUsers)) {
          if (u.phone === phone) { foundUid = uid; break; }
        }
        if (!foundUid) return jsonResponse(fail('المستخدم غير موجود'));
        const saved = await redis.get(`zn:reset:${foundUid}`);
        if (!saved || String(saved) !== code) return jsonResponse(fail('الرمز غير صحيح'));
        // نحفظ حالة التحقق مؤقتاً
        await redis.set(`zn:reset_verified:${foundUid}`, '1', { ex: 600 });
        return jsonResponse(ok({ uid: foundUid }, 'تم التحقق'));
      }

      case 'resetPassword': {
        const newPass = params.new_password || '';
        const uid     = sanitize(params.uid || '');
        if (!uid || newPass.length < 6) return jsonResponse(fail('بيانات غير صحيحة'));
        const verified = await redis.get(`zn:reset_verified:${uid}`);
        if (!verified) return jsonResponse(fail('غير مصرح، أعد التحقق'));
        const raw = await getUser(uid);
        if (!raw) return jsonResponse(fail('المستخدم غير موجود'));
        const u = parseUser(raw);
        u.password = hashPassword(newPass);
        await saveUser(uid, u);
        await redis.del(`zn:reset:${uid}`, `zn:reset_verified:${uid}`);
        return jsonResponse(ok({}, 'تم تعيين كلمة المرور الجديدة'));
      }

      case 'getTelegramLinkCode': {
        const uid = await getCurrentUserId();
        if (!uid) return jsonResponse(fail('يجب تسجيل الدخول'));
        const code = crypto.createHash('md5').update(uid + Date.now()).digest('hex').slice(0, 6).toUpperCase();
        const raw = await getUser(uid);
        const u = parseUser(raw);
        u.telegram_code = code;
        await saveUser(uid, u);
        return jsonResponse(ok({ code }));
      }

      // ══════════════════════════════════════════════
      //  المنتجات
      // ══════════════════════════════════════════════
      case 'getProducts': {
        const products = await getProducts();
        const category = sanitize(params.category || '');
        if (category && category !== 'all') {
          return jsonResponse(ok({ products: products.filter(p => p.category === category) }));
        }
        return jsonResponse(ok({ products }));
      }

      case 'getProduct': {
        const productId = sanitize(params.product_id || '');
        const products  = await getProducts();
        const p = products.find(x => x.id === productId);
        if (!p) return jsonResponse(fail('المنتج غير موجود'));
        return jsonResponse(ok({ product: p }));
      }

      case 'searchProducts': {
        const q = sanitize(params.q || '');
        if (q.length < 1) return jsonResponse(ok({ products: [] }));
        const products = await getProducts();
        const results = products.filter(p =>
          p.title?.includes(q) ||
          p.description?.includes(q) ||
          p.category?.includes(q)
        );
        return jsonResponse(ok({ products: results }));
      }

      case 'getCategories':
        return jsonResponse(ok({ categories: CATEGORIES }));

      case 'addProduct': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const title       = sanitize(params.title || '');
        const price       = parseFloat(params.price || 0);
        const stock       = parseInt(params.stock || 0);
        const category    = sanitize(params.category || '');
        const description = sanitize(params.description || '');
        const outOfStock  = params.out_of_stock === '1';
        let images = [];
        try { images = JSON.parse(params.images || '[]'); } catch {}

        if (!title || price <= 0 || !category || !description)
          return jsonResponse(fail('يرجى ملء جميع الحقول المطلوبة'));

        const products = await getProducts();
        const newProduct = {
          id: generateID('prod'), category, title, price, stock,
          description, images, out_of_stock: outOfStock,
          created_at: new Date().toISOString(), ratings: []
        };
        products.push(newProduct);
        await saveProducts(products);
        return jsonResponse(ok({ product: newProduct }, 'تمت إضافة المنتج'));
      }

      case 'updateProduct': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const productId   = sanitize(params.product_id || '');
        const title       = sanitize(params.title || '');
        const price       = parseFloat(params.price || 0);
        const stock       = parseInt(params.stock || 0);
        const category    = sanitize(params.category || '');
        const description = sanitize(params.description || '');
        const outOfStock  = params.out_of_stock === '1';

        const products = await getProducts();
        const idx = products.findIndex(p => p.id === productId);
        if (idx === -1) return jsonResponse(fail('المنتج غير موجود'));

        products[idx] = { ...products[idx], title, price, stock, category, description,
          out_of_stock: outOfStock, updated_at: new Date().toISOString() };

        if (params.images !== undefined) {
          try { products[idx].images = JSON.parse(params.images); } catch {}
        }
        await saveProducts(products);
        return jsonResponse(ok({}, 'تم تحديث المنتج'));
      }

      case 'updateProductStock': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const productId = sanitize(params.product_id || '');
        const outOfStock = params.out_of_stock === '1';
        const products = await getProducts();
        const idx = products.findIndex(p => p.id === productId);
        if (idx !== -1) { products[idx].out_of_stock = outOfStock; await saveProducts(products); }
        return jsonResponse(ok({}, 'تم التحديث'));
      }

      case 'deleteProduct': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const productId = sanitize(params.product_id || '');
        const products = await getProducts();
        await saveProducts(products.filter(p => p.id !== productId));
        return jsonResponse(ok({}, 'تم الحذف'));
      }

      // ══════════════════════════════════════════════
      //  رفع الصور
      // ══════════════════════════════════════════════
      case 'uploadImage': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const file = params['__file_image'];
        if (!file) return jsonResponse(fail('لم يتم رفع صورة'));

        const buffer = Buffer.from(await file.arrayBuffer());
        if (buffer.length > 8 * 1024 * 1024)
          return jsonResponse(fail('حجم الصورة يتجاوز 8MB'));

        // محاولة Backblaze B2 أولاً
        if (B2_KEY_ID && B2_APP_KEY) {
          const url = await uploadToB2(buffer, file.name);
          if (url) return jsonResponse(ok({ url }, 'تم رفع الصورة'));
        }

        // احتياطي: Vercel Blob
        try {
          const blob = await put(file.name || `img_${Date.now()}.jpg`, buffer, {
            access: 'public',
            contentType: 'image/jpeg',
          });
          return jsonResponse(ok({ url: blob.url }, 'تم رفع الصورة'));
        } catch (e) {
          return jsonResponse(fail('فشل رفع الصورة: ' + e.message));
        }
      }

      case 'deleteB2Image': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const url = params.url || '';
        await deleteFromB2(url);
        return jsonResponse(ok({}, 'تم حذف الصورة'));
      }

      case 'downloadProxy': {
        const url = params.url || '';
        if (!url) return jsonResponse(fail('رابط غير صالح'));
        const allowedHosts = ['ik.imagekit.io','imagekit.io','f005.backblazeb2.com','backblazeb2.com'];
        const host = new URL(url).hostname;
        if (!allowedHosts.some(h => host === h || host.endsWith('.' + h)))
          return jsonResponse(fail('مصدر غير مسموح'));
        const res = await fetch(url);
        if (!res.ok) return jsonResponse(fail('فشل تحميل الصورة'));
        const buffer = await res.arrayBuffer();
        const ct = res.headers.get('content-type') || 'image/jpeg';
        return new Response(buffer, {
          headers: {
            'Content-Type': ct,
            'Content-Disposition': `attachment; filename="znstore_${Date.now()}.jpg"`,
            'Access-Control-Allow-Origin': '*',
          }
        });
      }

      // ══════════════════════════════════════════════
      //  الطلبات
      // ══════════════════════════════════════════════
      case 'createOrder': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const customerName  = sanitize(params.customer_name || '');
        const customerPhone = sanitize(params.customer_phone || '');
        const wilaya        = sanitize(params.wilaya || '');
        const deliveryType  = sanitize(params.delivery_type || '');
        const address       = sanitize(params.address || '');
        const notes         = sanitize(params.notes || '');
        const shippingCost  = parseFloat(params.shipping_cost || 0);
        const subtotal      = parseFloat(params.subtotal || 0);
        const total         = parseFloat(params.total || 0);

        if (!customerName || !customerPhone || !wilaya || !deliveryType)
          return jsonResponse(fail('يرجى ملء جميع الحقول المطلوبة'));
        if (!validatePhone(customerPhone))
          return jsonResponse(fail('رقم الهاتف غير صحيح'));
        if (deliveryType === 'home' && !address)
          return jsonResponse(fail('العنوان مطلوب للتوصيل للمنزل'));

        let items = [];
        try { items = JSON.parse(params.items || '[]'); } catch {}
        if (!items.length) return jsonResponse(fail('السلة فارغة'));

        const uid = await getCurrentUserId();
        const orderId = generateID('order');
        const order = {
          id: orderId, user_id: uid, customer_name: customerName,
          customer_phone: customerPhone, wilaya, delivery_type: deliveryType,
          address, payment_method: 'cod', notes, items,
          subtotal, shipping_cost: shippingCost, total,
          status: 'pending',
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        };
        await saveOrder(orderId, order);
        await addNotification(uid, '📦 تم استلام طلبك',
          `طلبك رقم #${orderId.slice(-8).toUpperCase()} قيد المراجعة. سنتصل بك قريباً.`, 'order');

        const itemsText = items.map(i => `${i.title} ×${i.quantity}`).join(', ');
        notifyAdmin(`🛒 طلب جديد!\nالعميل: ${customerName}\nهاتف: ${customerPhone}\nالولاية: ${wilaya}\nالمنتجات: ${itemsText}\nالمجموع: ${total} دج`);

        return jsonResponse(ok({ order_id: orderId }, 'تم إرسال الطلب بنجاح'));
      }

      case 'getMyOrders': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const uid    = await getCurrentUserId();
        const orders = await getAllOrders();
        const mine   = Object.values(orders)
          .filter(o => o.user_id === uid)
          .sort((a, b) => b.created_at?.localeCompare(a.created_at));
        return jsonResponse(ok({ orders: mine }));
      }

      case 'cancelOrder': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const orderId = sanitize(params.order_id || '');
        const uid     = await getCurrentUserId();
        const order   = await getOrder(orderId);
        if (!order) return jsonResponse(fail('الطلب غير موجود'));
        if (order.user_id !== uid) return jsonResponse(fail('غير مصرح'));
        if (order.status !== 'pending') return jsonResponse(fail('لا يمكن إلغاء الطلب في هذه المرحلة'));
        order.status     = 'cancelled';
        order.updated_at = new Date().toISOString();
        await saveOrder(orderId, order);
        return jsonResponse(ok({}, 'تم إلغاء الطلب'));
      }

      case 'editOrder': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const orderId = sanitize(params.order_id || '');
        const uid     = await getCurrentUserId();
        const order   = await getOrder(orderId);
        if (!order) return jsonResponse(fail('الطلب غير موجود'));
        if (order.user_id !== uid) return jsonResponse(fail('غير مصرح'));
        if (order.status !== 'pending') return jsonResponse(fail('لا يمكن تعديل الطلب الآن'));
        const name  = sanitize(params.customer_name || '');
        const phone = sanitize(params.customer_phone || '');
        const notes = sanitize(params.notes || '');
        if (name) order.customer_name = name;
        if (phone && validatePhone(phone)) order.customer_phone = phone;
        order.notes      = notes;
        order.updated_at = new Date().toISOString();
        await saveOrder(orderId, order);
        return jsonResponse(ok({}, 'تم التعديل'));
      }

      case 'getAllOrders': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const orders = await getAllOrders();
        const list = Object.values(orders).sort((a, b) => b.created_at?.localeCompare(a.created_at));
        return jsonResponse(ok({ orders: list }));
      }

      case 'updateOrderStatus': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const orderId         = sanitize(params.order_id || '');
        const status          = sanitize(params.status || '');
        const rejectionReason = sanitize(params.rejection_reason || '');
        const validStatuses   = ['pending','confirmed','shipping','delivered','rejected','returned','cancelled'];
        if (!validStatuses.includes(status)) return jsonResponse(fail('حالة غير صحيحة'));

        const order = await getOrder(orderId);
        if (!order) return jsonResponse(fail('الطلب غير موجود'));
        order.status     = status;
        order.updated_at = new Date().toISOString();
        if (status === 'rejected' && rejectionReason)
          order.rejection_reason = rejectionReason;

        const uid = order.user_id;

        if (status === 'delivered') {
          const raw = await getUser(uid);
          if (raw) {
            const u = parseUser(raw);
            const levelInfo    = getUserLevel(u.total_earned || 0);
            const rewardPerUnit = levelInfo.reward;
            const totalQty     = (order.items || []).reduce((s, i) => s + (i.quantity || 1), 0) || 1;
            const rewardAmount = rewardPerUnit * totalQty;
            u.wallet_balance = (u.wallet_balance || 0) + rewardAmount;
            u.total_earned   = (u.total_earned   || 0) + rewardAmount;
            u.orders_count   = (u.orders_count   || 0) + 1;
            // تحديث المستوى
            const newLevel = getUserLevel(u.total_earned).level;
            if (newLevel !== u.level && newLevel !== 'none') {
              u.level = newLevel;
              const li = getUserLevel(u.total_earned);
              await addNotification(uid,
                `${li.icon} تم توثيق حسابك! مرحباً بك في المستوى ${li.name}`,
                `تهانينا! وصلت للمستوى ${li.name}. ستحصل الآن على ${li.reward} دج لكل قطعة مُسلَّمة.`,
                'wallet');
            }
            await saveUser(uid, u);
            await addNotification(uid, '🎉 تم تسليم طلبك!',
              `تمت إضافة ${rewardAmount} دج لمحفظتك (${rewardPerUnit} دج × ${totalQty} قطعة).`, 'wallet');
            notifyUser(uid, `🎉 تم تسليم طلبك!\n💰 أُضيف ${rewardAmount} دج لمحفظتك.\n(${totalQty} قطعة × ${rewardPerUnit} دج)`);
          }
        } else if (status === 'confirmed') {
          await addNotification(uid, '📞 تم تأكيد طلبك', 'طلبك قيد التجهيز للشحن.', 'order');
          notifyUser(uid, '📞 تم تأكيد طلبك!\nطلبك قيد التجهيز للشحن.');
        } else if (status === 'shipping') {
          await addNotification(uid, '🚚 طلبك في الطريق', 'طلبك خرج للتوصيل.', 'order');
          notifyUser(uid, '🚚 طلبك في الطريق إليك!');
        } else if (status === 'rejected') {
          const reason = rejectionReason ? `\nالسبب: ${rejectionReason}` : '';
          await addNotification(uid, '❌ تم رفض طلبك', `نأسف، تم رفض طلبك.${reason}`, 'order');
          notifyUser(uid, `❌ تم رفض طلبك.${reason}`);
        } else if (status === 'returned') {
          await addNotification(uid, '↩️ تم إرجاع طلبك', 'تم إرجاع الطلب.', 'order');
        }

        await saveOrder(orderId, order);
        return jsonResponse(ok({}, 'تم تحديث الحالة'));
      }

      // ══════════════════════════════════════════════
      //  التقييمات
      // ══════════════════════════════════════════════
      case 'addRating': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const productId = sanitize(params.product_id || '');
        const rating    = parseInt(params.rating || 0);
        const comment   = sanitize(params.comment || '');
        const uid       = await getCurrentUserId();

        if (rating < 1 || rating > 5) return jsonResponse(fail('التقييم بين 1 و 5'));

        const orders = await getAllOrders();
        let eligible = false;
        for (const o of Object.values(orders)) {
          if (o.user_id === uid && o.status === 'delivered') {
            if ((o.items || []).some(i => i.id === productId)) { eligible = true; break; }
          }
        }
        if (!eligible) return jsonResponse(fail('يمكنك التقييم فقط بعد استلام الطلب'));

        const products = await getProducts();
        const idx = products.findIndex(p => p.id === productId);
        if (idx !== -1) {
          products[idx].ratings = (products[idx].ratings || []).filter(r => r.user_id !== uid);
          products[idx].ratings.push({ user_id: uid, rating, comment, created_at: new Date().toISOString() });
          await saveProducts(products);
        }
        return jsonResponse(ok({}, 'شكراً على تقييمك'));
      }

      // ══════════════════════════════════════════════
      //  المحفظة
      // ══════════════════════════════════════════════
      case 'getWalletBalance': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const uid    = await getCurrentUserId();
        const raw    = await getUser(uid);
        if (!raw) return jsonResponse(fail('المستخدم غير موجود'));
        const u      = parseUser(raw);
        const orders = await getAllOrders();
        const ordersCount = Object.values(orders).filter(o => o.user_id === uid).length;
        const levelInfo   = getUserLevel(u.total_earned || 0);
        return jsonResponse(ok({
          balance: u.wallet_balance || 0,
          total_earned: u.total_earned || 0,
          total_withdrawn: u.total_withdrawn || 0,
          orders_count: ordersCount,
          payment_methods: u.payment_methods || [],
          level: levelInfo,
          user_level: u.level || 'none',
        }));
      }

      case 'addPaymentMethod': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const uid     = await getCurrentUserId();
        const type    = sanitize(params.type || '');
        const details = sanitize(params.details || '');
        if (!['ccp','flexi'].includes(type)) return jsonResponse(fail('النوع غير صحيح'));
        if (!details) return jsonResponse(fail('التفاصيل مطلوبة'));
        const raw = await getUser(uid);
        const u   = parseUser(raw);
        if ((u.payment_methods || []).length >= 5) return jsonResponse(fail('الحد الأقصى 5 طرق دفع'));
        u.payment_methods = [...(u.payment_methods || []), {
          id: generateID('pm'), type, details, created_at: new Date().toISOString()
        }];
        await saveUser(uid, u);
        return jsonResponse(ok({}, 'تمت الإضافة'));
      }

      case 'deletePaymentMethod': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const uid      = await getCurrentUserId();
        const methodId = sanitize(params.method_id || '');
        const raw      = await getUser(uid);
        const u        = parseUser(raw);
        u.payment_methods = (u.payment_methods || []).filter(m => m.id !== methodId);
        await saveUser(uid, u);
        return jsonResponse(ok({}, 'تم الحذف'));
      }

      case 'requestWithdrawal': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const amount   = parseFloat(params.amount || 0);
        const methodId = sanitize(params.method_id || '');
        if (amount < MIN_WITHDRAWAL) return jsonResponse(fail(`الحد الأدنى للسحب هو ${MIN_WITHDRAWAL} دج`));

        const uid = await getCurrentUserId();
        const raw = await getUser(uid);
        if (!raw) return jsonResponse(fail('المستخدم غير موجود'));
        const u = parseUser(raw);

        if (amount > (u.wallet_balance || 0)) return jsonResponse(fail('الرصيد غير كافٍ'));
        const levelInfo = getUserLevel(u.total_earned || 0);
        if (levelInfo.level === 'none') return jsonResponse(fail('يجب أن تصل للمستوى البرونزي (4000 دج) أولاً لطلب السحب'));

        const method = (u.payment_methods || []).find(m => m.id === methodId);
        if (!method) return jsonResponse(fail('طريقة الدفع غير موجودة'));

        u.wallet_balance  = (u.wallet_balance  || 0) - amount;
        u.total_withdrawn = (u.total_withdrawn || 0) + amount;
        await saveUser(uid, u);

        const wId = generateID('withdraw');
        await saveWithdrawal(wId, {
          id: wId, user_id: uid, user_name: u.name, user_phone: u.phone,
          amount, method_type: method.type, method_details: method.details,
          status: 'pending', created_at: new Date().toISOString()
        });

        await addNotification(uid, '💸 طلب سحب جديد', `طلب سحب ${amount} دج قيد المعالجة.`, 'wallet');
        notifyAdmin(`💸 طلب سحب جديد!\nالمستخدم: ${u.name} (${u.phone})\nالمبلغ: ${amount} دج\nالطريقة: ${method.type} - ${method.details}`);

        return jsonResponse(ok({}, 'تم إرسال طلب السحب'));
      }

      case 'getWithdrawals': {
        if (isAdmin()) {
          const all = await getAllWithdrawals();
          const list = Object.values(all).sort((a, b) => b.created_at?.localeCompare(a.created_at));
          return jsonResponse(ok({ withdrawals: list }));
        }
        if (await isLoggedIn()) {
          const uid = await getCurrentUserId();
          const all = await getAllWithdrawals();
          const mine = Object.values(all)
            .filter(w => w.user_id === uid)
            .sort((a, b) => b.created_at?.localeCompare(a.created_at));
          return jsonResponse(ok({ withdrawals: mine }));
        }
        return jsonResponse(fail('يجب تسجيل الدخول'));
      }

      case 'updateWithdrawalStatus': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const wId   = sanitize(params.withdrawal_id || '');
        const status = sanitize(params.status || '');
        if (!['paid','rejected'].includes(status)) return jsonResponse(fail('حالة غير صحيحة'));

        const w = await getWithdrawal(wId);
        if (!w) return jsonResponse(fail('طلب السحب غير موجود'));
        if (w.status !== 'pending') return jsonResponse(fail('الطلب مُعالَج مسبقاً'));

        w.status     = status;
        w.updated_at = new Date().toISOString();

        if (status === 'rejected') {
          const raw = await getUser(w.user_id);
          if (raw) {
            const u = parseUser(raw);
            u.wallet_balance  = (u.wallet_balance  || 0) + w.amount;
            u.total_withdrawn = Math.max(0, (u.total_withdrawn || 0) - w.amount);
            await saveUser(w.user_id, u);
          }
          await addNotification(w.user_id, '❌ تم رفض طلب السحب',
            `تم رفض طلب سحب ${w.amount} دج. أُعيد الرصيد لمحفظتك.`, 'wallet');
          notifyUser(w.user_id, `❌ تم رفض طلب السحب.\n💵 ${w.amount} دج أُعيدت لمحفظتك.`);
        } else {
          await addNotification(w.user_id, '✅ تم صرف طلب السحب', `تم تحويل ${w.amount} دج.`, 'wallet');
          notifyUser(w.user_id, `✅ تم صرف طلب السحب!\n💵 ${w.amount} دج`);
        }

        await saveWithdrawal(wId, w);
        return jsonResponse(ok({}, 'تم التحديث'));
      }

      // ══════════════════════════════════════════════
      //  الإشعارات
      // ══════════════════════════════════════════════
      case 'getUserNotifications': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const uid    = await getCurrentUserId();
        const raw    = await redis.get(K.notifications(uid));
        let notifs   = [];
        try { notifs = raw ? JSON.parse(raw) : []; } catch {}
        return jsonResponse(ok({ notifications: notifs }));
      }

      case 'getUnreadNotificationsCount': {
        if (!(await isLoggedIn())) return jsonResponse(ok({ count: 0 }));
        const uid  = await getCurrentUserId();
        const raw  = await redis.get(K.notifications(uid));
        let notifs = [];
        try { notifs = raw ? JSON.parse(raw) : []; } catch {}
        const count = notifs.filter(n => !n.read).length;
        return jsonResponse(ok({ count }));
      }

      case 'markNotificationAsRead': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const uid     = await getCurrentUserId();
        const notifId = sanitize(params.notification_id || '');
        const all     = params.all === '1';
        const raw     = await redis.get(K.notifications(uid));
        let notifs    = [];
        try { notifs = raw ? JSON.parse(raw) : []; } catch {}
        notifs = notifs.map(n => ({ ...n, read: all || n.id === notifId ? true : n.read }));
        await redis.set(K.notifications(uid), JSON.stringify(notifs));
        return jsonResponse(ok({}));
      }

      // ══════════════════════════════════════════════
      //  الإحصائيات
      // ══════════════════════════════════════════════
      case 'getUserStats': {
        if (!(await isLoggedIn())) return jsonResponse(fail('يجب تسجيل الدخول'));
        const uid      = await getCurrentUserId();
        const orders   = await getAllOrders();
        const myOrders = Object.values(orders).filter(o => o.user_id === uid);
        const raw      = await getUser(uid);
        const u        = parseUser(raw) || {};
        const levelInfo = getUserLevel(u.total_earned || 0);
        return jsonResponse(ok({
          total_orders:     myOrders.length,
          delivered_orders: myOrders.filter(o => o.status === 'delivered').length,
          pending_orders:   myOrders.filter(o => o.status === 'pending').length,
          wallet_balance:   u.wallet_balance  || 0,
          total_earned:     u.total_earned    || 0,
          total_withdrawn:  u.total_withdrawn || 0,
          level: levelInfo,
        }));
      }

      case 'getAdminStats': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const [orders, users, products, withdrawals] = await Promise.all([
          getAllOrders(), getAllUsers(), getProducts(), getAllWithdrawals()
        ]);
        const delivered = Object.values(orders).filter(o => o.status === 'delivered');
        const revenue   = delivered.reduce((s, o) => s + (o.total || 0), 0);
        return jsonResponse(ok({
          total_orders:        Object.keys(orders).length,
          pending_orders:      Object.values(orders).filter(o => o.status === 'pending').length,
          confirmed_orders:    Object.values(orders).filter(o => o.status === 'confirmed').length,
          delivered_orders:    delivered.length,
          total_users:         Object.keys(users).length,
          total_products:      products.length,
          total_revenue:       revenue,
          pending_withdrawals: Object.values(withdrawals).filter(w => w.status === 'pending').length,
        }));
      }

      // ══════════════════════════════════════════════
      //  الشحن
      // ══════════════════════════════════════════════
      case 'getShippingPrices': {
        const shipping = await getShipping();
        return jsonResponse(ok({ shipping }));
      }

      case 'updateShipping': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        let data = [];
        try { data = JSON.parse(params.shipping || '[]'); } catch {}
        await redis.set(K.shipping(), JSON.stringify(data));
        return jsonResponse(ok({}, 'تم تحديث أسعار الشحن'));
      }

      // ══════════════════════════════════════════════
      //  الأدمن
      // ══════════════════════════════════════════════
      case 'adminLogin': {
        const password = params.password || '';
        if (password !== ADMIN_PASSWORD) return jsonResponse(fail('كلمة السر غير صحيحة'));
        const token = createAdminToken();
        return jsonResponse(ok({ admin_token: token }, 'مرحباً بك في لوحة الإدارة'));
      }

      case 'adminLogout':
        return jsonResponse(ok({}, 'تم تسجيل الخروج'));

      case 'checkAdminSession':
        return jsonResponse(ok({ logged_in: isAdmin() }));

      case 'getAllUsers': {
        if (!isAdmin()) return jsonResponse(fail('غير مصرح'));
        const allUsers = await getAllUsers();
        const orders   = await getAllOrders();
        const list = Object.entries(allUsers).map(([id, u]) => {
          const levelInfo = getUserLevel(u.total_earned || 0);
          const safe = { ...u };
          delete safe.password; delete safe.reset_code;
          delete safe.telegram_code; delete safe.reset_expires;
          safe.orders_count = Object.values(orders).filter(o => o.user_id === id).length;
          safe.level_info   = levelInfo;
          safe.level        = u.level || 'none';
          return safe;
        });
        list.sort((a, b) => b.created_at?.localeCompare(a.created_at));
        return jsonResponse(ok({ users: list }));
      }

      // ══════════════════════════════════════════════
      //  تهيئة البيانات (نقل من JSON إلى Redis)
      // ══════════════════════════════════════════════
      case 'initData': {
        // endpoint سري لنقل البيانات الأولية
        const secret = params.secret || '';
        if (secret !== (process.env.INIT_SECRET || 'zn_init_2024')) {
          return jsonResponse(fail('غير مصرح'));
        }
        // تهيئة الشحن
        await getShipping();
        return jsonResponse(ok({ message: 'تم التهيئة' }));
      }

      default:
        return jsonResponse(fail(`الإجراء غير معروف: ${action}`));
    }
  } catch (e) {
    console.error('[ZN API Error]', action, e);
    return jsonResponse(fail('خطأ في السيرفر: ' + (e.message || 'unknown')), 500);
  }
}

