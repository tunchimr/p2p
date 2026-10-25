import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getFirestore, doc, setDoc, getDoc, onSnapshot, runTransaction } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";

// ==================== FIREBASE INIT ====================
let db = null;
let unsubscribe = null;
let _fbDebounceTimer = null;
let _pushing = false, _pushAgain = false, _resolving = false, _pendingStamp = null;
const DOC_ID = 'main';
const COLLECTION = 'sudacash';
const LS_SYNCED = 'sc_last_synced';   // آخر نسخة سحابية عرفها هذا الجهاز
const LS_DIRTY  = 'sc_dirty';         // في تغييرات محلية لسه ما اترفعت؟

window._firebase = { db: null, setDoc, doc, getDoc };

function getSavedConfig() {
  try { return JSON.parse(localStorage.getItem('sc_firebase_cfg') || 'null'); } catch { return null; }
}
const getLastSynced = () => localStorage.getItem(LS_SYNCED) || null;
function setLastSynced(v) { localStorage.setItem(LS_SYNCED, v || ''); }
function isDirty() { return localStorage.getItem(LS_DIRTY) === '1'; }
function setDirty(v) { localStorage.setItem(LS_DIRTY, v ? '1' : '0'); }

// ترتيب المفاتيح ثابت عشان المقارنة بين المحلي والسحابي تكون صحيحة
function canon(x) {
  if (Array.isArray(x)) return x.map(canon);
  if (x && typeof x === 'object') {
    const o = {};
    Object.keys(x).sort().forEach(k => { if (x[k] !== undefined) o[k] = canon(x[k]); });
    return o;
  }
  return x;
}
function localPayload() {
  const d = window._data;
  return { trades: d.trades, inventory: d.inventory, settings: d.settings, accounts: d.accounts, cashMoves: d.cashMoves };
}
function remotePayload(r) {
  return { trades: r.trades || [], inventory: r.inventory, settings: r.settings, accounts: r.accounts, cashMoves: r.cashMoves || [] };
}
function sameData(r) {
  try { return JSON.stringify(canon(localPayload())) === JSON.stringify(canon(remotePayload(r))); } catch { return false; }
}
function hasLocalData() {
  const d = window._data;
  return !!((d.trades && d.trades.length) || (d.cashMoves && d.cashMoves.length) ||
            (d.inventory && d.inventory.amount > 0) ||
            (d.accounts || []).some(a => a.balance || a.initBalance));
}
function stampLabel(iso) { try { return new Date(iso).toLocaleString('en-GB'); } catch { return iso || '—'; } }

function applyCloud(remote) {
  applyRemoteData(remote);
  setLastSynced(remote.updatedAt);
  setDirty(false);
  notify('☁️ تم تحميل أحدث بيانات من السحابة');
}

// قرار واحد لكل حالة: ما نكتب فوق بيانات إلا لو متأكدين
function reconcile(remote) {
  if (_resolving) return;
  if (sameData(remote)) { setLastSynced(remote.updatedAt); setDirty(false); return; }          // متطابقتين
  if (isDirty() && remote.updatedAt === getLastSynced()) { doPush(); return; }                  // السحابة ما اتغيرت، وعندنا تغييرات محلية → ارفعها
  if (!isDirty() && (getLastSynced() || !hasLocalData())) { applyCloud(remote); return; }       // ما عندنا تغييرات → خذ السحابة
  resolveConflict(remote);                                                                      // الاتنين اتغيروا
}

function resolveConflict(remote) {
  _resolving = true;
  const msg = '⚠️ تعارض في البيانات\n\n' +
    '☁️ السحابة: ' + (remote.trades || []).length + ' صفقة (آخر تحديث ' + stampLabel(remote.updatedAt) + ')\n' +
    '📱 هذا الجهاز: ' + window._data.trades.length + ' صفقة (فيها تغييرات ما اتزامنت)\n\n' +
    'موافق = استخدم نسخة السحابة (وتنزل نسخة احتياطية من بيانات الجهاز)\n' +
    'إلغاء = استخدم بيانات هذا الجهاز وارفعها فوق السحابة (وتنزل نسخة احتياطية من السحابة)';
  const useCloud = confirm(msg);
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  try {
    if (useCloud) {
      window.downloadJSON({ ...localPayload(), exportDate: new Date().toISOString(), version: 4 }, 'sudacash-device-backup-' + stamp + '.json');
      applyCloud(remote);
    } else {
      window.downloadJSON({ ...remotePayload(remote), exportDate: new Date().toISOString(), version: 4 }, 'sudacash-cloud-backup-' + stamp + '.json');
      setLastSynced(remote.updatedAt);
      setDirty(true);
    }
  } finally { _resolving = false; }
  if (!useCloud) doPush();
}

async function initFirebase(cfg) {
  try {
    setSyncStatus('syncing', 'جاري الاتصال...');
    const app = initializeApp(cfg, 'sudacash-' + Date.now());
    db = getFirestore(app);
    window._firebase.db = db;

    showLoading(true);
    const ref = doc(db, COLLECTION, DOC_ID);
    const snap = await getDoc(ref);
    if (snap.exists()) reconcile(snap.data());
    else await doPush();                       // أول مرة: ارفع البيانات المحلية
    showLoading(false);

    // الاستماع للتغييرات من الأجهزة التانية
    if (unsubscribe) unsubscribe();
    unsubscribe = onSnapshot(ref, (s) => {
      if (!s.exists() || s.metadata.hasPendingWrites) return;
      const remote = s.data();
      if (remote.updatedAt === getLastSynced() || remote.updatedAt === _pendingStamp) return;   // صدى كتابتنا نحن
      reconcile(remote);
    });

    setSyncStatus(isDirty() ? 'syncing' : 'synced', isDirty() ? 'بانتظار المزامنة' : 'متزامن');
    updateFirebaseStatusCard(true, cfg.projectId);
    localStorage.setItem('sc_firebase_cfg', JSON.stringify(cfg));
    document.getElementById('setup-screen').classList.remove('show');

  } catch(e) {
    showLoading(false);
    setSyncStatus('offline', 'خطأ في الاتصال');
    notify('❌ فشل الاتصال بـ Firebase: ' + e.message, 'var(--red)');
    console.error(e);
  }
}

// الكتابة بمعاملة (transaction): ترفض لو السحابة اتغيرت من جهاز تاني بعد آخر مزامنة
async function doPush() {
  if (!db) return;
  if (_pushing) { _pushAgain = true; return; }
  _pushing = true;
  const ref = doc(db, COLLECTION, DOC_ID);
  const stamp = new Date().toISOString();
  const version = window._localVersion || 0;
  try {
    setSyncStatus('syncing', 'جاري الحفظ...');
    const payload = JSON.parse(JSON.stringify({ ...localPayload(), updatedAt: stamp }));
    _pendingStamp = stamp;
    await runTransaction(db, async (tx) => {
      const snap = await tx.get(ref);
      if (snap.exists()) {
        const remoteAt = snap.data().updatedAt;
        if (remoteAt && remoteAt !== getLastSynced()) throw new Error('CONFLICT');
      }
      tx.set(ref, payload);
    });
    setLastSynced(stamp);
    if ((window._localVersion || 0) === version) setDirty(false);   // لو اتغير شي أثناء الرفع نخليه dirty
    setSyncStatus('synced', 'متزامن');
  } catch (e) {
    _pendingStamp = null;
    if (e && e.message === 'CONFLICT') {
      try { const fresh = await getDoc(ref); if (fresh.exists()) reconcile(fresh.data()); } catch (_) {}
    } else {
      setSyncStatus('offline', 'فشل الحفظ — البيانات محفوظة على الجهاز');
      notify('⚠️ فشل الحفظ على السحابة. بيانات الجهاز سليمة وبترفع أول ما يرجع الاتصال', 'var(--red)');
    }
  } finally {
    _pushing = false;
    if (_pushAgain) { _pushAgain = false; window.pushToFirebase(); }
  }
}

window.pushToFirebase = function() {
  if (!db) return;
  if (_fbDebounceTimer) clearTimeout(_fbDebounceTimer);
  _fbDebounceTimer = setTimeout(doPush, 500);   // تجميع التغييرات المتتالية
};

window.addEventListener('online', () => {
  const cfg = getSavedConfig();
  if (!cfg) return;
  if (!unsubscribe) initFirebase(cfg);
  else if (isDirty()) doPush();
});

function applyRemoteData(remote) {
  if (remote.trades)    { window._data.trades    = remote.trades;    trades    = remote.trades; }
  if (remote.inventory) { window._data.inventory = remote.inventory; inventory = remote.inventory; }
  if (remote.settings)  { window._data.settings  = remote.settings;  settings  = remote.settings; }
  if (remote.accounts)  { window._data.accounts  = remote.accounts;  accounts  = remote.accounts; }
  if (remote.cashMoves) { window._data.cashMoves = remote.cashMoves; cashMoves = remote.cashMoves; }
  if (normalizeAccounts()) save();                   // نسخة قديمة من السحابة → ندمج الحسابات
  if (window.persistLocal) window.persistLocal();     // نحدّث نسخة الجهاز كمان
  populateAccountSelects();
  updateSellStock();
  renderDashboard();
  updateHeader();
  if (document.getElementById('tab-history').classList.contains('active')) renderHistory();
  if (document.getElementById('tab-inventory').classList.contains('active')) renderInventory();
  if (document.getElementById('tab-analytics').classList.contains('active')) renderAnalytics();
  if (document.getElementById('tab-cash').classList.contains('active')) renderCashSection();
}

window.saveFirebaseConfig = function() {
  const cfg = {
    apiKey:            document.getElementById('fb-apiKey').value.trim(),
    authDomain:        document.getElementById('fb-projectId').value.trim() + '.firebaseapp.com',
    projectId:         document.getElementById('fb-projectId').value.trim(),
    storageBucket:     document.getElementById('fb-projectId').value.trim() + '.appspot.com',
    messagingSenderId: '562531232302',
    appId:             document.getElementById('fb-appId').value.trim()
  };
  if (!cfg.apiKey || !cfg.projectId || !cfg.appId) {
    notify('⚠️ أدخل جميع الحقول المطلوبة', 'var(--gold)'); return;
  }
  initFirebase(cfg);
};

window.useLocalOnly = function() {
  document.getElementById('setup-screen').classList.remove('show');
  setSyncStatus('offline', 'وضع محلي');
  notify('⚠️ تعمل بدون مزامنة — البيانات محلية فقط', 'var(--gold)');
};

window.forcSync = async function() {
  if (!db) { notify('⚠️ غير متصل بـ Firebase', 'var(--gold)'); return; }
  await doPush();
  if (!isDirty()) notify('✅ تمت المزامنة');
};

function setSyncStatus(state, text) {
  const el = document.getElementById('sync-status');
  const tx = document.getElementById('sync-text');
  el.className = 'sync-indicator ' + state;
  tx.textContent = text;
}

function showLoading(show) {
  document.getElementById('loading-overlay').classList.toggle('show', show);
}

function updateFirebaseStatusCard(connected, projectId) {
  const el = document.getElementById('firebase-status-card');
  if (connected) {
    el.innerHTML = `<span style="color:var(--green)">✅ متصل بـ Firebase</span> — المشروع: <strong style="color:var(--gold)">${projectId}</strong><br><span style="font-size:11px;color:var(--text2)">بياناتك تتزامن تلقائياً بين كل أجهزتك</span>`;
  } else {
    el.innerHTML = `<span style="color:var(--red)">❌ غير متصل</span> — اضغط "تغيير إعدادات Firebase" للاتصال`;
  }
}

window.showSetupScreen = function() {
  const cfg = getSavedConfig();
  if (cfg) {
    document.getElementById('fb-apiKey').value   = cfg.apiKey    || '';
    document.getElementById('fb-projectId').value= cfg.projectId || '';
    document.getElementById('fb-appId').value    = cfg.appId     || '';
  }
  document.getElementById('setup-screen').classList.add('show');
};

// Auto-init on load
const savedCfg = getSavedConfig();
if (savedCfg) {
  initFirebase(savedCfg);
} else {
  document.getElementById('setup-screen').classList.add('show');
  setSyncStatus('offline', 'غير متصل');
}
