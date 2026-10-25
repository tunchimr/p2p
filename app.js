let initializeApp, getFirestore, doc, setDoc, getDoc, onSnapshot, runTransaction;
async function loadFirebase() {   // تحميل عند الطلب: التطبيق يعمل محلياً حتى بدون إنترنت
  if (initializeApp) return;
  const [a, f] = await Promise.all([
    import("https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js"),
    import("https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js")
  ]);
  ({ initializeApp } = a);
  ({ getFirestore, doc, setDoc, getDoc, onSnapshot, runTransaction } = f);
  window._firebase = { db: null, setDoc, doc, getDoc };
}


// Firebase module logic (kept in its own scope to preserve its original bindings).
{
// ==================== FIREBASE INIT ====================
let db = null;
let unsubscribe = null;
let _fbDebounceTimer = null;
let _pushing = false, _pushAgain = false, _resolving = false, _pendingStamp = null;
const DOC_ID = 'main';
const COLLECTION = 'sudacash';
const LS_SYNCED = 'sc_last_synced';   // آخر نسخة سحابية عرفها هذا الجهاز
const LS_DIRTY  = 'sc_dirty';         // في تغييرات محلية لسه ما اترفعت؟


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
    await loadFirebase();
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
    if (!navigator.onLine) { setSyncStatus('offline', 'وضع محلي — لا يوجد إنترنت'); return; }
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
    const size = JSON.stringify(payload).length;
    if (size > 900000) notify('⚠️ حجم البيانات (' + Math.round(size/1024) + ' KB) قارب حد Firestore (1MB). صدّر نسخة احتياطية وانقل الصفقات القديمة.', 'var(--red)');
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
  window._setLocalData(remote);   // الكتابة على متغيرات الكتلة الثانية تتم من داخلها
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
    el.innerHTML = `<span style="color:var(--green)">✅ متصل بـ Firebase</span> — المشروع: <strong style="color:var(--gold)">${window.esc(projectId)}</strong><br><span style="font-size:11px;color:var(--text2)">بياناتك تتزامن تلقائياً بين كل أجهزتك</span>`;
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

}

// Main application logic.
{
// ==================== DATA ====================
window._data = {};
window._isSaving = false;

let trades    = JSON.parse(localStorage.getItem('p2p_trades')    || '[]');
let inventory = JSON.parse(localStorage.getItem('p2p_inventory') || JSON.stringify({ amount: 0, totalCost: 0 }));
let settings  = JSON.parse(localStorage.getItem('p2p_settings')  || JSON.stringify({ initCash:0, initUSDT:0, initUSDTCost:0, cashBalance:0, defaultMargin:1.5 }));
let accounts  = JSON.parse(localStorage.getItem('p2p_accounts')  || 'null');
if (!accounts) {
  accounts = [{ id: 'cash', name: 'الخزينة', type: 'جنيه سوداني', balance: 0, initBalance: 0, color: '#F0B429' }];
}

let cashMoves = JSON.parse(localStorage.getItem('p2p_cash_moves') || '[]');

// ==================== خزينة واحدة: دمج الحسابات القديمة ====================
const TREASURY_ID = 'cash';
function normalizeAccounts() {
  const single = accounts.length === 1 && accounts[0].id === TREASURY_ID;
  const hasTransfers = cashMoves.some(m => m.type === 'transfer_in' || m.type === 'transfer_out');
  const stale = trades.some(t => t.accountId !== TREASURY_ID) || cashMoves.some(m => m.accountId !== TREASURY_ID);
  if (single && !hasTransfers && !stale) return false;
  const balance = accounts.reduce((x, a) => x + (+a.balance || 0), 0);
  const init    = accounts.reduce((x, a) => x + (+a.initBalance || 0), 0);
  accounts = [{ id: TREASURY_ID, name: 'الخزينة', type: 'جنيه سوداني', balance, initBalance: init, color: '#F0B429' }];
  trades.forEach(t => { t.accountId = TREASURY_ID; });
  const moves = [];
  cashMoves.forEach(m => {
    if (m.type === 'transfer_in') return;   // التحويل بين الحسابات صار داخلي، ما بيغيّر الخزينة
    if (m.type === 'transfer_out') {        // العمولة بس هي اللي كانت خسارة حقيقية
      if (m.fee > 0) moves.push({ id: m.id, date: m.date, type: 'withdraw', accountId: TREASURY_ID, amount: m.fee, fee: 0, balanceAfter: null, notes: 'عمولة تحويل (من النظام القديم)' });
      return;
    }
    moves.push({ ...m, accountId: TREASURY_ID, balanceAfter: null });
  });
  cashMoves = moves;
  return true;
}
if (normalizeAccounts()) save();

function _setLocalData(r) {
  if (r.trades)    trades    = r.trades;
  if (r.inventory) inventory = r.inventory;
  if (r.settings)  settings  = r.settings;
  if (r.accounts)  accounts  = r.accounts;
  if (r.cashMoves) cashMoves = r.cashMoves;
  syncData();
}

// Keep _data in sync with local vars
function syncData() {
  window._data.trades    = trades;
  window._data.inventory = inventory;
  window._data.settings  = settings;
  window._data.accounts  = accounts;
  window._data.cashMoves = cashMoves;
}
syncData();

// Init UI
document.getElementById('buy-date').value      = today();
document.getElementById('sell-date').value     = today();
document.getElementById('cash-date').value     = today();
document.getElementById('transfer-date').value = today();
populateAccountSelects();
loadSettingsForm();
updateSellStock();
  const feeGroup = document.getElementById('cash-fee-group'); if (feeGroup) feeGroup.style.display = 'none';
renderDashboard();
updateHeader();

// بطاقات الجوال: نكتب عنوان العمود على كل خلية ليظهر في عرض البطاقة
function labelCells(tb) {
  const t = tb.closest('table'); if (!t) return;
  const heads = [...t.querySelectorAll('thead th')].map(x => x.textContent.trim());
  tb.querySelectorAll('tr').forEach(r => [...r.children].forEach((c, i) => { if (!c.classList.contains('empty') && heads[i]) c.dataset.label = heads[i]; }));
}
['trades-table','recent-trades','inventory-table','cash-log-table','top-trades'].forEach(id => {
  const b = document.getElementById(id); if (!b) return;
  new MutationObserver(() => labelCells(b)).observe(b, { childList: true }); labelCells(b);
});
function toggleMore() { document.getElementById('more-sheet').classList.toggle('show'); }
function closeMore() { const s = document.getElementById('more-sheet'); if (s) s.classList.remove('show'); }

// ==================== UTILS ====================
function today() { const d = new Date(); return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().split('T')[0]; }

function persistLocal() {
  localStorage.setItem('p2p_trades',    JSON.stringify(trades));
  localStorage.setItem('p2p_inventory', JSON.stringify(inventory));
  localStorage.setItem('p2p_settings',  JSON.stringify(settings));
  localStorage.setItem('p2p_accounts',  JSON.stringify(accounts));
  localStorage.setItem('p2p_cash_moves', JSON.stringify(cashMoves));
}
function save() {
  persistLocal();                                   // نسخة على الجهاز دايماً
  window._localVersion = (window._localVersion || 0) + 1;
  localStorage.setItem('sc_dirty', '1');            // في تغييرات لسه ما اترفعت للسحابة
  syncData();
  if (window.pushToFirebase) window.pushToFirebase();
}
function downloadJSON(data, filename) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a'); a.href = url;
  a.download = filename;
  a.click(); URL.revokeObjectURL(url);
}

function esc(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function fmt(n, dec=0) {
  if (n === undefined || n === null || isNaN(n)) return '-';
  return Number(n).toLocaleString('en-US', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function notify(msg, color='var(--green)') {
  const n = document.getElementById('notif');
  n.textContent = msg; n.style.borderColor = color; n.style.color = color;
  n.classList.add('show');
  setTimeout(() => n.classList.remove('show'), 3000);
}

function getAccount(id) { return accounts.find(a => a.id === id) || null; }

function accBadgeHTML(id) {
  const a = getAccount(id);
  if (!a) return '<span style="color:var(--text2)">—</span>';
  const cls = id === 'bk' ? 'bk' : id === 'faisal' ? 'faisal' : 'other';
  return `<span class="acc-badge ${cls}">🏦 ${esc(a.name)}</span>`;
}

// ==================== ACCOUNTS ====================
function populateAccountSelects() {
  ['buy-account','sell-account','edit-account','f-account','cash-account','transfer-from','transfer-to','cash-filter-acc'].forEach(sid => {
    const el = document.getElementById(sid);
    if (!el) return;
    const isFilter = sid === 'f-account';
    el.innerHTML = isFilter ? '<option value="">💳 كل الحسابات</option>' : '';
    accounts.forEach(a => {
      const opt = document.createElement('option');
      opt.value = a.id;
      opt.textContent = `${esc(a.name)} (${esc(a.type)})`;
      el.appendChild(opt);
    });
  });
}

function renderAccountsList() {
  const el = document.getElementById('accounts-list');
  if (!el) return;
  el.innerHTML = accounts.map(a => `
    <div class="account-row">
      <div class="account-row-dot" style="background:${esc(a.color)}"></div>
      <div class="account-row-info">
        <div class="account-row-name">${esc(a.name)}</div>
        <div class="account-row-type">${esc(a.type)}</div>
      </div>
      <div class="account-row-balance">${fmt(a.balance)} SDG</div>
      <button class="btn btn-outline btn-xs" onclick="openAccModal('${a.id}')">✏️ تعديل</button>
      ${accounts.length > 1 ? `<button class="btn btn-red btn-xs" onclick="deleteAccount('${a.id}')">🗑</button>` : ''}
    </div>
  `).join('');
}

function addAccount() {
  const name    = document.getElementById('new-acc-name').value.trim();
  const type    = document.getElementById('new-acc-type').value.trim();
  const balance = parseFloat(document.getElementById('new-acc-balance').value) || 0;
  if (!name) { notify('⚠️ أدخل اسم الحساب', 'var(--gold)'); return; }
  const colors = ['#9B59B6','#F0B429','#F6465D','#1890FF','#0ECB81'];
  const color  = colors[accounts.length % colors.length];
  accounts.push({ id: 'acc_' + Date.now(), name, type, balance, initBalance: balance, color });
  save(); populateAccountSelects(); renderAccountsList(); updateHeader();
  ['new-acc-name','new-acc-type','new-acc-balance'].forEach(id => document.getElementById(id).value = '');
  notify('✅ تم إضافة الحساب: ' + name);
}

function deleteAccount(id) {
  if (accounts.length <= 1) { notify('⚠️ يجب أن يكون هناك حساب واحد على الأقل', 'var(--red)'); return; }
  if (!confirm('حذف هذا الحساب؟')) return;
  accounts = accounts.filter(a => a.id !== id);
  save(); populateAccountSelects(); renderAccountsList(); updateHeader();
  notify('🗑 تم حذف الحساب');
}

function openAccModal(id) {
  const a = getAccount(id); if (!a) return;
  document.getElementById('acc-edit-id').value      = a.id;
  document.getElementById('acc-edit-name').value    = a.name;
  document.getElementById('acc-edit-type').value    = a.type;
  document.getElementById('acc-edit-balance').value = a.balance;
  document.getElementById('acc-modal').classList.add('show');
}
function closeAccModal() { document.getElementById('acc-modal').classList.remove('show'); }
function saveAccEdit() {
  const id = document.getElementById('acc-edit-id').value;
  const a  = getAccount(id); if (!a) return;
  a.name    = document.getElementById('acc-edit-name').value.trim()    || a.name;
  a.type    = document.getElementById('acc-edit-type').value.trim()    || a.type;
  { const v = parseFloat(document.getElementById('acc-edit-balance').value); if (!isNaN(v)) a.balance = v; }
  save(); closeAccModal(); renderAccountsList(); populateAccountSelects(); updateHeader();
  if (document.getElementById('tab-dashboard').classList.contains('active')) renderDashboard();
  notify('✅ تم تحديث الحساب');
}

// ==================== TABS ====================
function showTab(id, el) {
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  const target = document.getElementById('tab-' + id);
  if (target) target.classList.add('active');
  document.querySelectorAll('.sidebar .nav-item[data-tab]').forEach(item => {
    item.classList.toggle('active', item.dataset.tab === id);
    if (item.dataset.tab === id) item.setAttribute('aria-current', 'page');
    else item.removeAttribute('aria-current');
  });
  const mb = document.getElementById('nav-more'); if (mb) mb.classList.toggle('active', ['inventory','analytics','calc','cash','settings'].includes(id));
  closeMore();
  window.scrollTo({ top: 0 });
  if (id === 'sell')      { prefillSellCost(); updateSellStock(); }
  if (id === 'cash')      { renderCashSection(); }
  if (id === 'dashboard') renderDashboard();
  if (id === 'history')   renderHistory();
  if (id === 'inventory') renderInventory();
  if (id === 'analytics') renderAnalytics();
  if (id === 'settings')  { loadSettingsForm(); renderAccountsList(); }
}

// ==================== BUY ====================
function calcBuyTotal() {
  const amt = parseFloat(document.getElementById('buy-amount').value) || 0;
  const price = parseFloat(document.getElementById('buy-price').value) || 0;
  if (amt && price) { document.getElementById('buy-total').value = (amt * price).toFixed(2); updateBuyPreview(); }
}
function calcBuyPrice() {
  const amt = parseFloat(document.getElementById('buy-amount').value) || 0;
  const total = parseFloat(document.getElementById('buy-total').value) || 0;
  if (amt && total) { document.getElementById('buy-price').value = (total / amt).toFixed(2); updateBuyPreview(); }
}
function calcBuyFee() {
  const total = parseFloat(document.getElementById('buy-total').value) || 0;
  const fee   = parseFloat(document.getElementById('buy-fee-fixed').value) || 0;
  if (total > 0) document.getElementById('buy-fee-pct').value = ((fee / total) * 100).toFixed(4);
  updateBuyPreview();
}
function calcBuyFeePct() {
  const total = parseFloat(document.getElementById('buy-total').value) || 0;
  const pct   = parseFloat(document.getElementById('buy-fee-pct').value) || 0;
  if (total > 0) document.getElementById('buy-fee-fixed').value = ((pct / 100) * total).toFixed(2);
  updateBuyPreview();
}
function updateBuyPreview() {
  const amt       = parseFloat(document.getElementById('buy-amount').value) || 0;
  const price     = parseFloat(document.getElementById('buy-price').value) || 0;
  const fee       = parseFloat(document.getElementById('buy-fee-fixed').value) || 0;
  const accId     = document.getElementById('buy-account').value;
  const total     = amt * price;
  const realTotal = total + fee;
  const realPrice = amt > 0 ? realTotal / amt : 0;
  document.getElementById('bp-amount').textContent     = amt    ? fmt(amt, 2) + ' USDT' : '-';
  document.getElementById('bp-price').textContent      = price  ? fmt(price, 2) + ' SDG' : '-';
  document.getElementById('bp-total').textContent      = total  ? fmt(total, 2) + ' SDG' : '-';
  document.getElementById('bp-fee').textContent        = fee > 0 ? fmt(fee, 2) + ' SDG' : '0 SDG';
  document.getElementById('bp-real-total').textContent = total  ? fmt(realTotal, 2) + ' SDG' : '-';
  document.getElementById('bp-real-price').textContent = realPrice ? fmt(realPrice, 2) + ' SDG/USDT' : '-';
  const acc  = getAccount(accId);
  const note = document.getElementById('buy-cash-note');
  if (acc && acc.balance > 0) { note.textContent = `${acc.name} بعد الشراء: ${fmt(acc.balance - realTotal)} SDG`; }
  else { note.textContent = ''; }
}

function addBuy() {
  const date     = document.getElementById('buy-date').value;
  const platform = document.getElementById('buy-platform').value;
  const accId    = document.getElementById('buy-account').value;
  const amount   = parseFloat(document.getElementById('buy-amount').value);
  const price    = parseFloat(document.getElementById('buy-price').value);
  const fee      = parseFloat(document.getElementById('buy-fee-fixed').value) || 0;
  const notes    = document.getElementById('buy-notes').value;
  if (!date || !amount || !price) { notify('⚠️ أكمل الحقول المطلوبة', 'var(--gold)'); return; }
  const total     = amount * price;
  const realTotal = total + fee;
  const realPrice = realTotal / amount;
  const accChk = getAccount(accId);
  if (accChk && realTotal > accChk.balance && !confirm('الرصيد النقدي (' + fmt(accChk.balance) + ' SDG) أقل من قيمة الشراء (' + fmt(realTotal) + ' SDG). هل تريد المتابعة؟')) return;
  const trade = { id: Date.now(), date, type: 'buy', platform, accountId: accId, amount, price, total, fee, realTotal, realPrice, profit: 0, notes };
  trades.push(trade);
  inventory.amount    += amount;
  inventory.totalCost += realTotal;
  const acc = getAccount(accId);
  if (acc) acc.balance -= realTotal;
  save();
  notify('✅ تم تسجيل الشراء | التكلفة الحقيقية: ' + fmt(realPrice, 2) + ' SDG/USDT');
  ['buy-amount','buy-price','buy-total','buy-fee-fixed','buy-fee-pct','buy-notes'].forEach(id => document.getElementById(id).value = '');
  updateBuyPreview(); updateHeader();
}

// ==================== SELL ====================
function prefillSellCost() {
  const avg = inventory.amount > 0 ? inventory.totalCost / inventory.amount : 0;
  document.getElementById('sell-cost').value = avg > 0 ? avg.toFixed(2) : '';
  calcSellProfit();
}
function updateSellStock() {
  document.getElementById('sell-stock-avail').textContent = fmt(inventory.amount, 2) + ' USDT';
}
function calcSellProfit() {
  const amt     = parseFloat(document.getElementById('sell-amount').value) || 0;
  const price   = parseFloat(document.getElementById('sell-price').value) || 0;
  const cost    = parseFloat(document.getElementById('sell-cost').value) || 0;
  const revenue = amt * price;
  const costT   = amt * cost;
  const profit  = revenue - costT;
  const margin  = cost > 0 ? ((price - cost) / cost * 100) : 0;
  document.getElementById('sp-revenue').textContent = amt && price ? fmt(revenue, 2) + ' SDG' : '-';
  document.getElementById('sp-cost').textContent    = amt && cost  ? fmt(costT, 2) + ' SDG' : '-';
  document.getElementById('sp-margin').textContent  = cost > 0 ? fmt(margin, 2) + '%' : '-';
  const pr = document.getElementById('sp-profit');
  if (amt && price && cost) {
    pr.textContent = fmt(profit, 2) + ' SDG';
    pr.style.color = profit >= 0 ? 'var(--green)' : 'var(--red)';
    document.getElementById('sp-profit-row').className = 'calc-row total' + (profit < 0 ? ' loss' : '');
  } else { pr.textContent = '-'; }
}

function addSell() {
  const date     = document.getElementById('sell-date').value;
  const platform = document.getElementById('sell-platform').value;
  const accId    = document.getElementById('sell-account').value;
  const amount   = parseFloat(document.getElementById('sell-amount').value);
  const price    = parseFloat(document.getElementById('sell-price').value);
  const cost     = parseFloat(document.getElementById('sell-cost').value);
  const notes    = document.getElementById('sell-notes').value;
  if (!date || !amount || !price || !cost) { notify('⚠️ أكمل الحقول المطلوبة', 'var(--gold)'); return; }
  if (amount > inventory.amount + 0.01) { notify('⚠️ الكمية أكبر من المخزون (' + fmt(inventory.amount, 2) + ' USDT)', 'var(--red)'); return; }
  const total  = amount * price;
  const profit = total - (amount * cost);
  const trade  = { id: Date.now(), date, type: 'sell', platform, accountId: accId, amount, price, total, fee: 0, realTotal: total, realPrice: price, profit, cost, notes };
  trades.push(trade);
  const ratio = inventory.amount > 0 ? 1 - (amount / inventory.amount) : 0;
  inventory.totalCost = inventory.totalCost * ratio;
  inventory.amount   -= amount;
  if (inventory.amount < 0.001) { inventory.amount = 0; inventory.totalCost = 0; }
  const acc = getAccount(accId);
  if (acc) acc.balance += total;
  save();
  notify('🔴 تم البيع | الربح: ' + fmt(profit, 2) + ' SDG');
  ['sell-amount','sell-price','sell-cost','sell-notes'].forEach(id => document.getElementById(id).value = '');
  updateSellStock(); updateHeader(); prefillSellCost();
}

// ==================== DASHBOARD ====================
function renderDashboard() {
  const sells      = trades.filter(t => t.type === 'sell');
  const buys       = trades.filter(t => t.type === 'buy');
  const totalProfit    = sells.reduce((s, t) => s + t.profit, 0);
  const totalBought    = buys.reduce((s, t) => s + t.amount, 0);
  const totalBoughtSDG = buys.reduce((s, t) => s + t.total, 0);
  const totalSold      = sells.reduce((s, t) => s + t.amount, 0);
  const totalSoldSDG   = sells.reduce((s, t) => s + t.total, 0);
  // هامش الربح المرجّح: إجمالي الربح ÷ إجمالي تكلفة الكميات المباعة (الصفقات الكبيرة تأخذ وزنها الحقيقي)
  const sellsWithCost = sells.filter(t => t.cost > 0);
  const soldCostBase  = sellsWithCost.reduce((s, t) => s + t.amount * t.cost, 0);
  const avgMargin = soldCostBase > 0 ? sellsWithCost.reduce((s, t) => s + t.profit, 0) / soldCostBase * 100 : 0;
  const avgCost    = inventory.amount > 0 ? inventory.totalCost / inventory.amount : 0;
  const stockSDG   = inventory.amount * avgCost;
  const totalCash  = accounts.reduce((s, a) => s + a.balance, 0);
  const totalCap   = totalCash + stockSDG;
  const initCash   = accounts.reduce((s, a) => s + (a.initBalance || 0), 0);
  const initCap    = initCash + (settings.initUSDT * settings.initUSDTCost);
  const growth     = initCap > 0 ? ((totalCap - initCap) / initCap * 100) : null;

  const banner = document.getElementById('accounts-banner');
  banner.innerHTML = `<div class="account-card account-card-total"><div class="account-card-label">💰 خزينة الجنيه</div><div class="account-card-name">الرصيد الحالي</div><div class="account-card-balance">${fmt(totalCash)} SDG</div><div class="account-card-type">رصيد ابتدائي: ${fmt(initCash)} SDG | رأس المال الكلي: ${fmt(totalCap)} SDG</div></div>`;

  document.getElementById('d-stock').textContent        = fmt(inventory.amount, 2) + ' USDT';
  document.getElementById('d-stock-sdg').textContent    = '≈ ' + fmt(stockSDG) + ' SDG';
  document.getElementById('d-profit').textContent       = fmt(totalProfit) + ' SDG';
  document.getElementById('d-profit-usdt').textContent  = avgCost > 0 ? '≈ ' + fmt(totalProfit / avgCost, 2) + ' USDT' : '';
  document.getElementById('d-trades').textContent       = trades.length;
  document.getElementById('d-trades-split').textContent = 'شراء: ' + buys.length + ' | بيع: ' + sells.length;
  document.getElementById('d-margin').textContent       = fmt(avgMargin, 2) + '%';
  document.getElementById('d-total-bought').textContent     = fmt(totalBought, 2) + ' USDT';
  document.getElementById('d-total-bought-sdg').textContent = '= ' + fmt(totalBoughtSDG) + ' SDG';
  document.getElementById('d-total-sold').textContent       = fmt(totalSold, 2) + ' USDT';
  document.getElementById('d-total-sold-sdg').textContent   = '= ' + fmt(totalSoldSDG) + ' SDG';
  document.getElementById('d-total-capital').textContent    = fmt(totalCap) + ' SDG';
  const growthEl = document.getElementById('d-growth');
  if (growth !== null) {
    growthEl.textContent = 'نمو: ' + (growth >= 0 ? '+' : '') + fmt(growth, 2) + '%';
    growthEl.style.color = growth >= 0 ? 'var(--green)' : 'var(--red)';
  } else { growthEl.textContent = 'نمو: —'; growthEl.style.color = 'var(--text2)'; }

  // Dashboard summary cards (display only; source values remain the existing ledger).
  const stockSummary = document.getElementById('summary-stock');
  const stockCostSummary = document.getElementById('summary-stock-cost');
  const cashSummary = document.getElementById('summary-cash');
  const tradesSummary = document.getElementById('summary-trades');
  if (stockSummary) stockSummary.textContent = fmt(inventory.amount, 2) + ' USDT';
  if (stockCostSummary) stockCostSummary.textContent = fmt(stockSDG) + ' SDG';
  if (cashSummary) cashSummary.textContent = fmt(totalCash) + ' SDG';
  if (tradesSummary) tradesSummary.textContent = String(trades.length);
  const dateLabel = document.getElementById('dashboard-date');
  if (dateLabel) dateLabel.textContent = new Date().toLocaleDateString('ar', { year: 'numeric', month: 'short', day: 'numeric' });
  renderProfitChart();

  const recent = [...trades].reverse().slice(0, 8);
  const tbody  = document.getElementById('recent-trades');
  if (!recent.length) { tbody.innerHTML = '<tr><td colspan="8" class="empty"><div class="empty-icon">📭</div>لا توجد صفقات بعد</td></tr>'; return; }
  tbody.innerHTML = recent.map(t => `
    <tr>
      <td>${esc(t.date)}</td>
      <td><span class="badge ${t.type==='buy'?'badge-green':'badge-red'}">${t.type==='buy'?'شراء':'بيع'}</span></td>
      <td><span class="platform ${esc(t.platform.toLowerCase())}">${esc(t.platform)}</span></td>
      <td>${fmt(t.amount,2)}</td>
      <td>${fmt(t.price,2)}</td>
      <td>${fmt(t.total,2)}</td>
      <td style="color:var(--red)">${t.fee>0?fmt(t.fee,2):'-'}</td>
      <td style="color:${t.profit>0?'var(--green)':t.profit<0?'var(--red)':'var(--text2)'}">${t.type==='sell'?fmt(t.profit,2):'-'}</td>
    </tr>
  `).join('');
}

// Dashboard chart: cumulative realized profit grouped by sell date.
function renderProfitChart() {
  const host = document.getElementById('profit-chart');
  if (!host) return;
  const sells = trades.filter(t => t.type === 'sell' && t.date)
    .slice().sort((a, b) => String(a.date).localeCompare(String(b.date)));
  if (!sells.length) {
    host.innerHTML = '<div class="chart-empty">ستظهر حركة الأرباح هنا بعد تسجيل عمليات البيع.</div>';
    return;
  }
  const byDate = new Map();
  sells.forEach(t => byDate.set(String(t.date), (byDate.get(String(t.date)) || 0) + (Number(t.profit) || 0)));
  let cumulative = 0;
  const points = [...byDate.entries()].sort((a,b) => a[0].localeCompare(b[0])).map(([date, profit]) => {
    cumulative += profit;
    return { date, value: cumulative };
  });
  const values = points.map(p => p.value);
  let min = Math.min(0, ...values), max = Math.max(0, ...values);
  if (max === min) max = min + 1;
  const W = 640, H = 205, left = 16, right = 14, top = 16, bottom = 28;
  const plotW = W-left-right, plotH = H-top-bottom;
  const xy = points.map((p, i) => ({
    x: points.length === 1 ? W/2 : left + i * plotW / (points.length-1),
    y: top + (max-p.value) / (max-min) * plotH,
    ...p
  }));
  const line = xy.map((p,i) => (i ? 'L' : 'M') + p.x.toFixed(1) + ' ' + p.y.toFixed(1)).join(' ');
  const zeroY = top + (max-0)/(max-min)*plotH;
  const area = line + ` L ${xy[xy.length-1].x.toFixed(1)} ${zeroY.toFixed(1)} L ${xy[0].x.toFixed(1)} ${zeroY.toFixed(1)} Z`;
  const firstDate = points[0].date, lastDate = points[points.length-1].date;
  const lastValue = points[points.length-1].value;
  host.innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="الأرباح المحققة التراكمية">
    <defs><linearGradient id="sc-profit-gradient" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#D7FF43" stop-opacity=".25"/><stop offset="100%" stop-color="#D7FF43" stop-opacity="0"/></linearGradient></defs>
    ${[0,1,2,3].map(i => `<line x1="${left}" y1="${top+i*plotH/3}" x2="${W-right}" y2="${top+i*plotH/3}" stroke="#303A32" stroke-dasharray="3 5"/>`).join('')}
    <path d="${area}" fill="url(#sc-profit-gradient)"/>
    <path d="${line}" fill="none" stroke="#D7FF43" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
    <circle cx="${xy[xy.length-1].x}" cy="${xy[xy.length-1].y}" r="4.5" fill="#D7FF43" stroke="#1D2420" stroke-width="2"/>
    <text x="${left}" y="${H-7}" fill="#7F8A81" font-size="10" text-anchor="start">${esc(firstDate)}</text>
    <text x="${W-right}" y="${H-7}" fill="#7F8A81" font-size="10" text-anchor="end">${esc(lastDate)}</text>
    <text x="${W-right}" y="${top+10}" fill="${lastValue >= 0 ? '#6EE7A8' : '#F36B75'}" font-size="11" font-weight="700" text-anchor="end">${fmt(lastValue)} SDG</text>
  </svg>`;
}

// ==================== HISTORY ====================
let filterType = 'all';
function setFilter(type, el) {
  filterType = type;
  document.querySelectorAll('.filter-bar .btn').forEach(b => b.classList.remove('active-filter'));
  if (el) el.classList.add('active-filter');
  renderHistory();
}
function clearFilters() {
  filterType = 'all';
  ['f-platform','f-from','f-to'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('f-account').value = '';
  document.querySelectorAll('.filter-bar .btn').forEach(b => b.classList.remove('active-filter'));
  document.getElementById('f-all').classList.add('active-filter');
  renderHistory();
}
function renderHistory() {
  let list = [...trades];
  if (filterType !== 'all') list = list.filter(t => t.type === filterType);
  const plat = document.getElementById('f-platform').value.trim().toLowerCase();
  if (plat) list = list.filter(t => t.platform.toLowerCase().includes(plat));
  const fAcc = document.getElementById('f-account').value;
  if (fAcc) list = list.filter(t => t.accountId === fAcc);
  const from = document.getElementById('f-from').value;
  const to   = document.getElementById('f-to').value;
  if (from) list = list.filter(t => t.date >= from);
  if (to)   list = list.filter(t => t.date <= to);
  const tbody = document.getElementById('trades-table');
  if (!list.length) { tbody.innerHTML = '<tr><td colspan="12" class="empty"><div class="empty-icon">📭</div>لا توجد صفقات</td></tr>'; return; }
  tbody.innerHTML = [...list].reverse().map((t, i) => `
    <tr>
      <td style="color:var(--text2)">${list.length - i}</td>
      <td>${esc(t.date)}</td>
      <td><span class="badge ${t.type==='buy'?'badge-green':'badge-red'}">${t.type==='buy'?'شراء':'بيع'}</span></td>
      <td><span class="platform ${esc(t.platform.toLowerCase())}">${esc(t.platform)}</span></td>
      <td>${fmt(t.amount,2)}</td>
      <td>${fmt(t.price,2)}</td>
      <td>${fmt(t.total,2)}</td>
      <td style="color:var(--red)">${t.fee>0?fmt(t.fee,2):'-'}</td>
      <td style="color:var(--blue)">${t.type==='buy'&&t.realPrice?fmt(t.realPrice,2):'-'}</td>
      <td style="color:${t.profit>0?'var(--green)':t.profit<0?'var(--red)':'var(--text2)'}">${t.type==='sell'?fmt(t.profit,2):'-'}</td>
      <td style="font-size:11px;color:var(--text2)">${esc(t.notes)||'-'}</td>
      <td><div style="display:flex;gap:4px"><button class="btn btn-outline btn-xs" onclick="openEdit(${Number(t.id)})">✏️</button><button class="btn btn-red btn-xs" onclick="deleteTrade(${Number(t.id)})">🗑</button></div></td>
    </tr>
  `).join('');
}

// ==================== إعادة حساب المخزون (Replay) ====================
// الفكرة: نرجع المخزون لحالته قبل أول صفقة (بعكس الصفقات من الآخر)،
// وبعدين نعيد تشغيل الصفقات بالترتيب. كده أي حذف أو تعديل يتحسب متوسط التكلفة والأرباح بعده صح.
function deriveBaseline(list) {
  let A = inventory.amount, C = inventory.totalCost;
  for (let i = list.length - 1; i >= 0; i--) {
    const t = list[i];
    if (t.type === 'buy') { A -= t.amount; C -= (t.realTotal || t.total); }
    else {
      // البيع بمتوسط التكلفة ما بيغيّر المتوسط، فالمتوسط قبله = بعده
      const avg = A > 0.001 ? C / A : (t.cost || 0);
      A += t.amount; C += t.amount * avg;
    }
  }
  if (A < -0.01 || C < -0.01) return null;   // التاريخ ما متسق (مثلاً تعديل يدوي للمخزون) → ما نقدر نعيد الحساب
  return { amount: Math.max(A, 0), totalCost: Math.max(C, 0) };
}
// fromIndex: الصفقات من هذا الموضع وما بعده بس هي اللي بتتحدّث تكلفتها وربحها
function replayTrades(list, base, fromIndex) {
  let A = base.amount, C = base.totalCost;
  const updates = [];
  for (let i = 0; i < list.length; i++) {
    const t = list[i];
    if (t.type === 'buy') { A += t.amount; C += (t.realTotal || t.total); continue; }
    if (t.amount > A + 0.01) return { ok: false, trade: t, stock: A };
    const avg  = A > 0.001 ? C / A : (t.cost || 0);
    const cost = parseFloat(avg.toFixed(2));            // نفس التقريب البيظهر في شاشة البيع
    if (i >= fromIndex) updates.push({ trade: t, cost, profit: t.total - t.amount * cost });
    const ratio = A > 0 ? 1 - (t.amount / A) : 0;
    C = C * ratio; A = A - t.amount;
    if (A < 0.001) { A = 0; C = 0; }
  }
  return { ok: true, inventory: { amount: A, totalCost: C }, updates };
}
function applyReplayUpdates(updates) {
  updates.forEach(u => { u.trade.cost = u.cost; u.trade.profit = u.profit; });
}
function replayBlockedMsg(res) {
  return '⚠️ ما بتقدر تنفّذ التغيير: بيع بتاريخ ' + res.trade.date + ' (' + fmt(res.trade.amount, 2) +
         ' USDT) ما بيلقى مخزون كافي (المتاح ' + fmt(res.stock, 2) + ' USDT). عدّل أو احذف البيع ده الأول.';
}

function deleteTrade(id) {
  if (!confirm('هل تريد حذف هذه الصفقة؟')) return;
  const idx = trades.findIndex(x => x.id === id);
  if (idx < 0) return;
  const t   = trades[idx];
  const acc = getAccount(t.accountId);

  const base = deriveBaseline(trades);
  if (base) {
    // نعيد حساب المخزون ومتوسط التكلفة وأرباح كل البيوعات اللي بعد الصفقة المحذوفة
    const newList = trades.filter(x => x.id !== id);
    const res = replayTrades(newList, base, idx);
    if (!res.ok) { notify(replayBlockedMsg(res), 'var(--red)'); return; }
    inventory = res.inventory;
    applyReplayUpdates(res.updates);
    trades = newList;
  } else {
    // التاريخ غير متسق (غالباً بسبب تعديل يدوي للمخزون) → الطريقة القديمة
    if (t.type === 'buy') {
      inventory.amount    -= t.amount;
      inventory.totalCost -= t.realTotal;
    } else {
      inventory.amount    += t.amount;
      inventory.totalCost += t.amount * (t.cost || 0);
    }
    if (inventory.totalCost < 0) inventory.totalCost = 0;
    if (inventory.amount < 0.001) { inventory.amount = 0; inventory.totalCost = 0; }
    trades = trades.filter(x => x.id !== id);
    notify('ℹ️ المخزون اتعدّل يدوياً قبل كده، فمتوسط التكلفة ما اتعاد حسابو', 'var(--gold)');
  }

  // الحسابات البنكية
  if (acc) acc.balance += (t.type === 'buy') ? t.realTotal : -t.total;

  save(); renderHistory(); updateHeader(); updateSellStock();
  if (document.getElementById('tab-dashboard').classList.contains('active')) renderDashboard();
  notify('🗑 تم حذف الصفقة وإعادة حساب المخزون والأرباح');
}

function clearAll() {
  if (!confirm('حذف جميع الصفقات؟ لا يمكن التراجع.')) return;
  trades = [];
  inventory = (settings.initUSDT > 0 && settings.initUSDTCost > 0) ? { amount: settings.initUSDT, totalCost: settings.initUSDT * settings.initUSDTCost } : { amount: 0, totalCost: 0 };
  cashMoves = []; // مسح سجل الحركات النقدية أيضاً
  accounts.forEach(a => a.balance = a.initBalance || 0);
  save(); renderHistory(); updateHeader();
  if (document.getElementById('tab-cash').classList.contains('active')) renderCashSection();
  notify('🗑 تم حذف جميع الصفقات والحركات النقدية');
}

// ==================== EDIT MODAL ====================
function openEdit(id) {
  const t = trades.find(x => x.id === id); if (!t) return;
  const editId = document.getElementById('edit-id');
  editId.value              = t.id;
  editId.dataset.oldTotal   = t.total;      // نحفظ القيمة القديمة
  editId.dataset.oldAccId   = t.accountId;  // نحفظ الحساب القديم
  editId.dataset.tradeType  = t.type;       // نحفظ النوع
  document.getElementById('edit-date').value     = t.date;
  document.getElementById('edit-platform').value = t.platform;
  document.getElementById('edit-account').value  = t.accountId || '';
  document.getElementById('edit-amount').value   = t.amount;
  document.getElementById('edit-price').value    = t.price;
  document.getElementById('edit-fee').value      = t.fee || 0;
  document.getElementById('edit-fee').disabled = t.type === 'sell';
  document.getElementById('edit-notes').value    = t.notes || '';
  document.getElementById('edit-modal').classList.add('show');
}
function closeModal() { document.getElementById('edit-modal').classList.remove('show'); }
function saveEdit() {
  const id  = parseInt(document.getElementById('edit-id').value);
  const idx = trades.findIndex(x => x.id === id); if (idx < 0) return;
  const t   = trades[idx];
  const oldRealTotal = t.realTotal || t.total;
  const oldAccId     = t.accountId;
  const newDate  = document.getElementById('edit-date').value;
  const newAmt   = parseFloat(document.getElementById('edit-amount').value);
  const newPrice = parseFloat(document.getElementById('edit-price').value);
  // التحقق من صحة البيانات
  if (!newDate) { notify('⚠️ أدخل التاريخ', 'var(--gold)'); return; }
  if (!newAmt || newAmt <= 0) { notify('⚠️ أدخل كمية صحيحة أكبر من صفر', 'var(--gold)'); return; }
  if (!newPrice || newPrice <= 0) { notify('⚠️ أدخل سعر صحيح أكبر من صفر', 'var(--gold)'); return; }
  const before = { ...t };                 // نسخة للتراجع لو التعديل ما ينفع
  const base   = deriveBaseline(trades);   // حالة المخزون قبل أول صفقة (بالقيم القديمة)
  t.date      = newDate;
  t.platform  = document.getElementById('edit-platform').value;
  t.accountId = document.getElementById('edit-account').value;
  t.amount    = newAmt;
  t.price     = newPrice;
  t.fee       = parseFloat(document.getElementById('edit-fee').value)    || 0;
  t.notes     = document.getElementById('edit-notes').value;
  t.total     = t.amount * t.price;
  t.realTotal = t.total + t.fee;
  t.realPrice = t.amount > 0 ? t.realTotal / t.amount : 0;
  if (t.type === 'sell') { t.fee = 0; t.realTotal = t.total; t.realPrice = t.price; }
  if (t.type === 'sell') t.profit = t.total - (t.amount * (t.cost || 0));
  const order = [...trades], resort = before.date !== newDate;
  if (resort) trades.sort((x, y) => x.date === y.date ? x.id - y.id : (x.date < y.date ? -1 : 1));
  let replayed = false;
  if (base) {
    const res = replayTrades(trades, base, resort ? 0 : idx);
    if (!res.ok) { Object.assign(t, before); if (resort) trades.splice(0, trades.length, ...order); notify(replayBlockedMsg(res), 'var(--red)'); return; }
    inventory = res.inventory;
    applyReplayUpdates(res.updates);
    replayed = true;
  }
  if (t.type === 'buy') {
    // تعديل شراء: عدّل المخزون (لو ما اتعاد حسابو) والحساب
    if (!replayed) inventory.totalCost = inventory.totalCost - oldRealTotal + t.realTotal;
    const oldAcc = getAccount(oldAccId);
    const newAcc = getAccount(t.accountId);
    if (oldAcc) oldAcc.balance += oldRealTotal;   // ارجع القيمة القديمة
    if (newAcc) newAcc.balance -= t.realTotal;    // اخصم القيمة الجديدة
  } else {
    // تعديل بيع: عدّل الحساب فقط (المخزون اتخصم وقت البيع الأصلي)
    const oldTotal  = parseFloat(document.getElementById('edit-id').dataset.oldTotal) || t.total;
    const storedAcc = document.getElementById('edit-id').dataset.oldAccId;
    const oldAcc    = getAccount(storedAcc || oldAccId);
    const newAcc   = getAccount(t.accountId);
    if (oldAcc) oldAcc.balance -= oldTotal;       // اخصم القيمة القديمة
    if (newAcc) newAcc.balance += t.total;        // أضف القيمة الجديدة
    // أعد حساب الربح
    t.profit = t.total - (t.amount * (t.cost || 0));
  }
  save(); closeModal(); renderHistory(); updateHeader(); updateSellStock();
  if (document.getElementById('tab-dashboard').classList.contains('active')) renderDashboard();
  notify('✅ تم حفظ التعديل وإعادة حساب المخزون والأرباح');
}

// ==================== INVENTORY ====================
function renderInventory() {
  const avgCost = inventory.amount > 0 ? inventory.totalCost / inventory.amount : 0;
  const margin  = settings.defaultMargin || 1.5;
  document.getElementById('inv-stock').textContent      = fmt(inventory.amount, 2) + ' USDT';
  document.getElementById('inv-stock-sdg').textContent  = 'بتكلفة حقيقية: ' + fmt(inventory.totalCost) + ' SDG';
  document.getElementById('inv-avg-cost').textContent   = fmt(avgCost, 2) + ' SDG';
  document.getElementById('inv-suggest').textContent    = fmt(avgCost * (1 + margin/100), 2) + ' SDG';
  document.getElementById('inv-suggest-pct').textContent= `بهامش ${margin}% من التكلفة`;
  const buys = trades.filter(t => t.type === 'buy');
  const tbody = document.getElementById('inventory-table');
  if (!buys.length) { tbody.innerHTML = '<tr><td colspan="7" class="empty"><div class="empty-icon">📦</div>لا توجد مشتريات</td></tr>'; return; }
  tbody.innerHTML = [...buys].reverse().map(t => `
    <tr>
      <td>${esc(t.date)}</td>
      <td><span class="platform ${esc(t.platform.toLowerCase())}">${esc(t.platform)}</span></td>
      <td>${fmt(t.amount,2)}</td>
      <td>${fmt(t.price,2)}</td>
      <td>${fmt(t.total,2)}</td>
      <td style="color:var(--red)">${t.fee>0?fmt(t.fee,2):'0'}</td>
      <td style="color:var(--blue)">${fmt(t.realPrice||t.price,2)}</td>
    </tr>
  `).join('');
}
function adjustInventory() {
  const amount = parseFloat(document.getElementById('adj-amount').value);
  const cost   = parseFloat(document.getElementById('adj-cost').value);
  if (!amount || !cost) { notify('أدخل الكمية والتكلفة', 'var(--gold)'); return; }
  inventory.amount = amount; inventory.totalCost = amount * cost;
  save(); renderInventory(); updateHeader();
  notify('✅ تم تحديث المخزون');
}

// ==================== CALCULATOR ====================
function calcHelper() {
  const amt    = parseFloat(document.getElementById('c-amount').value) || 0;
  const buy    = parseFloat(document.getElementById('c-buy').value)    || 0;
  const fee    = parseFloat(document.getElementById('c-fee').value)    || 0;
  const margin = parseFloat(document.getElementById('c-margin').value) || 0;
  if (amt && buy) {
    const totalCost = amt * buy;
    const realTotal = totalCost + fee;
    const realPrice = realTotal / amt;
    const sellPrice = realPrice * (1 + margin / 100);
    const profit    = (sellPrice - realPrice) * amt;
    document.getElementById('c-total-cost').textContent = fmt(totalCost, 2) + ' SDG';
    document.getElementById('c-fee-val').textContent    = fee > 0 ? fmt(fee, 2) + ' SDG' : '0 SDG';
    document.getElementById('c-real-price').textContent = fmt(realPrice, 2) + ' SDG/USDT';
    document.getElementById('c-sell-price').textContent = margin ? fmt(sellPrice, 2) + ' SDG/USDT' : '-';
    document.getElementById('c-net').textContent        = margin ? fmt(profit, 2) + ' SDG' : '-';
  }
}
function calcMargin() {
  const buy  = parseFloat(document.getElementById('m-buy').value)    || 0;
  const sell = parseFloat(document.getElementById('m-sell').value)   || 0;
  const amt  = parseFloat(document.getElementById('m-amount').value) || 0;
  const fee  = parseFloat(document.getElementById('m-fee').value)    || 0;
  if (buy && sell) {
    const spread = sell - buy;
    const pct    = (spread / buy) * 100;
    const gross  = amt * spread;
    const net    = gross - fee;
    document.getElementById('m-spread').textContent = fmt(spread, 2) + ' SDG';
    document.getElementById('m-pct').textContent    = fmt(pct, 3) + '%';
    document.getElementById('m-gross').textContent  = amt ? fmt(gross, 2) + ' SDG' : '-';
    document.getElementById('m-total').textContent  = amt ? fmt(net, 2) + ' SDG' : '-';
    document.getElementById('m-total').style.color  = net >= 0 ? 'var(--green)' : 'var(--red)';
  }
}

// ==================== ANALYTICS ====================
function renderAnalytics() {
  const sells    = trades.filter(t => t.type === 'sell');
  const buys     = trades.filter(t => t.type === 'buy');
  const totalFees= buys.reduce((s, t) => s + (t.fee || 0), 0);
  const byDay = {};
  sells.forEach(t => { byDay[t.date] = (byDay[t.date] || 0) + t.profit; });
  const bestDay = Object.entries(byDay).sort((a, b) => b[1] - a[1])[0];
  document.getElementById('a-best-day').textContent        = bestDay ? bestDay[0] : '—';
  document.getElementById('a-best-day-profit').textContent = bestDay ? fmt(bestDay[1]) + ' SDG' : '';
  const byPlat = {};
  trades.forEach(t => { byPlat[t.platform] = (byPlat[t.platform] || 0) + 1; });
  const topPlat = Object.entries(byPlat).sort((a, b) => b[1] - a[1])[0];
  document.getElementById('a-top-platform').textContent       = topPlat ? topPlat[0] : '—';
  document.getElementById('a-top-platform-count').textContent = topPlat ? topPlat[1] + ' صفقة' : '';
  const avgSize = trades.length > 0 ? trades.reduce((s, t) => s + t.amount, 0) / trades.length : 0;
  document.getElementById('a-avg-size').textContent   = avgSize > 0 ? fmt(avgSize, 2) : '—';
  document.getElementById('a-total-fees').textContent = fmt(totalFees);
  const maxMargin = sells.length > 0 ? sells.reduce((best, t) => { const m = t.cost > 0 ? (t.price - t.cost) / t.cost * 100 : 0; return m > (best.m || 0) ? { m, t } : best; }, {}) : {};
  document.getElementById('a-max-margin').textContent      = maxMargin.m ? fmt(maxMargin.m, 3) + '%' : '—';
  document.getElementById('a-max-margin-date').textContent = maxMargin.t ? maxMargin.t.date : '';
  const maxSell = sells.length > 0 ? sells.reduce((best, t) => t.profit > (best.profit || -Infinity) ? t : best, {}) : {};
  document.getElementById('a-max-sell').textContent      = maxSell.profit !== undefined ? fmt(maxSell.profit) + ' SDG' : '—';
  document.getElementById('a-max-sell-date').textContent = maxSell.date || '';
  const days = new Set(trades.map(t => t.date));
  document.getElementById('a-trading-days').textContent = days.size;
  const totalProfit = sells.reduce((s, t) => s + t.profit, 0);
  const sellDays    = new Set(sells.map(t => t.date));
  document.getElementById('a-daily-profit').textContent = sellDays.size > 0 ? fmt(totalProfit / sellDays.size) + ' SDG' : '—';

  const pb = document.getElementById('platform-breakdown');
  if (!trades.length) { pb.innerHTML = '<div style="color:var(--text2);text-align:center;padding:20px">لا توجد صفقات بعد</div>'; }
  else {
    pb.innerHTML = ['Binance','OKX'].map(p => {
      const ptrades = trades.filter(t => t.platform === p);
      const psells  = sells.filter(t => t.platform === p);
      const pprofit = psells.reduce((s, t) => s + t.profit, 0);
      const pct     = trades.length > 0 ? (ptrades.length / trades.length * 100) : 0;
      if (!ptrades.length) return '';
      return `<div style="margin-bottom:16px"><div style="display:flex;justify-content:space-between;margin-bottom:6px"><span class="platform ${p.toLowerCase()}" style="font-size:14px">${p}</span><span style="font-size:13px;color:var(--text2)">${ptrades.length} صفقة (${fmt(pct,1)}%)</span></div><div style="background:var(--bg3);border-radius:4px;height:8px;overflow:hidden"><div style="background:${p==='Binance'?'var(--gold)':'#e0e0e0'};width:${pct}%;height:100%;border-radius:4px;transition:width 0.5s"></div></div><div style="font-size:12px;color:var(--text2);margin-top:4px">ربح: <span style="color:var(--green)">${fmt(pprofit)} SDG</span></div></div>`;
    }).join('');
  }

  const ab = document.getElementById('account-breakdown');
  if (!sells.length) { ab.innerHTML = '<div style="color:var(--text2);text-align:center;padding:20px">لا توجد مبيعات بعد</div>'; }
  else {
    ab.innerHTML = accounts.map(a => {
      const aSells  = sells.filter(t => t.accountId === a.id);
      const aprofit = aSells.reduce((s, t) => s + t.profit, 0);
      const pct     = sells.length > 0 ? (aSells.length / sells.length * 100) : 0;
      if (!aSells.length) return '';
      return `<div style="margin-bottom:16px"><div style="display:flex;justify-content:space-between;margin-bottom:6px"><span style="font-size:14px;font-weight:700;color:${esc(a.color)}">${esc(a.name)}</span><span style="font-size:13px;color:var(--text2)">${aSells.length} صفقة (${fmt(pct,1)}%)</span></div><div style="background:var(--bg3);border-radius:4px;height:8px;overflow:hidden"><div style="background:${esc(a.color)};width:${pct}%;height:100%;border-radius:4px;transition:width 0.5s"></div></div><div style="font-size:12px;color:var(--text2);margin-top:4px">ربح: <span style="color:var(--green)">${fmt(aprofit)} SDG</span> | رصيد: <span style="color:${esc(a.color)}">${fmt(a.balance)} SDG</span></div></div>`;
    }).join('') || '<div style="color:var(--text2);text-align:center;padding:20px">لا توجد مبيعات مرتبطة بحسابات</div>';
  }

  const top5 = [...sells].sort((a, b) => b.profit - a.profit).slice(0, 5);
  document.getElementById('top-trades').innerHTML = top5.length ? top5.map((t, i) => `
    <tr>
      <td style="color:var(--gold)">${i+1}</td>
      <td>${esc(t.date)}</td>
      <td>${fmt(t.amount,2)}</td>
      <td style="color:var(--blue)">${t.cost>0?fmt((t.price-t.cost)/t.cost*100,3)+'%':'-'}</td>
      <td style="color:var(--green)">${fmt(t.profit,2)}</td>
    </tr>
  `).join('') : '<tr><td colspan="5" class="empty" style="padding:20px">لا توجد مبيعات</td></tr>';
}

// ==================== SETTINGS ====================
function loadSettingsForm() {
  document.getElementById('s-init-usdt').value      = settings.initUSDT || '';
  document.getElementById('s-init-usdt-cost').value = settings.initUSDTCost || '';
  document.getElementById('s-default-margin').value = settings.defaultMargin || 1.5;
  const ic = document.getElementById('s-init-cash'); if (ic) ic.value = (accounts[0] && accounts[0].initBalance) || '';
  const cn = document.getElementById('s-cash-now'); if (cn && accounts[0]) cn.textContent = 'الرصيد الحالي: ' + fmt(accounts[0].balance) + ' SDG';
  const initCash = accounts.reduce((s, a) => s + (a.initBalance || 0), 0);
  const initCap  = initCash + (settings.initUSDT * settings.initUSDTCost);
  const totalCash= accounts.reduce((s, a) => s + a.balance, 0);
  if (initCap > 0) {
    document.getElementById('s-opening-display').innerHTML =
      `✅ رأس المال الابتدائي: <strong style="color:var(--gold)">${fmt(initCap)} SDG</strong> | إجمالي النقدية الحالية: <strong style="color:var(--green)">${fmt(totalCash)} SDG</strong>`;
  }
}
function saveInitCash() {
  const v = parseFloat(document.getElementById('s-init-cash').value) || 0;
  const a = accounts[0]; if (!a) return;
  a.balance += v - (a.initBalance || 0);   // الفرق يتضاف أو يتخصم من الرصيد الحالي
  a.initBalance = v;
  save(); loadSettingsForm(); updateHeader();
  notify('✅ تم حفظ الرصيد الابتدائي للخزينة');
}
function saveOpeningBalance() {
  const initUSDT     = parseFloat(document.getElementById('s-init-usdt').value) || 0;
  const initUSDTCost = parseFloat(document.getElementById('s-init-usdt-cost').value) || 0;
  settings.initUSDT     = initUSDT;
  settings.initUSDTCost = initUSDTCost;
  if (initUSDT > 0 && initUSDTCost > 0 && inventory.amount === 0) {
    inventory.amount    = initUSDT;
    inventory.totalCost = initUSDT * initUSDTCost;
  }
  save(); loadSettingsForm(); updateHeader();
  notify('✅ تم حفظ البيانات الابتدائية');
}
function saveDefaultMargin() {
  const m = parseFloat(document.getElementById('s-default-margin').value);
  if (!m || m <= 0) { notify('⚠️ أدخل نسبة صحيحة', 'var(--gold)'); return; }
  settings.defaultMargin = m;
  save();
  notify('✅ تم حفظ هامش الربح الافتراضي: ' + m + '%');
}

// ==================== HEADER ====================
function updateHeader() {
  const totalProfit = trades.filter(t => t.type === 'sell').reduce((s, t) => s + t.profit, 0);
  const hc = document.getElementById('h-cash');
  if (hc) hc.textContent = fmt(accounts.reduce((x, acc) => x + acc.balance, 0)) + ' SDG';
  document.getElementById('h-stock').textContent  = fmt(inventory.amount, 2) + ' USDT';
  document.getElementById('h-profit').textContent = fmt(totalProfit) + ' SDG';
  document.getElementById('h-trades').textContent = trades.length;
}

// ==================== CASH MANAGEMENT ====================

function renderCashTab() {
  // Refresh account selects
  populateAccountSelects();

  // Accounts banner in cash tab
  const banner = document.getElementById('cash-accounts-banner');
  if (banner) {
    let html = accounts.map(a => `
      <div class="account-card ${a.id==='bk'?'bk':a.id==='faisal'?'faisal':''}" style="${a.id!=='bk'&&a.id!=='faisal'?'border-right:4px solid '+a.color:''}">
        <div class="account-card-label">💳 ${esc(a.type)}</div>
        <div class="account-card-name">${esc(a.name)}</div>
        <div class="account-card-balance" style="${a.id!=='bk'&&a.id!=='faisal'?'color:'+a.color:''}">${fmt(a.balance)} SDG</div>
      </div>
    `).join('');
    const total = accounts.reduce((s,a) => s + a.balance, 0);
    html += `<div class="account-card account-card-total">
      <div class="account-card-label">💰 الإجمالي</div>
      <div class="account-card-name">كل الحسابات</div>
      <div class="account-card-balance">${fmt(total)} SDG</div>
    </div>`;
    banner.innerHTML = html;
  }

  // Render log
  renderCashLog();
}





function clearCashLog() {
  if (!confirm('مسح كامل سجل حركات النقدية؟ لن يتأثر الرصيد الحالي.')) return;
  cashMoves = [];
  save(); renderCashLog();
  notify('🗑 تم مسح السجل');
}


// ==================== CASH MANAGEMENT ====================

function renderCashSection() {
  // mini accounts banner
  const el = document.getElementById('cash-accounts-banner');
  if (el) {
    el.innerHTML = accounts.map(a => `
      <div class="account-card ${a.id==='bk'?'bk':a.id==='faisal'?'faisal':''}" style="${a.id!=='bk'&&a.id!=='faisal'?'border-right:4px solid '+a.color:''}">
        <div class="account-card-label">💳 ${esc(a.type)}</div>
        <div class="account-card-name">${esc(a.name)}</div>
        <div class="account-card-balance" style="${a.id!=='bk'&&a.id!=='faisal'?'color:'+a.color:''}">${fmt(a.balance)} SDG</div>
      </div>
    `).join('') + `
      <div class="account-card account-card-total">
        <div class="account-card-label">💰 الإجمالي</div>
        <div class="account-card-name">كل الحسابات</div>
        <div class="account-card-balance">${fmt(accounts.reduce((s,a)=>s+a.balance,0))} SDG</div>
      </div>`;
  }
  renderCashLog();
}

function updateCashType() { calcCashPreview(); }

function calcCashPreview() {
  const accId  = document.getElementById('cash-account').value;
  const type   = document.getElementById('cash-type').value;
  const amount = parseFloat(document.getElementById('cash-amount').value) || 0;
  const fee    = type === 'withdraw' ? (parseFloat(document.getElementById('cash-fee').value) || 0) : 0;
  const acc    = getAccount(accId);
  // Show/hide fee field
  const feeGroup = document.getElementById('cash-fee-group');
  if (feeGroup) feeGroup.style.display = type === 'withdraw' ? 'flex' : 'none';
  if (!acc) return;
  const total = amount + fee;
  const after = type === 'deposit' ? acc.balance + amount : acc.balance - total;
  document.getElementById('cp-before').textContent = fmt(acc.balance) + ' SDG';
  document.getElementById('cp-amount').textContent = (type==='deposit'?'+':'-') + fmt(amount) + ' SDG';
  document.getElementById('cp-amount').style.color = type==='deposit'?'var(--green)':'var(--red)';
  const feeEl = document.getElementById('cp-fee');
  if (feeEl) { feeEl.textContent = fee > 0 ? '-' + fmt(fee) + ' SDG' : '—'; feeEl.style.color = fee > 0 ? 'var(--red)' : 'var(--text2)'; }
  document.getElementById('cp-after').textContent  = fmt(after) + ' SDG';
  document.getElementById('cp-after').style.color  = after >= 0 ? 'var(--green)' : 'var(--red)';
}

function addCashEntry() {
  const date   = document.getElementById('cash-date').value;
  const accId  = document.getElementById('cash-account').value;
  const type   = document.getElementById('cash-type').value;
  const amount = parseFloat(document.getElementById('cash-amount').value);
  const fee    = type === 'withdraw' ? (parseFloat(document.getElementById('cash-fee').value) || 0) : 0;
  const notes  = document.getElementById('cash-notes').value;
  if (!date || !amount || amount <= 0) { notify('⚠️ أكمل الحقول المطلوبة', 'var(--gold)'); return; }
  const acc = getAccount(accId);
  if (!acc) return;
  const totalDeduct = amount + fee;
  if (type === 'withdraw' && totalDeduct > acc.balance) {
    notify('⚠️ الرصيد غير كافٍ — المتاح: ' + fmt(acc.balance) + ' SDG', 'var(--red)'); return;
  }
  if (type === 'deposit') acc.balance += amount;
  else acc.balance -= totalDeduct;
  const entry = {
    id: Date.now(), date, type, accountId: accId,
    amount, fee, balanceAfter: acc.balance, notes
  };
  cashMoves.push(entry);
  save();
  renderCashSection(); updateHeader();
  if (document.getElementById('tab-dashboard').classList.contains('active')) renderDashboard();
  ['cash-amount','cash-notes','cash-fee'].forEach(id => { const el = document.getElementById(id); if(el) el.value = ''; });
  calcCashPreview();
  const msg = type === 'deposit'
    ? '✅ تم الإيداع: +' + fmt(amount) + ' SDG'
    : '✅ تم السحب: -' + fmt(amount) + ' SDG' + (fee > 0 ? ' (عمولة: ' + fmt(fee) + ' SDG)' : '');
  notify(msg);
}

function calcTransferPreview() {
  const fromId = document.getElementById('transfer-from').value;
  const amount = parseFloat(document.getElementById('transfer-amount').value) || 0;
  const fee    = parseFloat(document.getElementById('transfer-fee').value) || 0;
  const from   = getAccount(fromId);
  const totalDeduct = amount + fee;
  const fromAfter   = from ? from.balance - totalDeduct : 0;
  document.getElementById('tp-amount').textContent       = amount ? fmt(amount) + ' SDG' : '-';
  document.getElementById('tp-fee').textContent          = fee > 0 ? fmt(fee) + ' SDG' : '0 SDG';
  document.getElementById('tp-total-deduct').textContent = totalDeduct ? fmt(totalDeduct) + ' SDG' : '-';
  document.getElementById('tp-received').textContent     = amount ? fmt(amount) + ' SDG' : '-';
  document.getElementById('tp-from-after').textContent   = from ? fmt(fromAfter) + ' SDG' : '-';
  document.getElementById('tp-from-after').style.color   = fromAfter >= 0 ? 'var(--green)' : 'var(--red)';
}

function addTransfer() {
  const date   = document.getElementById('transfer-date').value;
  const fromId = document.getElementById('transfer-from').value;
  const toId   = document.getElementById('transfer-to').value;
  const amount = parseFloat(document.getElementById('transfer-amount').value);
  const fee    = parseFloat(document.getElementById('transfer-fee').value) || 0;
  const notes  = document.getElementById('transfer-notes').value;
  if (!date || !amount || amount <= 0) { notify('⚠️ أكمل الحقول المطلوبة', 'var(--gold)'); return; }
  if (fromId === toId) { notify('⚠️ الحساب المُرسِل والمستقبِل نفس الحساب!', 'var(--red)'); return; }
  const from = getAccount(fromId);
  const to   = getAccount(toId);
  if (!from || !to) return;
  const totalDeduct = amount + fee;
  if (totalDeduct > from.balance) {
    notify('⚠️ الرصيد غير كافٍ — المتاح: ' + fmt(from.balance) + ' SDG', 'var(--red)'); return;
  }
  from.balance -= totalDeduct;
  to.balance   += amount;
  const ts = Date.now();
  // سجل خروج
  cashMoves.push({
    id: ts, date, type: 'transfer_out', accountId: fromId,
    toAccountId: toId, amount, fee, balanceAfter: from.balance,
    notes: notes || ('تحويل إلى ' + to.name)
  });
  // سجل دخول
  cashMoves.push({
    id: ts + 1, date, type: 'transfer_in', accountId: toId,
    fromAccountId: fromId, amount, fee: 0, balanceAfter: to.balance,
    notes: notes || ('تحويل من ' + from.name)
  });
  save();
  renderCashSection(); updateHeader();
  if (document.getElementById('tab-dashboard').classList.contains('active')) renderDashboard();
  ['transfer-amount','transfer-fee','transfer-notes'].forEach(id => document.getElementById(id).value = '');
  calcTransferPreview();
  notify('🔁 تم التحويل: ' + fmt(amount) + ' SDG من ' + from.name + ' إلى ' + to.name + (fee>0?' (عمولة: '+fmt(fee)+' SDG)':''));
}

function renderCashLog() {
  let list    = [...cashMoves];
  const fAcc  = document.getElementById('cash-filter-acc')?.value  || '';
  const fType = document.getElementById('cash-filter-type')?.value || '';
  if (fAcc)  list = list.filter(e => e.accountId === fAcc);
  if (fType) list = list.filter(e => e.type === fType);
  const tbody = document.getElementById('cash-log-table');
  if (!tbody) return;
  if (!list.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty"><div class="empty-icon">💰</div>لا توجد حركات بعد</td></tr>'; return;
  }
  const typeLabel = { deposit:'⬆️ إيداع', withdraw:'⬇️ سحب', transfer_out:'📤 تحويل صادر', transfer_in:'📥 تحويل وارد' };
  const typeColor = { deposit:'var(--green)', withdraw:'var(--red)', transfer_out:'var(--red)', transfer_in:'var(--green)' };
  tbody.innerHTML = [...list].reverse().map((e, i) => `
    <tr>
      <td style="color:var(--text2)">${list.length - i}</td>
      <td>${esc(e.date)}</td>
      <td><span style="color:${typeColor[e.type]};font-weight:700;font-size:12px">${typeLabel[e.type]||e.type}</span></td>
      <td style="color:${typeColor[e.type]};font-weight:700">${e.type==='deposit'||e.type==='transfer_in'?'+':'-'}${fmt(e.amount)}</td>
      <td style="color:var(--red)">${e.fee>0?fmt(e.fee):'-'}</td>
      <td style="color:var(--text2)">${e.balanceAfter == null ? '—' : fmt(e.balanceAfter)}</td>
      <td style="font-size:11px;color:var(--text2)">${esc(e.notes)||'-'}</td>
      <td><button class="btn btn-red btn-xs" onclick="deleteCashEntry(${Number(e.id)})">🗑</button></td>
    </tr>
  `).join('');
}

function deleteCashEntry(id) {
  if (!confirm('حذف هذه الحركة؟ سيتم عكس تأثيرها على الرصيد تلقائياً.')) return;
  const entry = cashMoves.find(e => e.id === id);
  if (entry) {
    const acc = getAccount(entry.accountId);
    if (acc) {
      if (entry.type === 'deposit') {
        // حذف إيداع: نخصم المبلغ من الحساب
        acc.balance -= entry.amount;
      } else if (entry.type === 'withdraw') {
        // حذف سحب: نرجع المبلغ + العمولة للحساب
        acc.balance += entry.amount + (entry.fee || 0);
      } else if (entry.type === 'transfer_out') {
        // حذف تحويل صادر: نرجع المبلغ + العمولة للمُرسِل
        acc.balance += entry.amount + (entry.fee || 0);
        // ونخصم من المستقبِل
        const toAcc = getAccount(entry.toAccountId);
        if (toAcc) toAcc.balance -= entry.amount;
      } else if (entry.type === 'transfer_in') {
        // حذف تحويل وارد: نخصم المبلغ من المستقبِل
        acc.balance -= entry.amount;
        // ونرجع للمُرسِل
        const fromAcc = getAccount(entry.fromAccountId);
        if (fromAcc) fromAcc.balance += entry.amount + (entry.fee || 0);
      }
    }
    // احذف الحركة المقابلة (transfer_in مع transfer_out)
    if (entry.type === 'transfer_out' || entry.type === 'transfer_in') {
      // البحث عن الحركة المقابلة بنفس التاريخ والمبلغ والنوع المعاكس
      const pairedType = entry.type === 'transfer_out' ? 'transfer_in' : 'transfer_out';
      const pairedAccount = entry.type === 'transfer_out' ? entry.toAccountId : entry.fromAccountId;
      const paired = cashMoves.find(e => 
        e.type === pairedType && 
        e.date === entry.date && 
        e.amount === entry.amount &&
        e.accountId === pairedAccount
      );
      const excludeIds = paired ? [id, paired.id] : [id];
      cashMoves = cashMoves.filter(e => !excludeIds.includes(e.id));
    } else {
      cashMoves = cashMoves.filter(e => e.id !== id);
    }
  } else {
    cashMoves = cashMoves.filter(e => e.id !== id);
  }
  save(); renderCashSection(); updateHeader();
  if (document.getElementById('tab-dashboard').classList.contains('active')) renderDashboard();
  notify('🗑 تم حذف الحركة وعكس تأثيرها على الرصيد');
}

// ==================== BACKUP ====================
function exportJSON() {
  const data = { trades, inventory, settings, accounts, cashMoves: cashMoves, exportDate: new Date().toISOString(), version: 4 };
  downloadJSON(data, 'sudacash-backup-v4-' + today() + '.json');
  notify('📤 تم تصدير النسخة الاحتياطية');
}
function importJSON(input) {
  const file = input.files[0]; if (!file) return;
  const reader = new FileReader();
  reader.onload = e => {
    try {
      if (!confirm('سيتم استبدال جميع البيانات الحالية. هل أنت متأكد؟')) return;
      const data = JSON.parse(e.target.result);
      trades    = Array.isArray(data.trades) ? data.trades : [];
      inventory = data.inventory || { amount: 0, totalCost: 0 };
      settings  = data.settings  || { initCash:0, initUSDT:0, initUSDTCost:0, cashBalance:0, defaultMargin:1.5 };
      accounts  = data.accounts  || accounts;
      cashMoves   = Array.isArray(data.cashMoves) ? data.cashMoves : [];
      normalizeAccounts();
      localStorage.setItem('p2p_cash_moves', JSON.stringify(cashMoves));
      save(); populateAccountSelects(); renderDashboard(); updateHeader(); loadSettingsForm();
      notify('✅ تم الاستيراد بنجاح — ' + trades.length + ' صفقة');
    } catch(err) { notify('❌ خطأ في قراءة الملف', 'var(--red)'); }
  };
  reader.readAsText(file); input.value = '';
}
function csvCell(v) { let s = String(v ?? ''); if (/^[=+\-@]/.test(s) && isNaN(Number(s))) s = "'" + s; return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; }
function exportExcel() {
  if (!trades.length) { notify('لا توجد بيانات للتصدير', 'var(--gold)'); return; }
  let csv = '\uFEFF';
  csv += 'التاريخ,النوع,المنصة,الحساب,الكمية,السعر,الإجمالي,العمولة,التكلفة الحقيقية/USDT,الربح,الملاحظات\n';
  trades.forEach(t => {
    const acc = getAccount(t.accountId);
    csv += [t.date, t.type==='buy'?'شراء':'بيع', t.platform, acc?acc.name:'—',
      t.amount, t.price, t.total, t.fee||0, t.realPrice||t.price,
      t.type==='sell'?t.profit:'', t.notes||''].map(csvCell).join(',') + '\n';
  });
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a'); a.href = url;
  a.download = 'sudacash-trades-' + today() + '.csv';
  a.click(); URL.revokeObjectURL(url);
  notify('📥 تم تصدير ملف Excel (CSV)');
}
function resetAll() {
  if (!confirm('حذف جميع البيانات والإعدادات نهائياً؟')) return;
  if (!confirm('تأكيد أخير: هذا لا يمكن التراجع عنه.')) return;
  ['p2p_trades','p2p_inventory','p2p_settings','p2p_accounts','p2p_cash_moves','sc_firebase_cfg','sc_last_synced','sc_dirty'].forEach(k => localStorage.removeItem(k));
  location.reload();
}



// Preserve the original inline HTML handlers while this file is a module.
Object.assign(window, {
  normalizeAccounts,
  syncData,
  today,
  persistLocal,
  save,
  downloadJSON,
  fmt,
  notify,
  getAccount,
  accBadgeHTML,
  populateAccountSelects,
  renderAccountsList,
  addAccount,
  deleteAccount,
  openAccModal,
  closeAccModal,
  saveAccEdit,
  showTab,
  calcBuyTotal,
  calcBuyPrice,
  calcBuyFee,
  calcBuyFeePct,
  updateBuyPreview,
  addBuy,
  prefillSellCost,
  updateSellStock,
  calcSellProfit,
  addSell,
  renderDashboard,
  renderProfitChart,
  setFilter,
  clearFilters,
  renderHistory,
  deriveBaseline,
  replayTrades,
  applyReplayUpdates,
  replayBlockedMsg,
  deleteTrade,
  clearAll,
  openEdit,
  closeModal,
  saveEdit,
  renderInventory,
  adjustInventory,
  calcHelper,
  calcMargin,
  renderAnalytics,
  loadSettingsForm,
  saveInitCash,
  saveOpeningBalance,
  saveDefaultMargin,
  updateHeader,
  renderCashTab,
  clearCashLog,
  renderCashSection,
  updateCashType,
  calcCashPreview,
  addCashEntry,
  calcTransferPreview,
  addTransfer,
  renderCashLog,
  deleteCashEntry,
  exportJSON,
  importJSON,
  exportExcel,
  resetAll,
  toggleMore,
  closeMore,
  _setLocalData,
  esc
});
}
