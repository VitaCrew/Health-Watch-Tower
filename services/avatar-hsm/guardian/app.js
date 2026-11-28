const byId = id => document.getElementById(id);
const status = byId('status');
const input = byId('code');
const subscribe = byId('subscribe');
const confirm = byId('confirm');
let code = localStorage.getItem('avatar.guardian.code') || '';
let currentAlert;
const event = new URL(location.href).searchParams.get('event');
const alertUrl = event ? `/api/alerts?event=${encodeURIComponent(event)}` : '/api/alerts';

async function request(path, token, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { Authorization: `Bearer ${token}`, ...options.headers },
  });
  if (!response.ok) throw new Error('요청을 처리하지 못했습니다.');
  return response.status === 204 ? undefined : response.json();
}

function show(alert) {
  currentAlert = alert;
  byId('alert').hidden = !alert;
  if (!alert) return;
  byId('when').textContent = `합성 낙상 시험 · ${new Date(alert.created_at).toLocaleString('ko-KR')}`;
  confirm.disabled = Boolean(alert.confirmed_at);
  confirm.textContent = alert.confirmed_at ? '확인했습니다' : '제가 확인할게요';
}

async function refresh() {
  if (!code) return;
  try {
    const { alert } = await request(alertUrl, code);
    show(alert);
  } catch {
    status.textContent = '연결 또는 등록 코드를 확인해 주세요.';
  }
}

subscribe.addEventListener('click', async () => {
  const entered = input.value.trim() || code;
  if (!entered) { input.focus(); return; }
  subscribe.disabled = true;
  try {
    if (!window.isSecureContext || !('Notification' in window) || !('serviceWorker' in navigator)) {
      throw new Error('HTTPS에서 열어 주세요. 아이폰은 홈 화면에 추가한 뒤 실행해 주세요.');
    }
    if (await Notification.requestPermission() !== 'granted') {
      throw new Error('기기 설정에서 알림을 허용해 주세요.');
    }
    const data = await request(alertUrl, entered);
    await navigator.serviceWorker.register('/sw.js');
    const ready = await navigator.serviceWorker.ready;
    const push = await ready.pushManager.getSubscription() || await ready.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: Uint8Array.from(
        atob(data.vapid_public_key.replaceAll('-', '+').replaceAll('_', '/')),
        value => value.charCodeAt(0),
      ),
    });
    const { endpoint, keys } = push.toJSON();
    await request('/api/subscription', entered, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ endpoint, keys }),
    });
    code = entered;
    localStorage.setItem('avatar.guardian.code', code);
    input.value = '';
    show(data.alert);
    status.textContent = '알림을 받을 수 있어요.';
  } catch (error) {
    status.textContent = error.message;
  } finally {
    subscribe.disabled = false;
  }
});

confirm.addEventListener('click', async () => {
  const alert = currentAlert;
  if (!alert || alert.confirmed_at || confirm.disabled) return;
  confirm.disabled = true;
  try {
    await request(`/api/alerts/${encodeURIComponent(alert.event_id)}/confirm`, code, { method: 'POST' });
    show({ ...alert, confirmed_at: new Date().toISOString() });
    status.textContent = '확인 응답을 보냈습니다.';
  } catch {
    confirm.disabled = false;
    status.textContent = '확인 응답을 보내지 못했습니다. 다시 눌러 주세요.';
  }
});

window.addEventListener('focus', () => void refresh());
void refresh();
