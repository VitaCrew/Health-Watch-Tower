const element = id => document.getElementById(id);
const result = element('result');
async function rpc(surface, tool, args = {}) {
  const key = element(surface).value.trim();
  if (!key) throw new Error('이 작업에 사용할 키를 입력해 주세요.');
  const response = await fetch('/mcp/wearable-' + surface, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + key },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call',
      params: { name: tool, arguments: { provider: element('provider').value, ...args } } }),
  });
  if (!response.ok) throw new Error('키나 서버 연결을 확인해 주세요.');
  const { result } = await response.json();
  if (!result || result.isError) throw new Error(result?.content?.[0]?.text || '연결을 완료하지 못했어요.');
  return result.structuredContent;
}
function bind(id, action) {
  const button = element(id);
  button.addEventListener('click', async () => {
    button.disabled = true;
    try {
      result.textContent = await action() || '';
    } catch (error) {
      result.textContent = error.message;
    } finally {
      button.disabled = false;
    }
  });
}
bind('connect', async () => {
  const { authorization_url } = await rpc('control', 'connect_cloud_oauth');
  location.assign(authorization_url);
});
bind('status', async () => {
  const state = await rpc('read', 'cloud_device_status');
  const batch = state.garmin_backfill?.requests;
  if (batch?.length) {
    const labels = { queued: '요청 대기', accepted: 'Garmin 접수', duplicate: '이미 요청됨', failed: '요청 실패', permission_removed: '권한 철회' };
    const counts = {};
    for (const item of batch) counts[item.status] = (counts[item.status] || 0) + 1;
    const summary = Object.entries(counts).map(([status, count]) => `${labels[status] || status} ${count}종`).join(' · ');
    const last = state.last_synced_at ? ` · 최근 데이터 수신 ${new Date(state.last_synced_at).toLocaleString('ko-KR')}` : '';
    return summary + last;
  }
  if (state.schedule?.last_error) return '동기화에 실패했어요. 연결 권한을 확인해 주세요.';
  if (state.schedule?.backfill_remaining_to) return '과거 기록을 가져오고 있어요.';
  if (state.last_synced_at) return '최근 동기화 ' + new Date(state.last_synced_at).toLocaleString('ko-KR');
  return state.oauth_available ? '계정을 연결할 수 있어요.' : '서버에서 제조사 연결을 설정해야 해요.';
});
bind('sync', async () => {
  const start = new Date(element('start').value + 'T00:00:00');
  const end = new Date(element('end').value + 'T00:00:00');
  if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime())) {
    throw new Error('시작 날짜와 마지막 날짜를 선택해 주세요.');
  }
  end.setDate(end.getDate() + 1);
  const until = new Date(Math.min(Date.now(), end.getTime()));
  const request = await rpc('control', 'schedule_cloud_sync', { start: start.toISOString(), end: until.toISOString() });
  if (element('provider').value === 'garmin') return request.state === 'already_requested' ? '같은 기간을 이미 요청했어요.' : `Garmin 과거 기록 ${request.feeds_added}종의 요청을 예약했어요.`;
  return '과거 기록 가져오기를 예약했어요.';
});
bind('disconnect', async () => {
  if (!confirm('HSM에 가져온 이 제조사의 기록을 삭제할까요?')) return '';
  await rpc('control', 'revoke_cloud_device');
  return '연결을 해제하고 가져온 기록을 삭제했어요.';
});
