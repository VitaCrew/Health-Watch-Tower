self.addEventListener('push', event => {
  event.waitUntil(self.registration.showNotification('안부 확인이 필요합니다', {
    body: '알림을 열어 확인해 주세요.',
    tag: event.data?.json().event_id,
  }));
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil((async () => {
    const windows = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    const url = event.notification.tag ? `/?event=${encodeURIComponent(event.notification.tag)}` : '/';
    const existing = windows.find(client => new URL(client.url).pathname === '/');
    if (existing && existing.navigate) await existing.navigate(url);
    if (existing) return existing.focus();
    return clients.openWindow(url);
  })());
});
