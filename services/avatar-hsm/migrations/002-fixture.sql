INSERT INTO avatar_hsm.subject VALUES ('synthetic-mvp', 'Asia/Seoul')
ON CONFLICT DO NOTHING;

INSERT INTO avatar_hsm.product VALUES
    ('SYNTH-A10', '시험품목 알파정 10mg', 'synthetic-mfds-fixture'),
    ('SYNTH-A20', '시험품목 알파정 20mg', 'synthetic-mfds-fixture'),
    ('SYNTH-B10', '시험품목 베타정 10mg', 'synthetic-mfds-fixture'),
    ('SYNTH-C10', '시험품목 감마정 10mg', 'synthetic-mfds-fixture'),
    ('SYNTH-D10', '시험품목 델타정 10mg', 'synthetic-mfds-fixture')
ON CONFLICT DO NOTHING;

INSERT INTO avatar_hsm.dur_coverage VALUES
    ('SYNTH-A10', 'synthetic-mfds-v1', 'available'),
    ('SYNTH-A20', 'synthetic-mfds-v1', 'available'),
    ('SYNTH-B10', 'synthetic-mfds-v1', 'available'),
    ('SYNTH-C10', 'synthetic-mfds-v1', 'available'),
    ('SYNTH-D10', 'synthetic-mfds-v1', 'failed')
ON CONFLICT DO NOTHING;

INSERT INTO avatar_hsm.dur_pair VALUES
    ('SYNTH-A10', 'SYNTH-B10', '합성 시험 데이터의 병용금기 사유',
     'https://www.data.go.kr/data/15059486/openapi.do')
ON CONFLICT DO NOTHING;

INSERT INTO avatar_hsm.regimen (subject_id, item_seq, effective_from)
VALUES ('synthetic-mvp', 'SYNTH-A10', now() - interval '1 day')
ON CONFLICT DO NOTHING;

INSERT INTO avatar_hsm.dose (id, subject_id, item_seq, scheduled_at, eligible_at, expires_at)
VALUES ('synthetic-dose-001', 'synthetic-mvp', 'SYNTH-A10',
        now() - interval '20 minutes', now() - interval '5 minutes', now() + interval '1 day')
ON CONFLICT DO NOTHING;
