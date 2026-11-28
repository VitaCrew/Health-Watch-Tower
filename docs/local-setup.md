# 로컬 실행 준비

Docker Compose와 `openhuman-runtime/` 소스가 필요합니다. 아래 명령은 해당 소스의 루트에서 PowerShell로 실행합니다.

## 환경 설정

`target/avatar-docker.env`가 이미 있다면 기존 설정을 사용합니다. 처음 실행할 때는 예제 파일을 복사합니다.

```powershell
if (!(Test-Path target/avatar-docker.env)) {
    New-Item -ItemType Directory -Path target -Force | Out-Null
    Copy-Item docs/avatar-mvp-compose.env.example target/avatar-docker.env
}
```

새 파일에서 예제 값 여섯 개를 바꿉니다.

| 환경변수 | 설정할 값 |
| --- | --- |
| `WATCH_TOWER_DB_PASSWORD` | 로컬 PostgreSQL 비밀번호 |
| `AVATAR_HSM_READ_TOKEN` | HSM 읽기용 토큰 |
| `AVATAR_HSM_CONTROL_TOKEN` | HSM 제어용 토큰 |
| `FITNESS_OWNER_ID` | 운동 기록 소유자 ID |
| `OPENHUMAN_DOCKER_CORE_TOKEN` | 32자 이상의 URL 안전 문자로 만든 임의 토큰 |
| `OPENHUMAN_DOCKER_MASTER_KEY` | 임의의 32바이트를 16진수로 표현한 64자리 키 |

각 토큰에는 서로 다른 임의 값을 사용합니다. 설정을 마치면 [Quick Start](../README.md#로컬에서-실행)에서 플랫폼을 기동합니다.

## OpenHuman 연결

OpenHuman 클라이언트의 자체 호스팅 설정에는 **코어 주소** `http://127.0.0.1:17790`과 환경 파일의 `OPENHUMAN_DOCKER_CORE_TOKEN` 값을 입력합니다. OpenHuman 코어는 Compose 네트워크의 Watch Tower MCP에 연결합니다. 호스트에서 사용하는 Watch Tower MCP 주소는 `http://127.0.0.1:17842/mcp`입니다.

## 개인 기록 접근 권한

OpenHuman 계정을 Watch Tower 사용자와 연결하고, 필요한 읽기·쓰기 동의를 등록합니다. 해당 사용자의 OpenFGA 관계도 설정합니다. 환경변수, 사용자 연결 및 권한 설정 명령은 확보한 런타임 소스의 `openhuman-runtime/docs/avatar-mvp.md`에서 확인할 수 있습니다.

## 실행 상태 확인과 종료

```powershell
docker compose --env-file target/avatar-docker.env -f docker-compose.avatar-mvp.yml ps
docker compose --env-file target/avatar-docker.env -f docker-compose.avatar-mvp.yml stop
```
