# Health Watch Tower

**by Vitacrew · 나를 위한 AI 건강팀을 만드는 사람들**

## Overview

**Health Watch Tower는 내가 쓰는 AI에 내 건강 기록을 연결하는 MCP 기반 프레임워크입니다.** 웨어러블의 수면·활동 기록, 식사·수분·복약 기록, 운동 계획과 실제 수행 기록을 관리하고, 호환되는 AI가 내가 허용한 범위에서 필요한 기록과 도구에 접근할 수 있게 합니다.

**Vitacrew 팀은 Bryan Johnson의 Blueprint에서 영감을 받아 이 프로젝트를 시작했습니다.** 한 사람의 기록을 꾸준히 살피고 그 사람에게 맞춰 루틴을 조정하는 전담 건강관리 경험을, 누구나 이미 가진 기록으로 시작할 수 있게 만드는 것이 목표입니다.

> “최근 2주 동안 운동 계획을 얼마나 지켰어? 수면이 짧았던 날의 운동 기록도 같이 보고, 다음 주에 뭘 조정하면 좋을지 알려줘.”

Vitacrew 팀은 이처럼 내 기록을 바탕으로 변화를 살피고, 근거를 확인하고, 다음 행동과 점검 일정을 정하는 AI 건강팀의 흐름을 개발하고 있습니다.

## Health Watch Tower가 하는 일

| 영역 | 기능 |
| --- | --- |
| **건강 기록** | 웨어러블에서 수면·활동 기록을 가져오고, 식사·수분 일지와 복약 상태를 관리합니다. |
| **운동 계획과 수행** | 훈련 계획, 실제 세트 기록과 개인 최고 기록을 관리하고 계획과 수행을 연결합니다. |
| **AI 연결** | 호환되는 AI가 공통 MCP 엔드포인트에서 사용자 동의 범위에 맞춰 기록과 도구에 접근합니다. |
| **후속 확인** | 특정 기록에 대한 검토 요청을 남깁니다. 일정과 알림, 사용자 확인은 OpenHuman이 담당합니다. |

## AI 건강팀 설계

질문에 따라 코디네이터가 주 담당과 추가로 필요한 역할을 정합니다. 데이터 분석은 기록에서 달라진 점과 빠진 데이터를 확인하고, 근거 검토는 현재 자료로 어디까지 설명할 수 있는지 살핍니다. 코칭은 사용자 목표와 생활 여건을 바탕으로 다음 행동과 다시 확인할 시점을 함께 정합니다.

## 현재 개발 상태

Health Watch Tower의 통합 MCP 도구 호출과 읽기·쓰기 권한은 로컬 환경에서 검증했습니다. 외부 AI 클라이언트의 원격 인증과 실제 웨어러블 계정·기기 연동은 추가 검증이 필요합니다. 질문에 따라 역할을 조율하고 후속 점검까지 이어지는 AI 건강팀 워크플로는 개발 중입니다.

## 참고 자료

- [Bryan Johnson · Blueprint Protocol](https://blueprint.bryanjohnson.com/blogs/news/bryan-johnsons-protocol)
- README의 Overview 구성 참고. [Google Health · Personal Health Agent Framework](https://github.com/Google-Health/consumer-health-research/tree/main/personal_health_agent)
