# 게임 진단 로그 연결

개발 중인 방치형 게임은 플레이어가 직접 내보내는 진단 로그(JSONL, 최근 최대 256건)를 남깁니다. `game_log_pipeline/device.py`는 이 파일의 전투 기록을 파이프라인의 `stage_attempt` 이벤트로 바꿉니다. 바뀐 이벤트는 합성 이벤트와 같은 수집, 격리, 재계산 경로를 탑니다.

## 사용

```sh
python -m game_log_pipeline adapt --input examples/device_diagnostic_sample.jsonl --player tester_01
python -m game_log_pipeline replay --input runs/device/attempts.jsonl --output runs/device
```

`--player`는 내보낸 기기를 가리키는 가명입니다. 게임 로그에는 계정이나 기기 식별자가 없으므로 수집하는 쪽에서 붙입니다. 클러스터에는 같은 JSONL을 `POST /v1/events`로 보내면 됩니다.

`examples/device_diagnostic_sample.jsonl`은 **형식만 같은 합성 샘플**입니다. 실제 기기 로그는 이 저장소에 넣지 않습니다.

## 변환 규칙

| 게임 로그 (`battle_result`) | stage_attempt |
|---|---|
| `tower_floor` > 0 | `track=endless`, `stage=tower_floor` |
| 그 외 | `track=story`, `stage=stage + 1` (게임은 0부터 셈) |
| `won` | `outcome=clear` 또는 `fail` |
| `ticks` | `duration_ms = ticks × 100` (게임 시간, 배속과 무관) |
| `event_id`, `session_id`, `occurred_at` | 그대로 유지 |

같은 파일을 두 번 보내거나 겹치는 구간을 다시 내보내도 `event_id`가 같으므로 재전송으로 처리됩니다.

## 넣지 않는 것

| 제외 대상 | 이유 |
|---|---|
| `assisted` 또는 `session_assisted`가 true인 기록 | 개발용 4배속·테스트 재화를 쓴 판은 밸런스 근거가 아님 |
| 전투 외 기록 (저장, 오프라인 정산, 등용 등) | 클라이언트 로그에는 서버 거래 ID와 보상 권리 ID가 없어 중복 지급 규칙에 쓰면 안 됨 |
| 깨진 JSON, 계약 위반 | 사유별로 개수만 보고 |

검증 단계도 같은 경계를 지킵니다. `device_diagnostic` 출처는 `stage_attempt`에만 허용되고 재화 이벤트로 들어오면 격리됩니다.

## 한계

- 게임은 최근 256건만 보관하므로 파일 하나가 전체 이력은 아닙니다.
- 기기 시계 변경, 강제 종료, 보관 한도로 누락될 수 있습니다. 결과는 밸런스 신호이지 이용자 판정이 아닙니다.
- 자동 서버 전송은 없습니다. 출시 후 수집 경로(비공개 수집 API, 보관 정책)는 별도로 정합니다.
