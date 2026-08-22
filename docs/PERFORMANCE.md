# 성능과 자원 정책

이 문서는 현재 기본값을 선택한 근거만 보존한다. 수치는 2026-08-19~20 당시
release 빌드와 로컬 NVMe 환경의 중앙값 또는 대표값이며, 다른 장치에서 같은 절대
성능을 보장하지 않는다.

## 결론

| 항목 | 현재 정책 | 측정에서 확인한 trade-off |
| --- | --- | --- |
| Batch | 기본 5,000행 | 50,000행은 CSV 속도 이득 없이 RSS 약 2배 |
| CSV worker | 기본 1 | 3–4 worker는 wall 개선, RSS 최대 약 7.8배 |
| Parquet worker | adaptive 최대 3 | shape에 따라 약 15% 개선 또는 약 3% 열화 |
| Parquet batch | 파일별 metadata cap | wide row의 큰 요청 batch 메모리 제한 |
| Encoding | fixed RLE 기본 | adaptive는 출력 약 11% 감소, wall 4–7% 증가 |
| DataFrame | 한 in-memory batch | 편리하지만 bounded-memory 경로가 아님 |
| Row-key index | Parquet sidecar | NDJSON 대비 생성 약 33% 단축, 크기 약 86% 감소 |

## CSV 병렬화

대표 shape에서 worker를 늘렸을 때의 결과다.

| 입력 | Worker 1 | 선택 병렬 | Wall 변화 | Peak RSS 변화 |
| --- | ---: | ---: | ---: | ---: |
| 1M행·8컬럼 | 0.322초 / 24MB | W4 0.176초 / 119MB | -45.3% | 약 5.0배 |
| 100K행·64컬럼 | 0.658초 / 58MB | W4 0.330초 / 428MB | -49.8% | 약 7.4배 |
| 긴 quoted record | 0.452초 / 45MB | W3 0.360초 / 350MB | -20.4% | 약 7.8배 |

CSV 병렬화는 처리량을 높이지만 worker별 decode buffer와 fragment 때문에 CPU와
메모리를 더 사용한다. 자원 효율이 우선이면 worker 1을 유지한다.

## Parquet adaptive worker

Parquet은 row-group 수, 전체 uncompressed bytes와 평균 row-group 크기를 보고 실제
worker 수를 낮춘다.

| 입력 shape | Worker 1 | Worker 3 | 판정 |
| --- | ---: | ---: | --- |
| 300K행·147 row-group | 2.304초 | 1.949초 | W3 약 15.4% 개선 |
| 900K행·900 row-group | 6.099초 | 6.286초 | W3 약 3.1% 열화 |
| 40K행·610컬럼·1 row-group | 0.523초 | 1.060초 | direct fallback 필요 |

따라서 `workers=3`은 항상 세 worker를 쓰라는 의미가 아니라 최대 허용치다.
`adaptive_workers=True`가 기본이고 실제 값은 `convert_with_result()`의
`effective_workers`에서 확인한다.

## 파일별 Parquet batch cap

요청 `batch_size`는 최대 행 수다. 각 파일의 metadata에서 대략적인 row byte 폭을
계산하고, decoded batch가 목표 byte 크기를 크게 넘지 않도록 파일별 cap을 낮춘다.
여러 파일의 cap은 `effective_batch_sizes`에 입력 순서대로 기록한다.

이 통계는 기존 metadata 계획 값을 반환할 뿐 Parquet를 다시 읽지 않는다. 추가
메모리는 파일당 정수 하나 수준이다.

## CSV와 Parquet

동일한 1,000,000행·8컬럼 데이터의 과거 대표 측정에서는 CSV가 약 0.532초,
Parquet 복사 최적화 후 경로가 약 0.408초였다. Parquet은 schema가 이미 있고 입력
파일도 더 작지만, 두 형식 모두 최종적으로 SBDF encoding 비용을 부담한다.

이 값은 revision과 cache 조건이 완전히 같은 공식 benchmark가 아니므로 절대 비교가
아니라 방향성으로만 사용한다.

## Adaptive encoding

반복이 적은 값은 plain, 반복이 많은 값은 RLE를 선택하면 대표 입력에서 출력 크기가
약 11% 줄었지만 정확한 크기 계획 비용 때문에 wall time이 약 4–7% 늘었다.

- 처리량 우선: 기본 `adaptive_encoding=False`
- 저장 공간 우선: `adaptive_encoding=True`

## DataFrame

500K행·4컬럼 pandas 기준 과거 측정은 약 2.071초, 추가 peak 약 225MB였다. DataFrame
경로는 원본 frame 외에 Python list와 SBDF 입력 buffer를 만들기 때문에 대용량 입력의
메모리 상한이 중요하면 파일 기반 CSV·Parquet API를 사용한다.

## 비교할 때 지킬 조건

- 같은 source revision과 release build 사용
- 같은 입력 파일, batch, worker와 encoding 사용
- 같은 저장장치와 cache 조건 사용
- wall time뿐 아니라 CPU, peak RSS와 임시 disk I/O 기록
- schema, row 순서, null과 출력 reader parity 확인
- 한 번의 peak와 반복 실행에서 계속 증가하는 RSS를 구분

새 측정 결과가 현재 기본 정책을 바꿀 정도라면 이 문서의 숫자를 계속 덧붙이지 말고,
기존 표를 같은 조건의 최신 결과로 교체한다.
