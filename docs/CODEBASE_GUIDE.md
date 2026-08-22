# 코드베이스 가이드

이 문서는 사람과 LLM이 `smoking-sbdf`의 현재 구조를 빠르게 이해하고, 공개 계약을
깨뜨리지 않고 수정할 수 있도록 작성되었다. 과거 구현 계획이 아니라 현재 source를
기준으로 한다.

## 프로젝트 한 문장 요약

`smoking-sbdf`는 Python에서 CSV, Parquet와 DataFrame을 받아 Rust로 Spotfire SBDF
1.0 파일을 기록하는 로컬 변환 패키지다. 선택적으로 row key와 SBDF slice byte
범위를 연결하는 Parquet sidecar도 만든다.

## 기술 스택과 경계

```text
사용자 또는 상위 런타임
        │
        ▼
Python API / CLI
src/smoking_sbdf/
        │  경로·옵션 정규화, 형식 dispatch, sidecar 요청
        ▼
PyO3 native module
src/lib.rs
        │  Arrow/CSV decode, worker 계획, bounded batch, 통계 집계
        ├──────────────► src/rust_sbdf.rs ──► SBDF 파일
        └──────────────► src/sbdf_index.rs ─► Parquet sidecar
```

- Python은 사용하기 쉬운 API와 경로 처리를 담당한다.
- Rust는 데이터 decode, 타입 변환, 병렬 실행과 SBDF 직렬화를 담당한다.
- 현재 C writer나 C FFI는 사용하지 않는다.
- pandas와 Polars는 선택 dependency이며 패키지가 자동 설치하지 않는다.

## 먼저 읽을 파일

| 순서 | 파일 | 역할 |
| ---: | --- | --- |
| 1 | `README.md` | 기능, 기본 정책과 프로젝트 범위 |
| 2 | `USAGE.md` | 외부 사용자가 호출하는 API와 옵션 |
| 3 | `src/smoking_sbdf/__init__.py` | Python 공개 API와 형식 dispatch |
| 4 | `src/lib.rs` | Rust/PyO3 진입점과 전체 변환 orchestration |
| 5 | `src/rust_sbdf.rs` | SBDF preamble, slice, column encoding과 end marker |
| 6 | `src/sbdf_index.rs` | 완성된 SBDF를 읽어 Parquet sidecar 생성 |

CSV 병렬 경계를 수정할 때는 `src/csv_spans.rs`와 `src/native_csv.rs`를 추가로 읽는다.
컬럼 타입 자동 규칙을 수정할 때는 `src/type_rules/mod.rs`를 읽는다.

## 파일별 책임

### `src/smoking_sbdf/__init__.py`

- `convert()`와 `convert_with_result()` 형식 자동 판별
- CSV, 단일 Parquet, dataset, manifest helper 제공
- worker와 sidecar 옵션 검증
- pandas·Polars 스타일 DataFrame 변환
- `SbdfConversionResult` 조립
- `DataFrame.to_sbdf()` opt-in 등록

이 파일에서 payload loop를 새로 구현하지 않는다. 데이터 처리 비용이 큰 작업은 Rust
경계 아래에 둔다.

### `src/smoking_sbdf/cli.py`

- `smoking-sbdf convert`
- `smoking-sbdf init`
- `-h`, `--help`, `-help`
- CLI 값을 Python `convert()`에 전달

CLI는 현재 sidecar 옵션과 구조화된 결과 출력을 제공하지 않는다. 이 기능이 필요하면
Python API를 사용한다.

### `src/lib.rs`

한 파일 안에 다음 계층이 함께 있다.

- Python/Arrow 값을 SBDF 타입 buffer로 변환
- Parquet metadata에서 effective batch cap 계산
- Parquet adaptive worker 선택
- CSV sequential 또는 mmap parallel 실행
- Parquet sequential 또는 row-group parallel 실행
- 임시 fragment의 순서 보장과 atomic publish
- 실제 worker, batch cap, row와 slice 통계 반환
- PyO3 함수와 `StreamingSbdfWriter` 노출

크기가 크지만 데이터 경로 사이에서 공유하는 타입·writer 계약이 많다. 분리할 때는
PyO3 함수, 실행 계획, serializer 입력 경계를 먼저 명확히 해야 한다.

### `src/rust_sbdf.rs`

SBDF 1.0 wire format을 Rust로 기록한다.

- table metadata preamble
- table slice와 column slice
- plain, RLE, bit encoding
- planned layout와 direct sink
- table end marker

포맷 호환성에 직접 영향을 주므로 section ID, 길이 encoding, epoch 변환과 invalid
bitmap 의미를 임의로 바꾸면 안 된다.

### `src/csv_spans.rs`

mmap CSV를 worker에 나눌 안전한 byte span을 계산한다. quoted newline, escaped quote,
마지막 newline 부재를 고려한다. 단순 줄바꿈 검색으로 교체하면 CSV record가 잘릴 수
있다.

### `src/native_csv.rs`

wide CSV에서 Arrow object overhead를 줄이기 위한 typed column buffer다. narrow
schema는 Arrow CSV decoder가 더 빠를 수 있어 현재 컬럼 수에 따라 경로를 선택한다.

### `src/sbdf_index.rs`

완성된 SBDF를 순차로 읽고 지정된 row-key 컬럼만 decode한다. 각 SBDF table slice마다
Parquet row group 하나를 기록하며 다음 정보를 보존한다.

- table ID와 SBDF 파일명
- 전체 row index와 slice 내부 row index
- slice 시작 행과 행 수
- SBDF byte offset과 byte length

sidecar는 조회 인덱스이지 SBDF 본문을 대신하지 않는다.

## 공개 Python 계약

### 안정적으로 유지할 API

- `convert(...) -> Path`
- `convert_with_result(...) -> SbdfConversionResult`
- `csv_to_sbdf_streaming(...) -> Path`
- `parquet_to_sbdf_streaming(...) -> Path`
- `parquet_files_to_sbdf_streaming(...) -> Path`
- `parquet_dataset_to_sbdf_streaming(...) -> Path`
- `parquet_manifest_to_sbdf_streaming(...) -> Path`
- `dataframe_to_sbdf(...) -> Path`
- `generate_sbdf_sidecar(...) -> Path`

기존 `Path` 반환 함수를 구조화된 결과로 직접 바꾸지 않는다. 상위 런타임이 실행
통계를 필요로 하면 `convert_with_result()`를 사용한다.

### `SbdfConversionResult`

`input_files[n]`과 `effective_batch_sizes[n]`은 같은 파일을 가리킨다. Parquet cap은
각 파일의 metadata에 있는 전체 행 수와 uncompressed byte 수로 계산하므로 파일마다
다를 수 있다.

통계 집계는 실행 중 수행한다. 통계를 위해 입력이나 SBDF를 다시 스캔하는 구현으로
되돌리지 않는다.

### DataFrame monkey patch

`import smoking_sbdf`는 pandas나 Polars 클래스를 변경하지 않는다. 사용자가
`install_dataframe_methods()`를 명시적으로 호출한 경우에만 `DataFrame.to_sbdf()`를
등록한다. Smoking Data 같은 상위 런타임에서는 함수 API를 우선한다.

## 변환 흐름

### CSV

```text
schema sample 추론
  ├─ workers=1 ──► sequential decoder ──► direct buffered writer
  └─ workers>1 ──► mmap safe spans ──► parallel fragments ──► ordered merge
                                         │
                                         └─ mmap 실패 시 sequential fallback
```

CSV의 `effective_batch_sizes`는 요청 cap 하나다. parallel 경로가 처리 중 span 목표
크기를 낮출 수 있지만 개별 decode batch는 이 cap을 넘지 않는다.

### Parquet

```text
입력 파일 목록 확정
  ──► 파일별 metadata와 strict schema 검사
  ──► 파일별 effective batch cap 계산
  ──► adaptive effective worker 선택
  ├─ worker=1 ──► bounded RecordBatch decode ──► direct writer
  └─ worker>1 ──► row-group task ──► parallel fragments ──► ordered merge
```

파일 목록 순서가 SBDF 행 순서다. dataset은 경로를 정렬하고, manifest는 기재된 순서를
사용한다.

### Sidecar

SBDF publish가 성공한 뒤 sidecar를 생성한다. sidecar 생성이 실패하면 이미 완성된
SBDF는 유지하되, byte range가 맞지 않을 수 있는 기존 sidecar는 제거한다.

## 지켜야 할 불변 조건

- 모든 column slice는 같은 table slice 안에서 row count가 같아야 한다.
- 여러 Parquet 파일은 첫 파일 schema와 strict match해야 한다.
- worker가 달라도 입력 행 순서는 바뀌지 않아야 한다.
- 최종 파일은 target과 같은 파일시스템의 partial을 완성한 뒤 publish한다.
- 실패한 변환은 불완전한 최종 파일을 노출하지 않아야 한다.
- sidecar row group과 SBDF table slice는 1:1이어야 한다.
- row key 기준 전역 정렬이나 동적 slice 재편성은 하지 않는다.
- `effective_workers`는 요청값이 아니라 실제 실행 경로를 기록해야 한다.

## 기본 자원 정책

- 기본 batch cap: 5,000행
- CSV 기본 worker: 1
- Parquet 기본 요청 worker: 3
- Parquet는 metadata 기반 adaptive worker와 파일별 batch cap 사용
- adaptive encoding은 출력 크기 우선 opt-in
- DataFrame 경로는 한 in-memory batch이며 bounded-memory가 아님

수치 근거는 [PERFORMANCE.md](PERFORMANCE.md)에 요약한다.

## 변경별 확인 범위

| 변경 | 최소 확인 |
| --- | --- |
| Python API | `maturin develop`, Python API 테스트, 반환형과 오류 확인 |
| CSV parser/span | quoted newline, escaped quote, 마지막 newline, worker parity |
| Parquet 계획 | 파일 순서, schema mismatch, row-group, 파일별 batch cap |
| serializer | Rust 단위 테스트, row/null/type parity, 기존 SBDF reader 확인 |
| sidecar | key dtype, row 좌표, slice byte range, stale sidecar 제거 |
| CLI | `--help`, `-help`, exit code, 옵션 전달 |

기본 검사 명령은 [BUILD.md](../BUILD.md)에 있다.

## 의도적으로 제공하지 않는 것

- PyPI 배포
- 정기 릴리스나 장기 호환성 보장
- S3 직접 읽기와 Range GET 조회 엔진
- sidecar query API
- DataFrame chunk streaming
- row-key 전역 정렬
- 모든 CSV dialect 옵션의 공개 노출

범위를 넓힐 때는 먼저 README의 프로젝트 범위와 Python 공개 계약을 함께 갱신한다.
