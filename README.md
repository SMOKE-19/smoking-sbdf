# `smoking-sbdf`

`smoking-sbdf`는 CSV, Parquet, Parquet dataset과 Python DataFrame을 Spotfire SBDF 1.0 파일로 변환하는 Rust 기반 Python 패키지다.

- 배포 패키지: `smoking-sbdf`
- Python import: `smoking_sbdf`
- CLI: `smoking-sbdf`
- 지원 Python: CPython 3.10–3.14
- 지원 플랫폼: Linux, Windows

v0.1.5의 핵심 변환 기능과 패키징은 완료되었으며 공개 API와 기본 정책을 이 문서에 정리한다.

## 제공 기능

- Rust 단독 SBDF header·metadata·table slice·end marker 직렬화
- CSV schema 추론과 bounded streaming 변환
- Parquet 파일·파일 목록·dataset·manifest 변환
- pandas·Polars DataFrame 함수 API와 `DataFrame.to_sbdf()`
- 단일 worker direct writer와 명시적 병렬 worker
- Parquet metadata 기반 adaptive worker 선택
- plain/RLE 및 opt-in adaptive encoding
- 기존 target을 보존하는 target-local partial과 atomic publish
- row key와 SBDF slice byte range를 연결하는 Parquet sidecar

vendored C writer와 C FFI는 사용하지 않는다. Python은 경로와 DataFrame 값을 정규화하는 얇은 래퍼이며 변환과 직렬화는 Rust에서 수행한다.

## 빠른 시작

사용 중인 CPython 버전과 플랫폼에 맞는 wheel을
[`v0.1.5` GitHub Release](https://github.com/SMOKE-19/smoking-sbdf/releases/tag/v0.1.5)에서
다운로드한 뒤 설치한다.

```bash
python -m pip install ./smoking_sbdf-0.1.5-<python-tag>-<platform-tag>.whl
```

예를 들어 CPython 3.14 Linux x86_64에서는 `cp314`·`manylinux` wheel을
사용한다. 현재 PyPI 인덱스에는 게시하지 않았다.

Python에서는 입력 형식을 자동 판별하는 `convert()`가 기본 진입점이다.

```python
from smoking_sbdf import convert

convert("input.parquet", "output.sbdf")
```

CLI도 같은 변환 규칙을 사용한다.

```bash
smoking-sbdf convert input.csv output.sbdf
smoking-sbdf -help
```

`-help`, `--help`, `-h`는 같은 도움말을 출력한다. `-help`는 LLM이나 자동화 도구가 단일 대시 형태로 요청해도 실패하지 않도록 제공하는 호환 별칭이다.

## 병렬 worker 설정

파일 변환은 CLI와 Python API에서 `1..8` worker를 지정할 수 있다.

```bash
# CSV: 낮은 메모리가 우선이면 기본 worker 1, 처리량 우선이면 명시적으로 증가
smoking-sbdf convert input.csv output.sbdf --workers 4

# Parquet: 기본 최대값 3, metadata가 실제 worker를 낮출 수 있음
smoking-sbdf convert input.parquet output.sbdf --workers 3

# 요청한 Parquet worker 수를 그대로 사용
smoking-sbdf convert input.parquet output.sbdf \
  --workers 4 --no-adaptive-workers
```

Python에서도 같은 정책을 사용한다.

```python
from smoking_sbdf import convert

convert("input.csv", "output.sbdf", workers=4)
convert(
    "input.parquet",
    "output.sbdf",
    workers=3,
    adaptive_workers=True,
)
```

대표 프로파일에서 CSV worker 3–4는 wall time을 약 31–50% 줄였지만 CPU 사용량이 늘고 peak RSS가 최대 약 7.8배까지 증가했다. Parquet worker 3은 shape에 따라 wall이 약 15% 개선되거나 약 3% 느려졌고, CPU는 늘지만 peak RSS는 크게 낮아지는 경우가 있었다. 따라서 CSV는 worker 1, Parquet은 adaptive 최대 3을 기본으로 유지한다.

DataFrame API는 이미 메모리에 올라온 객체를 한 batch로 기록하므로 `workers` 옵션을 제공하지 않는다.

이미 메모리에 있는 DataFrame도 직접 기록할 수 있다.

```python
import pandas as pd
import smoking_sbdf

frame = pd.DataFrame({"wafer_id": [1, 2], "value": [3.0, 4.0]})
frame.to_sbdf("output.sbdf")
```

## 프로젝트 README 초기화

LLM 또는 자동화 도구에 전달할 최소 사용 안내를 현재 디렉터리에 생성한다.

```bash
smoking-sbdf init
```

다른 프로젝트 디렉터리를 지정할 수도 있다.

```bash
smoking-sbdf init ./data-project
```

생성 경로는 `<directory>/README.md`다. 기존 README는 보호되며, 명시적으로 교체할 때만 `--force`를 사용한다.

```bash
smoking-sbdf init ./data-project --force
```

생성 문서는 CSV·Parquet 변환 예시, Python API, LLM 작업 안전 지침을 포함한다. 사용자명, 홈 디렉터리, 호스트명, 저장소 계정이나 실제 데이터 경로는 기록하지 않는다.

## Row-key sidecar

`row_key_columns`를 지정하면 `<output>.sidecar.parquet`이 함께 생성된다. sidecar의 Parquet row group은 SBDF table slice와 1:1이며 row key, 행 좌표, SBDF byte offset과 length를 보관한다. 조회자는 sidecar predicate 결과의 slice 범위를 S3 Range GET 같은 부분 읽기에 사용할 수 있다.

```python
convert(
    "events.parquet",
    "events.sbdf",
    row_key_columns=["device_id", "event_id"],
    table_id="fab.events",
)
```

row key 기준 전역 정렬이나 slice 재편성은 수행하지 않는다. 이 정책은 변환 처리량과 bounded-memory 특성을 보존한다.

## 프로젝트 구조

- `src/lib.rs`: PyO3 진입점과 CSV·Parquet 변환 orchestration
- `src/rust_sbdf.rs`: SBDF 1.0 직렬화
- `src/sbdf_index.rs`: Parquet sidecar 생성
- `src/smoking_sbdf/`: Python API와 CLI
- `src/type_rules/`: 컬럼명·dtype 매핑 규칙

## 현재 범위

v0.1.5는 로컬 파일 변환과 sidecar 생성을 제공한다. 원격 객체 저장소 읽기, sidecar 조회 엔진, DataFrame chunk streaming과 CSV dialect 전체 노출은 패키지 범위에 포함하지 않는다.

## 라이선스와 상표

`smoking-sbdf` 자체 코드는 BSD 3-Clause License로 배포한다. Rust SBDF
wire-format 구현은 Cloud Software Group의 공식
[`spotfire-sbdf-c`](https://github.com/spotfiresoftware/spotfire-sbdf-c)를 참고했으며,
원본 저작권과 라이선스 조건은
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md)에 보존한다.
Rust 의존성의 패키지별 저작권·라이선스 원문은
[`THIRD_PARTY_LICENSES.txt`](THIRD_PARTY_LICENSES.txt)에 보존한다.

현재 배포물에는 Spotfire SBDF C 소스나 C FFI가 포함되지 않는다.
Spotfire 명칭은 호환 대상 포맷을 식별하기 위해서만 사용하며, 이
프로젝트는 Cloud Software Group 또는 Spotfire의 공식 프로젝트가
아니고 후원이나 보증을 받지 않는다.

## 개인정보와 경로 정책

- 문서와 예제에는 상대 경로 또는 중립적인 placeholder만 사용한다.
- `init`은 실행 환경, Git remote, 사용자 계정과 입력 데이터 내용을 수집하지 않는다.
- 변환 API는 사용자가 지정한 입력과 출력 경로만 처리한다.
- 오류 메시지에는 작업에 필요한 파일 경로가 포함될 수 있으므로 외부 공유 전 로그를 별도로 검토한다.
