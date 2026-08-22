# `smoking-sbdf` 사용법

이 문서는 Python을 처음 사용하는 사람도 CSV나 Parquet 파일을 SBDF로 변환할 수
있도록 기본 개념부터 설명한다. 패키지 이름은 `smoking-sbdf`, Python에서 불러올
이름은 `smoking_sbdf`, 터미널 명령어는 `smoking-sbdf`다.

## 먼저 알아둘 것

- 입력 파일은 그대로 두고 새 SBDF 파일을 만든다.
- 출력 경로의 상위 디렉터리가 없으면 자동으로 만든다.
- 일반 변환 결과는 `pathlib.Path` 객체다.
- PyPI에는 게시하지 않는다. GitHub Release에서 운영체제와 Python 버전에 맞는
  wheel을 받아 설치한다.

## 설치

먼저 Python 버전을 확인한다.

```bash
python --version
```

CPython 3.10–3.14를 지원한다. GitHub Release에서 받은 wheel 파일이 현재
디렉터리에 있다면 다음처럼 설치한다.

```bash
python -m pip install ./smoking_sbdf-0.1.6-<python-tag>-<platform-tag>.whl
```

파일 이름에 `cp313`이 있으면 CPython 3.13용이다. `manylinux`는 Linux,
`win_amd64`는 64비트 Windows용이다. 개발용 source 설치는 [BUILD.md](BUILD.md)를
참고한다.

설치 확인:

```bash
python -c "import smoking_sbdf; print('설치 완료')"
smoking-sbdf --version
```

## 가장 간단한 Python 변환

`convert()`는 입력 경로를 보고 CSV, Parquet 파일 또는 Parquet dataset을 자동으로
구분한다.

```python
from smoking_sbdf import convert

output_path = convert("input.csv", "output.sbdf")
print(output_path)
```

Parquet도 호출 방법이 같다.

```python
from smoking_sbdf import convert

convert("input.parquet", "output.sbdf")
```

경로는 문자열 또는 `pathlib.Path`로 전달할 수 있다.

```python
from pathlib import Path
from smoking_sbdf import convert

source = Path("data/input.parquet")
target = Path("output/result.sbdf")
convert(source, target)
```

확장자가 `.csv`, `.parquet`, `.pq`가 아니면 입력 형식을 직접 지정한다.

```python
convert("input.data", "output.sbdf", input_format="csv")
```

## 변환 결과와 실행 통계 받기

Smoking Data 같은 상위 프로그램에서 실제 worker 수나 행 수를 기록하려면
`convert_with_result()`를 사용한다.

```python
from smoking_sbdf import convert_with_result

result = convert_with_result(
    "input.parquet",
    "output.sbdf",
    workers=3,
)

print(result.output_path)
print(result.row_count)
print(result.slice_count)
print(result.requested_workers)
print(result.effective_workers)
```

반환값은 `SbdfConversionResult`이며 다음 필드를 제공한다.

| 필드 | 의미 |
| --- | --- |
| `output_path` | 완성된 SBDF 경로 |
| `input_files` | 실제 처리한 입력 파일 목록 |
| `requested_workers` | 사용자가 요청한 worker 수 |
| `effective_workers` | adaptive 정책과 fallback 이후 실제 worker 수 |
| `requested_batch_size` | 사용자가 요청한 최대 batch 행 수 |
| `effective_batch_sizes` | 각 입력 파일에 실제 적용된 batch cap |
| `row_count` | 기록한 전체 행 수 |
| `slice_count` | 기록한 SBDF table slice 수 |
| `sidecar_path` | 생성된 sidecar 경로, 없으면 `None` |

`input_files`와 `effective_batch_sizes`는 같은 순서다.

```python
for input_file, batch_cap in zip(
    result.input_files,
    result.effective_batch_sizes,
):
    print(input_file, batch_cap)
```

통계는 변환 중 계산한 값을 사용한다. 통계를 얻기 위해 입력이나 SBDF를 다시 읽지
않는다. 단, row-key sidecar를 요청하면 sidecar 생성 자체에 필요한 SBDF 순차 읽기는
수행한다.

## 터미널에서 변환하기

Python 코드를 작성하지 않으려면 CLI를 사용한다.

```bash
smoking-sbdf convert input.csv output.sbdf
smoking-sbdf convert input.parquet output.sbdf
```

도움말:

```bash
smoking-sbdf --help
smoking-sbdf convert --help
```

`-help`도 같은 의미로 사용할 수 있다. 성공하면 출력 경로를 표시하고 종료 코드
`0`을 반환한다. 입력이나 옵션이 잘못되면 오류를 표시하고 종료 코드 `2`를 반환한다.

## 여러 Parquet 파일 처리하기

### Dataset 디렉터리

디렉터리 바로 아래의 `.parquet` 파일을 파일명 순으로 처리한다.

```python
from smoking_sbdf import convert

convert("snapshot", "snapshot.sbdf")
```

하위 디렉터리까지 찾으려면 `recursive=True`를 사용한다.

```python
convert("snapshot", "snapshot.sbdf", recursive=True)
```

### 파일 목록

처리 순서를 직접 정하려면 파일 목록 API를 사용한다.

```python
from smoking_sbdf import parquet_files_to_sbdf_streaming

parquet_files_to_sbdf_streaming(
    [
        "snapshot/part-000.parquet",
        "snapshot/part-001.parquet",
    ],
    "snapshot.sbdf",
)
```

모든 파일의 schema는 첫 파일과 정확히 같아야 한다.

### Manifest

반복 실행에서 입력 순서를 고정하려면 manifest가 편리하다.

```text
# snapshot.manifest
snapshot/part-000.parquet
snapshot/part-001.parquet
```

```python
from smoking_sbdf import convert

convert("snapshot.manifest", "snapshot.sbdf")
```

manifest의 상대 경로는 manifest 파일이 있는 디렉터리를 기준으로 해석한다. 빈 줄과
`#`으로 시작하는 줄은 무시한다.

## DataFrame 저장하기

### 함수 방식: 권장

pandas나 Polars DataFrame은 함수로 저장할 수 있다. 이 방식은 다른 라이브러리의
클래스를 변경하지 않아 상위 애플리케이션에서 가장 안전하다.

```python
import pandas as pd
from smoking_sbdf import dataframe_to_sbdf

frame = pd.DataFrame({
    "wafer_id": [1, 2],
    "value": [3.5, 4.5],
})

dataframe_to_sbdf(frame, "frame.sbdf")
```

### `DataFrame.to_sbdf()` 방식: 선택 사항

`pandas.DataFrame.to_csv()`와 비슷한 메서드가 필요하면 명시적으로 등록한다.

```python
import pandas as pd
import smoking_sbdf

smoking_sbdf.install_dataframe_methods()

frame = pd.DataFrame({"id": [1, 2]})
frame.to_sbdf("frame.sbdf")
```

`import smoking_sbdf`만으로는 pandas나 Polars 클래스를 변경하지 않는다. 이미
`to_sbdf`라는 메서드가 있으면 기본적으로 덮어쓰지 않는다.

DataFrame은 메모리에 올라온 전체 데이터를 Python list로 바꿔 한 batch로 기록한다.
대용량 파일을 bounded-memory로 처리해야 한다면 CSV나 Parquet 파일 API를 사용한다.

## 컬럼 타입 지정하기

대부분의 일반 dtype은 자동 변환한다. 추론 결과가 원하는 Spotfire 타입과 다르면
`column_types`로 지정한다.

```python
from smoking_sbdf import convert

convert(
    "input.csv",
    "output.sbdf",
    column_types={
        "wafer_id": "LongInteger",
        "event_time": "DateTime",
        "value": "Real",
    },
)
```

주요 타입 이름은 `Boolean`, `Integer`, `LongInteger`, `SingleReal`, `Real`,
`DateTime`, `Date`, `Time`, `TimeSpan`, `String`, `Binary`다.

CLI에서는 옵션을 반복한다.

```bash
smoking-sbdf convert input.csv output.sbdf \
  --column-type wafer_id=LongInteger \
  --column-type event_time=DateTime
```

## Worker와 메모리 사용량

- CSV 기본값은 worker 1이다.
- Parquet 기본 요청값은 worker 3이며 metadata를 보고 실제 수를 낮출 수 있다.
- `workers`는 1–8 범위다.
- worker를 늘리면 wall time이 줄 수 있지만 CPU와 메모리 사용량은 증가한다.

```python
from smoking_sbdf import convert

convert("large.csv", "output.sbdf", workers=4)
convert("large.parquet", "output.sbdf", workers=3)
```

Parquet worker 수를 반드시 요청값으로 사용하려면 adaptive 정책을 끈다.

```python
convert(
    "input.parquet",
    "output.sbdf",
    workers=3,
    adaptive_workers=False,
)
```

일반적으로 먼저 기본값으로 실행하고, 같은 데이터와 저장장치에서 측정한 뒤 worker를
늘리는 것이 안전하다. 자세한 측정 결과는 [성능 기록](docs/PERFORMANCE.md)에 있다.

## Row-key sidecar 만들기

특정 key가 들어 있는 SBDF slice의 byte offset이 필요하면 Parquet sidecar를 함께
만든다.

```python
from smoking_sbdf import convert_with_result

result = convert_with_result(
    "events.parquet",
    "events.sbdf",
    row_key_columns=["device_id", "event_id"],
    table_id="events",
)

print(result.sidecar_path)
```

기본 경로는 `<SBDF 경로>.sidecar.parquet`이다. sidecar의 Parquet row group 하나는
SBDF table slice 하나와 대응한다. row key 기준 전역 정렬이나 slice 재편성은 하지
않는다.

기존 SBDF에 sidecar만 추가할 수도 있다.

```python
from smoking_sbdf import generate_sbdf_sidecar

generate_sbdf_sidecar(
    "events.sbdf",
    row_key_columns=["device_id"],
)
```

## 자주 발생하는 오류

### 입력 형식을 알 수 없다는 오류

`input_format="csv"`, `"parquet"`, `"parquet-dataset"` 또는
`"parquet-manifest"`를 지정한다.

### CSV 타입 추론 오류

`infer_schema_rows`를 늘리거나 문제가 되는 컬럼의 `column_types`를 지정한다.

### Parquet schema mismatch

여러 입력 파일의 컬럼 이름, 순서, dtype과 nullability를 동일하게 맞춘다.

### workers 오류

정수 `1`부터 `8`까지만 사용할 수 있다. `True`, `False`, `0`은 허용하지 않는다.

### wheel 설치 오류

wheel의 `cp` 태그와 현재 CPython 버전, 플랫폼 태그가 일치하는지 확인한다.

## 다음 문서

- [README.md](README.md): 프로젝트 개요와 지원 범위
- [BUILD.md](BUILD.md): 로컬 개발 설치와 검증
- [코드베이스 가이드](docs/CODEBASE_GUIDE.md): 모듈 구조와 변환 흐름
- [성능 기록](docs/PERFORMANCE.md): 채택한 기본값의 측정 근거
