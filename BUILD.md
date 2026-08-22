# 로컬 빌드와 검증

이 문서는 저장소에서 `smoking-sbdf`를 직접 수정하고 확인할 때 필요한 최소 절차만
설명한다. PyPI 게시나 정기 릴리스 운영은 프로젝트 범위가 아니다.

## 준비물

- CPython 3.10 이상
- stable Rust toolchain과 Cargo
- `maturin>=1.7,<2.0`

## 가상환경 만들기

Linux와 macOS:

```bash
python -m venv .venv
.venv/bin/python -m pip install "maturin>=1.7,<2.0"
```

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install "maturin>=1.7,<2.0"
```

## 개발 설치

Linux와 macOS:

```bash
.venv/bin/python -m maturin develop
```

Windows PowerShell:

```powershell
.venv\Scripts\python -m maturin develop
```

이 명령은 Rust extension을 빌드하고 현재 source tree의 Python 래퍼를 가상환경에
설치한다. Rust 또는 PyO3 경계를 수정한 뒤에는 다시 실행해야 한다.

## 검증

가까운 검사부터 실행한다.

```bash
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

Python 테스트 전에 최신 native extension이 필요하면 `maturin develop`을 먼저
실행한다. 로컬 검증 작업공간에 `tests/`가 있는 경우 다음 검사도 실행한다.

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Python 테스트는 임시 디렉터리에 파일을 만들며 실제 입력 데이터를 사용하지 않는다.

## 로컬 wheel 만들기

```bash
.venv/bin/python -m maturin build --release --locked
```

wheel은 `target/wheels/`에 생성된다. 이 작업은 로컬 산출물만 만들며 PyPI나 GitHub에
업로드하지 않는다.

## 코드 위치

- Python 공개 API: `src/smoking_sbdf/__init__.py`
- CLI: `src/smoking_sbdf/cli.py`
- Rust 변환 orchestration: `src/lib.rs`
- SBDF serializer: `src/rust_sbdf.rs`
- sidecar indexer: `src/sbdf_index.rs`

구조와 변경 시 확인할 계약은 [코드베이스 가이드](docs/CODEBASE_GUIDE.md)에 있다.
