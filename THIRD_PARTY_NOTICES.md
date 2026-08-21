# Third-Party Notices

`smoking-sbdf` 자체 코드는 최상위 [`LICENSE`](LICENSE)의 BSD 3-Clause
License로 배포된다. 아래 구성 요소와 참고 구현은 각 저작권자의 권리와
라이선스 조건을 유지한다.

## Spotfire SBDF C 구현

- 원본 프로젝트: [spotfiresoftware/spotfire-sbdf-c](https://github.com/spotfiresoftware/spotfire-sbdf-c)
- 저작권자: Cloud Software Group, Inc.
- 라이선스: BSD 3-Clause License

현재 배포물은 `spotfire-sbdf-c`의 C 소스를 포함하거나 해당 라이브러리에
링크하지 않는다. 다만 Rust SBDF wire-format 구현은 공식 C 구현을 참고해
개발되었으므로, 소스 변환·적용 가능성을 포함하는 보수적인 라이선스 준수를
위해 원본 저작권과 라이선스 전문을 아래에 보존한다.

```text
Copyright (c) 2023 Cloud Software Group, Inc. All Rights Reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of Cloud Software Group, Inc. nor the names of any
   contributors may be used to endorse or promote products derived from this
   software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT OWNER AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

원본 소스 파일에는 2022년 Cloud Software Group, Inc. 저작권 표시도 포함되어
있으며, 해당 표시 역시 원저작자의 권리로 인정한다.

## Rust 의존성

배포 wheel은 PyO3, Apache Arrow, Apache Parquet, `csv`, `memmap2`와 그 전이
의존성을 포함할 수 있다. 각 wheel의
`smoking_sbdf-*.dist-info/sboms/smoking-sbdf.cyclonedx.json`에 빌드 시점의
구성 요소, 버전과 라이선스 식별자가 기록된다. 해당 구성 요소는 각각의
라이선스에 따라 배포되며 `smoking-sbdf`의 BSD 3-Clause License가 그
라이선스를 대체하지 않는다.
패키지별 저작권, 라이선스 식별자와 원문은
[`THIRD_PARTY_LICENSES.txt`](THIRD_PARTY_LICENSES.txt)에 함께 보존한다.

Spotfire 및 관련 명칭은 호환 대상 포맷을 식별하기 위해서만 사용한다. 이
프로젝트는 Cloud Software Group 또는 Spotfire의 공식 프로젝트가 아니며,
그 회사의 후원이나 보증을 받지 않는다.
