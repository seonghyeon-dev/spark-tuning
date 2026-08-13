# hourly Compaction 튜닝 결과

| 항목 | 내용 |
|------|------|
| 대상 | hourly Compaction Job (테이블 4개) |
| 측정 | 2026-08-11 ~ 08-12, 9회 |
| 변경 | Iceberg 옵션 2건, Spark 설정 2건, 리소스 2건 |

---

## 1. 결과

| 지표 | 변경 전 | 변경 후 | 증감 |
|------|--------|--------|------|
| 소요시간 (초/GB) | 3.24 | 2.41 | −26% |
| 리소스 비용 (dcu/GB) | 0.00416 | 0.00219 | −47% |
| 유휴 CPU | 58% | 17% | |
| DAG 전체 (테이블 4개) | 10~12분 | 약 6분 | |
| executor | 16개 | 12개 | CPU 16 core, 메모리 64GB 절감 |

- 실행 창: 매시 45분~정각(12분). 여유 3분 → 6분
- 절감 리소스 → 5분 주기 append Job과의 경합 감소

---

## 2. 원인 및 조치

### 2.1 처리 회차 분할 — 소요시간 30% 감소

- Compaction 처리 단위: 파티션별로 파일을 묶은 file group
- 동시 처리 group 수: `max-concurrent-file-group-rewrites`로 결정

**원인** — group 7개를 2개씩 처리, 4회차로 분할. 회차별로 느린 쪽 종료까지 대기

| 회차 | 처리 대상 | 데이터 비중 | 소요시간 비중 |
|------|----------|-----------|-------------|
| 1 | 1.4GB + 2.8GB | 11% | 21% |
| 2 | 9.7GB + 2.2GB | 32% | 27% |
| 3 | 9.9GB + 10.2GB | 54% | 29% |
| 4 | 1.5GB (단독) | 4% | 17% |

- 1·4회차: 데이터 15%에 소요시간 37% 사용
- CPU 64 core 중 58% 유휴

**조치**

| 설정 | 변경 | 효과 |
|------|------|------|
| `max-concurrent-file-group-rewrites` | 2 → 10 | 1회차로 처리 |
| `max-file-group-size-bytes` | 10GB → 기본값 | group 7개 → 4개 (파티션당 1개) |

### 2.2 리소스 과다 — 리소스 비용 13% 감소

**원인** — 2.1 조치 후 유휴 CPU 25~29% 잔존

**조치** — executor 16 → 12. 축소 한계 확인을 위해 3개 지점 측정

| executor | 소요시간 (초/GB) | 리소스 비용 (dcu/GB) | 유휴 CPU | disk spill |
|----------|----------------|-------------------|---------|-----------|
| 16 | 2.03 | 0.00251 | 25~29% | 0 |
| **12** | 2.41 | **0.00219** | 17% | 0 |
| 8 | 3.77 | 0.00247 | 15% | 0 |

- 8: CPU 33% 감소 대비 소요시간 56% 증가 → 리소스 비용 반등
- 하한: 12

### 2.3 무효 설정 — 삭제

| 설정 | 확인 방법 | 결과 |
|------|----------|------|
| `advisory-partition-size` 768MB | 삭제 전후 비교 | 파일당 크기 507.9 → 507.0MB |
| `coalescePartitions.parallelismFirst` false | true로 전환 | 파일당 크기 509 → 510MB |

- 출력 파일 크기 결정 요소: `target-file-size-bytes`(512MB) 단독
- Iceberg가 shuffle partition 수를 직접 산정 → 두 Spark 설정 미개입

---

## 3. 확정 설정

**변경**

| 설정 | 변경 전 | 변경 후 |
|------|--------|--------|
| `max-concurrent-file-group-rewrites` | 2 | 10 |
| `max-file-group-size-bytes` | 10GB | 기본값 (100GB) |
| `num-executors` | 16 | 12 |
| `driver cpu` | 1 | 2 |
| `advisory-partition-size` | 768MB | 삭제 |
| `coalescePartitions.parallelismFirst` | false | 삭제 |

**미변경**

| 설정 | 값 | 근거 |
|------|-----|------|
| `target-file-size-bytes` | 512MB | 스키마 설계 확정값 |
| `rewrite-all` | true | 입력 파일 전부가 대상이므로 false와 처리량 동일 |
| `executor cpu / memory` | 4 core / 16GB | 메모리 사용률 89~94%, disk spill 0 |
| `partial-progress.enabled` | 기본값 (false) | 변경 시 snapshot 증가 → 보존 정책·재처리 DAG에 영향 |
| rewrite 전략 | `sort` | 미적용 시 조회 40% 저하 (읽기 성능 테스트 §5.4) |

---

## 4. 출력 파일 품질

| 항목 | Compaction 전 | Compaction 후 |
|------|--------------|--------------|
| 파일 수 | 703개 | 75개 |
| 평균 크기 | 53.9MB | 509MB |
| 최대 크기 | 72.3MB | 607MB |
| 최소 크기 | 0.6MB | 300~415MB |

- 목표 512MB 대비 평균 509MB
- 9회 전부 동일 수준 유지

**최소 크기 300~415MB — 미조치**

| 항목 | 내용 |
|------|------|
| 원인 | `col_a=D` 파티션 시간당 600~830MB. 512MB 기준 분할로 623MB → 311MB 파일 2개 |
| 범위 | 파일 75개 중 2개, 데이터 37GB 중 0.9GB(2.4%) |
| 해소 조건 | 목표 파일 크기 830MB 이상 상향. 전체 파티션 적용으로 스키마 설계 결정과 충돌 |

---

## 5. 남은 작업

| 항목 | 내용 |
|------|------|
| executor 상한 확정 | 데이터 증가 대응 자동 산정에 K8S 리소스 할당량 필요 |
| 나머지 테이블 3개 검증 | 파티션 값 개수에 따라 file group 수 변동 |
| daily Compaction 점검 | 888GB에 30~60분. 소요시간이 데이터 양에 비례 → 전체 재작성 여부 확인 |

---

## 참고

| 문서 | 내용 |
|------|------|
| `tuning/compaction-tuning-guide.md` | 설정별 상세 설명, 측정값 전체, 동적 산정 |
| `tuning/spark-tuning-guide.md` | append Job 설정 |
| `schema/read-performance-test.md` §5 | Sort Order 읽기 성능 실측 |
