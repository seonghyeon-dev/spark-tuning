# S3FileIO 전환 결과 — MinIO API 호출 감소

| 항목 | 내용 |
|------|------|
| 대상 | Iceberg 카탈로그 전체 (측정: expire snapshots Job) |
| 문제 | 1일 배치 삭제 후 expire snapshots가 MinIO에 대량의 `deleteObject`/`listObject` 발생 |
| 조치 | Iceberg FileIO를 `HadoopFileIO`(S3A) → `S3FileIO`로 전환, bulk delete 적용 |
| 측정 | 2026-08-27, 운영환경 |

---

## 1. 결과

| 지표 | 변경 전 | 변경 후 | 증감 |
|------|--------|--------|------|
| **`deleteObject` (peak req/s)** | **481** | **17.4** | **−96.4%** |
| **`listObjectV2` (peak req/s)** | **680** | **281** | **−58.7%** |
| 소요시간 | 13.5분 | 3.8분 | −72% |
| 리소스 비용 (dcu) | 0.2002 | 0.0550 | −72.5% |
| DataFlint 경고 | 18건 | 6건 | |

- **총 요청 수 기준으로는 `deleteObject`가 약 −99%.** 위는 peak req/s이고 소요시간이 3.5배 짧아졌으므로, 시간축으로 적분하면 감소폭이 더 크다
- MinIO 부하 해소가 목적이었고, **부하의 주 원인이던 삭제 요청이 사실상 제거**되었다

### MinIO API 호출 추이 (Grafana)

> **[그림 1] 전환 전** — `pipeline/images/minio-api-before.png`
>
> ![전환 전](images/minio-api-before.png)

> **[그림 2] 전환 후** — `pipeline/images/minio-api-after.png`
>
> ![전환 후](images/minio-api-after.png)

캡처 시 포함할 것:
- API별 req/s 시계열 (`deleteObject`, `listObjectV2` 두 계열이 보이도록)
- expire snapshots 실행 구간이 들어가는 시간 범위
- 두 그림의 **Y축 범위를 동일하게** 맞출 것 (안 맞추면 감소폭이 시각적으로 왜곡된다)

---

## 2. 원인

### 2.1 S3A는 파일 1개를 지우는 데 요청을 3개 쓴다

`S3AFileSystem`은 오브젝트 스토리지를 **파일시스템처럼 보이게 하는 것**이 목적이라, 디렉터리가 없는 S3에 디렉터리 동작을 흉내 낸다.

| 순번 | 요청 | 목적 |
|------|------|------|
| ① | `HeadObject` | 삭제 전 존재 확인 |
| ② | `DeleteObject` | 실제 삭제 |
| ③ | **`ListObjectsV2`** | **부모 "디렉터리"가 비었는지 확인** |
| ④ | `PutObject` | 비었으면 디렉터리 마커 생성 (조건부) |

**관측된 `listObject` 대량 발생의 정체가 ③이다.** 삭제와 무관한, 디렉터리 시맨틱 유지용 요청이다. ④ 때문에 삭제 작업이 객체를 생성하기까지 한다.

### 2.2 기존 구성에서는 bulk delete가 동작하지 않았다

Iceberg는 삭제 대상 **전체 경로 목록을 이미 확보한 상태**로 일괄 삭제를 호출한다. 그런데 기존 `HadoopFileIO`의 일괄 삭제 구현이 내부적으로 **파일을 하나씩 지우는 반복문**이다.

```java
// HadoopFileIO.deleteFiles() — 일괄 삭제 인터페이스이지만
Tasks.foreach(pathsToDelete)
    .executeWith(executorService())
    .run(this::deleteFile);        // 내부는 단건 삭제 (fs.delete)
```

결과적으로 MinIO에 **`DeleteObjects`(일괄 삭제) 요청이 0건**이었고, 파일 수만큼의 단건 요청이 발생했다.

### 2.3 삭제 요청은 driver 한 곳에서 나간다

삭제 대상 산출은 executor가 분산 처리하지만, **실제 삭제 요청은 driver 단독**으로 보낸다. 요청량은 많은데 동시성은 driver에 묶여, MinIO 부하와 Job 지연이 동시에 발생하는 구조였다.

---

## 3. 조치

Iceberg가 스토리지에 접근하는 구현체(`FileIO`)를 교체했다.

| | 변경 전 | 변경 후 |
|---|---|---|
| 구현체 | `HadoopFileIO` (기본값) | `S3FileIO` |
| 접근 경로 | Iceberg → Hadoop FileSystem → S3A → AWS SDK v1 | Iceberg → AWS SDK v2 |
| 파일 삭제 | 파일당 요청 3개 | **250개당 `DeleteObjects` 1개** |

### 적용 내용

```properties
spark.sql.catalog.<카탈로그>.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.<카탈로그>.s3.endpoint=http://<minio>:9000
spark.sql.catalog.<카탈로그>.s3.path-style-access=true
spark.sql.catalog.<카탈로그>.client.region=us-east-1
```

- 이미지에 `iceberg-aws-bundle-1.10.1.jar` 추가 (AWS SDK v2 제공, 약 60MB)
- 삭제 배치 크기 기본값 250 (최대 1000까지 상향 가능)
- **기존 `fs.s3a.*` 설정은 유지** — 원천 avro 읽기와 Spark 이벤트 로그가 계속 사용한다

---

## 4. 사이드 이펙트 검증

| 항목 | 결과 |
|------|------|
| 기존 테이블 | **영향 없음.** 메타데이터의 `s3a://` 경로를 `S3FileIO`가 그대로 처리 |
| 마이그레이션 작업 | **불필요.** 메타데이터 재작성·테이블 재생성 없음 |
| 롤백 | **설정 제거만으로 원복.** 전환 중 기록된 파일도 기존 방식으로 읽힌다 |
| Trino 조회 | **영향 없음.** 자체 S3 클라이언트를 사용 |
| Job 동작 | append / expire snapshots / remove orphan files / rewrite manifests / Compaction **전부 정상** |

---

## 5. 남은 작업

| 항목 | 내용 |
|------|------|
| maintenance Job 리소스 축소 | 유휴 CPU가 전환 후에도 90%. executor 축소 검토 |
| 자격증명 관리 통합 | K8s Secret → 환경변수 일원화로 설정 중복 해소 |
| Compaction / append 성능 비교 | 리소스 튜닝과 함께 진행 |
| Iceberg 1.11.0 + Spark 4.1 업그레이드 | 별도 검토 완료 |

---

## 참고

- 상세 분석·소스 근거·전환 절차: `pipeline/s3fileio-migration-guide.md`
- Iceberg AWS Integration: https://iceberg.apache.org/docs/latest/aws/
