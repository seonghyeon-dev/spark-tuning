# S3FileIO 전환 결과 — MinIO API 호출 감소

| 항목 | 내용 |
|------|------|
| 배경 | 1일 배치 삭제 후 expire snapshots가 MinIO에 과도한 API 호출을 발생시켜 부하 유발 |
| 조치 | Iceberg의 스토리지 접근 구현체를 `HadoopFileIO`(S3A) → `S3FileIO`로 전환 |
| 효과 | 삭제 관련 API 호출 대폭 감소, MinIO 부하 해소 |
| 적용 | 2026-08-27, 운영환경 |

---

## 1. 문제

`expire_snapshots`는 만료된 snapshot이 참조하던 데이터 파일과 메타데이터 파일을 삭제한다. 1일 배치 삭제 이후에는 삭제 대상이 크게 늘어나는데, 이때 MinIO에 `deleteObject`와 `listObject` 호출이 대량으로 발생해 스토리지 부하를 유발했다.

---

## 2. 원인

### 2.1 S3A는 파일 1개를 지우는 데 요청을 3개 쓴다

`S3AFileSystem`은 오브젝트 스토리지를 **파일시스템처럼 보이게 하는 것**이 목적이다. 그래서 디렉터리 개념이 없는 S3에 디렉터리 동작을 흉내 낸다.

| 순번 | 요청 | 목적 |
|------|------|------|
| ① | `HeadObject` | 삭제 전 존재 확인 |
| ② | `DeleteObject` | 실제 삭제 |
| ③ | **`ListObjectsV2`** | **부모 "디렉터리"가 비었는지 확인** |
| ④ | `PutObject` | 비었으면 디렉터리 마커 생성 (조건부) |

**관측된 `listObject` 대량 발생의 정체가 ③이다.** 삭제와는 무관한, 디렉터리 시맨틱 유지용 요청이다. ④ 때문에 삭제 작업이 객체를 새로 만들기까지 한다.

> **원본 코드** (Hadoop 3.3.4)
> - [`S3AFileSystem.delete()` — ①②③ 호출 흐름](https://github.com/apache/hadoop/blob/rel/release-3.3.4/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L3162-L3181)
> - [`createFakeDirectoryIfNecessary()` — ③ LIST와 ④ 마커 생성](https://github.com/apache/hadoop/blob/rel/release-3.3.4/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L3203-L3212)

### 2.2 기존 구성에서는 일괄 삭제가 동작하지 않았다

Iceberg는 삭제 대상의 **전체 경로 목록을 이미 확보한 상태**로 일괄 삭제를 호출한다. 그런데 기존 `HadoopFileIO`의 일괄 삭제 구현이 내부적으로는 **파일을 하나씩 지우는 반복문**이다.

```java
// HadoopFileIO.deleteFiles() — 일괄 삭제 인터페이스이지만
Tasks.foreach(pathsToDelete)
    .executeWith(executorService())
    .run(this::deleteFile);        // 내부는 단건 삭제
```

결과적으로 MinIO에 **일괄 삭제 요청(`DeleteObjects`)이 한 건도 가지 않았고**, 파일 수만큼의 단건 요청이 발생했다.

> **원본 코드** (Iceberg 1.10.1)
> - [`HadoopFileIO.deleteFiles()` — 일괄 삭제 인터페이스의 실제 구현](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/hadoop/HadoopFileIO.java#L176-L194)
> - [`HadoopFileIO.deleteFile()` — 결국 호출되는 단건 삭제](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/hadoop/HadoopFileIO.java#L100-L109)
> - [`S3FileIO.deleteFiles()` — 전환 후 사용되는 `DeleteObjects` 일괄 삭제](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/aws/src/main/java/org/apache/iceberg/aws/s3/S3FileIO.java#L230-L300)

---

## 3. 조치

Iceberg가 스토리지에 접근하는 구현체(`FileIO`)를 교체했다.

| | 변경 전 | 변경 후 |
|---|---|---|
| 구현체 | `HadoopFileIO` (기본값) | `S3FileIO` |
| 접근 경로 | Iceberg → Hadoop FileSystem → S3A → AWS SDK v1 | Iceberg → AWS SDK v2 |
| 파일 삭제 | 파일당 단건 요청 3개 | **여러 파일을 한 요청으로 일괄 삭제** |

전환으로 두 가지가 함께 개선된다.

| 개선 | 대상 | 내용 |
|------|------|------|
| **일괄 삭제(bulk delete) 적용** | `deleteObject` | 파일 250개를 `DeleteObjects` 요청 1건으로 묶어 전송 |
| **디렉터리 흉내 제거** | `listObject` | `S3FileIO`는 디렉터리 개념을 쓰지 않아 부모 확인 LIST가 발생하지 않는다 |

### 적용 내용

```properties
spark.sql.catalog.<카탈로그>.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.<카탈로그>.s3.endpoint=http://<minio>:9000
spark.sql.catalog.<카탈로그>.s3.path-style-access=true
spark.sql.catalog.<카탈로그>.client.region=us-east-1
```

- 이미지에 `iceberg-aws-bundle-1.10.1.jar` 추가 (AWS SDK v2 제공)
- **기존 `fs.s3a.*` 설정은 유지** — 원천 avro 읽기와 Spark 이벤트 로그가 계속 사용한다

---

## 4. 결과

### MinIO API 호출 추이 (Grafana)

> **[그림 1] 전환 전**
>
> ![전환 전](images/minio-api-before.png)

> **[그림 2] 전환 후**
>
> ![전환 후](images/minio-api-after.png)

- **`deleteObject` 호출이 사실상 사라졌다.** 일괄 삭제로 묶여 요청 건수 자체가 줄었다
- **`listObject` 호출도 크게 감소했다.** 디렉터리 확인용 LIST가 발생하지 않는다
- 부수 효과로 **expire snapshots 소요시간이 13.5분에서 3.8분으로 단축**됐다. 삭제 요청을 순차적으로 주고받던 구간이 사라진 결과다

*(작성 메모 — 캡처 후 이 줄 삭제: 두 그림의 Y축 범위를 동일하게 맞출 것. 자동 스케일로 두면 감소폭이 시각적으로 드러나지 않는다.)*

---

## 5. 사이드 이펙트 검증

| 항목 | 결과 |
|------|------|
| 기존 테이블 | **영향 없음.** 메타데이터의 `s3a://` 경로를 `S3FileIO`가 그대로 처리 |
| 마이그레이션 작업 | **불필요.** 메타데이터 재작성·테이블 재생성 없음 |
| 롤백 | **설정 제거만으로 원복.** 전환 중 기록된 파일도 기존 방식으로 읽힌다 |
| Trino 조회 | **영향 없음.** 자체 S3 클라이언트를 사용 |
| Job 동작 | append / expire snapshots / remove orphan files / rewrite manifests / Compaction **전부 정상** |

---

## 6. 남은 작업

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
