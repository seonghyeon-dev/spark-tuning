# Iceberg FileIO 전환 가이드 — HadoopFileIO(S3AFileSystem) → S3FileIO

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | expire_snapshots의 MinIO 부하 원인 검증 및 S3FileIO 전환 타당성/절차 정리 |
| 대상 독자 | 데이터 엔지니어, 운영팀, 스토리지 담당자 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1(카탈로그: HMS), Airflow 3.2.2 |
| 검증 기준 | Apache Iceberg `apache-iceberg-1.10.1` 태그 소스, Apache Hadoop `rel/release-3.4.1` 소스 |
| 최종 수정일 | 2026-08-24 |

### 근거 수준 라벨

| 라벨 | 의미 |
|------|------|
| ✅ 소스 검증 | 해당 버전의 실제 소스 코드로 확인한 사실 |
| 📘 공식 문서/관행 | 공식 문서 또는 널리 쓰이는 설정 관행 |
| ⚠️ 미검증 | 우리 환경에서 측정/확인이 필요한 항목 |

### 목차

- [0. 결론 요약](#0-결론-요약)
- [1. 현상 검증 — expire_snapshots가 MinIO에 요청을 쏟는 구조](#1-현상-검증--expire_snapshots가-minio에-요청을-쏟는-구조)
- [2. S3FileIO로 바꾸면 무엇이 달라지는가](#2-s3fileio로-바꾸면-무엇이-달라지는가)
- [3. 전환 명분 판단 — 대안과의 비교](#3-전환-명분-판단--대안과의-비교)
- [4. 사이드 이펙트 분석](#4-사이드-이펙트-분석)
- [5. 전환 방법](#5-전환-방법)
- [6. 검증 방법](#6-검증-방법)
- [7. 미확인 사항 및 후속 과제](#7-미확인-사항-및-후속-과제)
- [8. 참고 자료](#8-참고-자료)

---

## 0. 결론 요약

| 질문 | 답 |
|------|----|
| 확인한 내용이 맞는가 | **맞다. 그리고 추측보다 더 나쁘다.** S3A가 multi-delete를 지원하는 것도 사실이고, Iceberg가 그걸 안 쓰는 것도 사실이다. 게다가 Iceberg 코드상으로는 **bulk delete 경로를 타는 것처럼 보이지만** 내부 구현이 파일 1개씩 지운다 (섹션 1.2) |
| 전환 명분이 충분한가 | **충분하다.** 파일 1개 삭제에 요청 3개(HEAD + DELETE + LIST, 경우에 따라 PUT까지)를 쓰던 것이 250개당 요청 1개로 바뀐다. 요청 수 기준 **약 99.6% 감소**. 다른 대안(S3A 튜닝, 보존 기간 조정)으로는 이 수준이 안 나온다 (섹션 3) |
| 기존 테이블에 사이드 이펙트가 있는가 | **데이터/메타데이터 측면에서는 없다.** 기존 메타데이터에 박힌 `s3a://` 경로를 S3FileIO가 그대로 읽고 쓴다 (소스로 확인, 섹션 4.1). 마이그레이션 스크립트 불필요, 설정 되돌리면 즉시 롤백 |
| 그럼 위험은 어디에 있는가 | **데이터가 아니라 클라이언트 설정에 있다.** ① AWS SDK v2의 checksum 기본 동작과 MinIO 버전 궁합 (섹션 4.5, 최대 위험) ② 인증 정보 이원화 (섹션 4.4) ③ 읽기/쓰기 성능은 우리 환경에서 미측정 (섹션 4.7) |
| 어떻게 넘어가야 하는가 | **maintenance Job에만 먼저 적용**하고 append/Compaction은 나중에. FileIO는 Spark 세션(카탈로그) 단위 설정이라 Job별로 다르게 쓸 수 있고, 이게 blast radius를 가장 줄이는 순서다 (섹션 5) |

> **핵심 한 줄**: 이건 데이터 포맷 전환이 아니라 **S3 클라이언트 라이브러리 교체**다. 테이블은 그대로 있고, 누가 어떤 HTTP 요청을 보내느냐만 바뀐다.

---

## 1. 현상 검증 — expire_snapshots가 MinIO에 요청을 쏟는 구조

### 1.1 현재 우리가 쓰고 있는 FileIO 확인

`spark.sql.catalog.<카탈로그>.io-impl`을 명시하지 않으면 HMS 카탈로그는 **`HadoopFileIO`로 고정**된다. ✅ 소스 검증

```java
// HiveCatalog.java:119-123 (Iceberg 1.10.1)
String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
this.fileIO =
    fileIOImpl == null
        ? new HadoopFileIO(conf)
        : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);
```

`HadoopFileIO`는 경로 scheme에 맞는 Hadoop `FileSystem`을 찾아 쓰므로, `s3a://` 경로는 곧 `S3AFileSystem`이다. 즉 **`io-impl`을 설정한 적이 없다면 지금 상태가 HadoopFileIO + S3AFileSystem이 맞다.**

> 확인 방법은 섹션 6.1에 있다. 설정을 뒤지는 것보다 driver 로그 한 줄 보는 게 빠르다.

### 1.2 핵심 발견 — HadoopFileIO의 bulk delete는 "가짜"다

expire_snapshots의 삭제 분기는 이렇게 생겼다. ✅ 소스 검증

```java
// ExpireSnapshotsSparkAction.java:257-272 (Iceberg 1.10.1)
private ExpireSnapshots.Result deleteFiles(Iterator<FileInfo> files) {
  DeleteSummary summary;
  if (deleteFunc == null && table.io() instanceof SupportsBulkOperations) {
    summary = deleteFiles((SupportsBulkOperations) table.io(), files);   // ← bulk 경로
  } else {
    LOG.info("Table IO {} does not support bulk operations. Using non-bulk deletes.", ...);
    summary = deleteFiles(deleteExecutorService, table.io()::deleteFile, files);
  }
  ...
}
```

여기서 **`HadoopFileIO`는 `SupportsBulkOperations`를 구현한다.** 따라서 위 코드는 `if` 분기, 즉 "bulk 경로"를 탄다.

```java
// HadoopFileIO.java:48 (Iceberg 1.10.1)
public class HadoopFileIO implements HadoopConfigurable, DelegateFileIO {
// DelegateFileIO = FileIO + SupportsBulkOperations + SupportsPrefixOperations
```

문제는 그 `deleteFiles()`의 **내용물**이다.

```java
// HadoopFileIO.java:177-198 (Iceberg 1.10.1)
@Override
public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
  AtomicInteger failureCount = new AtomicInteger(0);
  Tasks.foreach(pathsToDelete)
      .executeWith(executorService())      // 스레드 풀로 병렬화할 뿐
      .retry(DELETE_RETRY_ATTEMPTS)
      ...
      .run(this::deleteFile);              // ← 결국 파일 1개씩
  ...
}

// HadoopFileIO.java:101-110
@Override
public void deleteFile(String path) {
  Path toDelete = new Path(path);
  FileSystem fs = Util.getFs(toDelete, getConf());
  fs.delete(toDelete, false /* not recursive */);   // ← S3A 단건 삭제
}
```

**`deleteFiles(경로 목록)`이라는 bulk 인터페이스를 받아놓고, 내부에서 스레드 풀로 단건 삭제를 흩뿌린다.** MinIO 입장에서는 `DeleteObjects`(multi-delete) 요청이 단 한 건도 오지 않고, `DeleteObject`가 파일 수만큼 온다.

> **팀원 추측 검증 결과**: 정확했다. 다만 이유가 조금 다르다.
> - "S3A가 multiple delete를 지원한다" → **맞다.** Hadoop 3.4.1에는 `BulkDeleteOperation`(`BulkDelete` API, HADOOP-18679)이 있고, `fs.delete(디렉터리, recursive=true)`도 내부적으로 multi-delete를 쓴다.
> - "expire_snapshots에서 여러 곳에 분포된 파일을 목록화해서 처리하지는 못할 것" → **맞다.** 단, 파일이 흩어져 있어서 못 하는 게 아니다. Iceberg는 **삭제 대상 전체 목록을 이미 손에 들고 있다**(Spark로 anti-join해서 만든다). 그걸 `Tasks.foreach(...).run(this::deleteFile)`로 풀어버리는 게 원인이다. 즉 **목록화는 되는데 Iceberg 1.10.1의 `HadoopFileIO`가 Hadoop의 bulk delete API를 호출하지 않는다.**

### 1.3 S3A에서 파일 1개 삭제 = MinIO 요청 3개

Hadoop 3.4.1 `S3AFileSystem.delete(path, false)`의 실제 흐름이다. ✅ 소스 검증

```java
// S3AFileSystem.java:3581-3606 (Hadoop 3.4.1)
protected boolean deleteWithoutCloseCheck(Path f, boolean recursive) throws IOException {
  ...
  boolean outcome = trackDuration(..., new DeleteOperation(
          storeContext,
          innerGetFileStatus(path, true, StatusProbeEnum.ALL),   // ① HEAD (없으면 LIST 추가)
          recursive, ..., pageSize, dirOperationsPurgeUploads));  // ② DeleteObject
  if (outcome) {
    maybeCreateFakeParentDirectory(path);                         // ③ + ④
  }
  ...
}

// S3AFileSystem.java:3624-3648
private void createFakeDirectoryIfNecessary(Path f) throws IOException, SdkException {
  String key = pathToKey(f);
  // we only make the LIST call; ...
  if (!key.isEmpty() && !s3Exists(f, StatusProbeEnum.DIRECTORIES)) {   // ③ LIST
    createFakeDirectory(key, putOptionsForPath(f));                     // ④ PUT (조건부)
  }
}
```

정리하면 **파일 1개를 지울 때마다**:

| 순번 | 요청 | 목적 | 발생 조건 |
|------|------|------|-----------|
| ① | `HeadObject` | 삭제 전 파일 존재/타입 확인 | 항상 |
| ② | `DeleteObject` | 실제 삭제 | 항상 |
| ③ | `ListObjectsV2` | 부모 "디렉터리"가 비었는지 확인 | 항상 |
| ④ | `PutObject` | 비었으면 가짜 디렉터리 마커 생성 | 파티션의 마지막 파일을 지웠을 때 |

**관측하신 "너무 많은 listObject, deleteObject"의 정체가 정확히 이 ③과 ②다.** ③의 LIST는 삭제와 아무 관계 없는, 오직 POSIX 디렉터리 시맨틱을 흉내 내기 위한 요청이다. 그리고 ④ 때문에 **expire_snapshots가 객체를 지우면서 동시에 객체를 만들기까지 한다.**

여기에 하나 더: 삭제는 **executor가 아니라 driver에서** 일어난다. ✅ 소스 검증

```java
// ExpireSnapshotsSparkAction.java:222-228
private ExpireSnapshots.Result doExecute() {
  if (streamResults()) {
    return deleteFiles(expireFiles().toLocalIterator());     // driver
  } else {
    return deleteFiles(expireFiles().collectAsList().iterator());  // driver
  }
}
```

삭제 대상 산출(anti-join)은 Spark 분산 처리지만, **삭제 요청 자체는 driver Pod 한 곳에서 전부 나간다.** 병렬도는 `iceberg.hadoop.delete-file-parallelism`(기본 `availableProcessors × 4`)이고, driver cpu가 2면 스레드 8개다. 요청 수는 많은데 동시성은 낮으니 **MinIO는 두들겨 맞고 우리 Job은 느린**, 가장 나쁜 조합이 된다. 현재 expire snapshots duration이 6~12분인 것과 일치한다.

### 1.4 요청 수 산식

```
현재(HadoopFileIO + S3A):
  MinIO 요청 수 ≈ 삭제 파일 수 × 3  (+ 파티션 수만큼의 PUT)

전환 후(S3FileIO):
  MinIO 요청 수 ≈ ceil(삭제 파일 수 ÷ s3.delete.batch-size)
                = ceil(삭제 파일 수 ÷ 250)   ← 기본값
                = ceil(삭제 파일 수 ÷ 1000)  ← 최대값으로 올릴 경우
```

⚠️ 미검증 — 실제 삭제 파일 수는 계측이 필요하다. 다만 규모 감을 잡기 위해, 하루 삭제 파일이 테이블당 2,000개이고 대상 테이블이 20개라면:

| 항목 | 현재 | S3FileIO (batch 250) | S3FileIO (batch 1000) |
|------|------|----------------------|------------------------|
| 삭제 대상 파일 | 40,000 | 40,000 | 40,000 |
| `DeleteObject` / `DeleteObjects` | 40,000 | 160 | 40 |
| `HeadObject` | 40,000 | 0 | 0 |
| `ListObjectsV2`(부모 확인용) | 40,000 | 0 | 0 |
| **총 요청** | **120,000** | **160** | **40** |
| 감소율 | — | **−99.87%** | **−99.97%** |

파일 수가 우리 실제 값과 다르더라도 **비율은 그대로다.** 요청 수가 파일 수에 선형 비례하다가 파일 수/250로 바뀌는 구조적 변화이기 때문이다.

### 1.5 부수 확인 — `max_concurrent_deletes`는 이미 무시되고 있다

혹시 튜닝 목적으로 `max_concurrent_deletes`를 넘기고 있다면, **그 값은 지금 적용되지 않는다.** ✅ 소스 검증

```java
// ExpireSnapshotsProcedure.java:133-143 (Iceberg 1.10.1)
if (maxConcurrentDeletes != null) {
  if (table.io() instanceof SupportsBulkOperations) {   // HadoopFileIO도 여기 해당
    LOG.warn(
        "max_concurrent_deletes only works with FileIOs that do not support bulk deletes. This "
            + "table is currently using {} which supports bulk deletes so the parameter will be ignored. ...");
  } else {
    action.executeDeleteWith(executorService(maxConcurrentDeletes, "expire-snapshots"));
  }
}
```

`HadoopFileIO`가 `SupportsBulkOperations`라고 자기 신고를 하는 바람에, **bulk도 안 되면서 동시성 조절 파라미터까지 막혀 있는 상태**다. 병렬도를 올리려면 Hadoop conf의 `iceberg.hadoop.delete-file-parallelism`을 써야 한다 — 다만 이건 요청 수를 줄여주지 않으므로 MinIO 부하 대책은 못 된다. 오히려 늘린다.

---

## 2. S3FileIO로 바꾸면 무엇이 달라지는가

### 2.1 진짜 bulk delete

`S3FileIO`는 `DelegateFileIO`를 구현하되, `deleteFiles()`를 **버킷별로 묶어 `DeleteObjects` API를 호출**한다. ✅ 소스 검증

```java
// S3FileIO.java:230-270 (Iceberg 1.10.1, 요약)
public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
  ...
  SetMultimap<String, String> bucketToObjects = ...;
  for (String path : paths) {
    S3URI location = new S3URI(path, ...);
    bucketToObjects.get(location.bucket()).add(location.key());
    if (bucketToObjects.get(bucket).size() == client.s3FileIOProperties().deleteBatchSize()) {
      deletionTasks.add(executorService().submit(() -> deleteBatch(client, bucket, keys)));
      bucketToObjects.removeAll(bucket);
    }
  }
  // 남은 것도 배치로 처리
  ...
}
// → deleteBatch()가 s3.deleteObjects(DeleteObjectsRequest) 호출 (S3FileIO.java:333-340)
```

| 속성 | 값 | 출처 |
|------|----|------|
| 배치 크기 기본값 | **250** | `S3FileIOProperties.DELETE_BATCH_SIZE_DEFAULT` (line 307) |
| 배치 크기 최대값 | **1000** | `S3FileIOProperties.DELETE_BATCH_SIZE_MAX` (line 313) |
| 설정 키 | `s3.delete.batch-size` | line 299 |
| 배치 처리 스레드 풀 | `s3.delete.num-threads`, 기본 `availableProcessors()` | line 395, `S3FileIO.java:465` |

삭제 전 `HeadObject`가 없고, 부모 디렉터리 확인 `ListObjectsV2`도 없다. **S3FileIO에는 디렉터리라는 개념 자체가 없기 때문이다.** 키를 지우면 그걸로 끝이다.

### 2.2 remove_orphan_files도 같은 이득을 본다

`DeleteOrphanFilesSparkAction`의 삭제 분기는 expire와 동일한 구조다(line 255-272). 즉 **orphan 파일 삭제도 지금은 파일당 요청 3개**이고, S3FileIO로 바꾸면 함께 배치화된다. maintenance 스케줄 재배치(`reprocessing-dag-design.md` §6.2)에서 orphan이 5~9분 걸리는 것도 같은 원인일 가능성이 높다. ⚠️ 미검증

추가 옵션이 하나 더 열린다: `remove_orphan_files`의 **`prefix_listing => true`** 파라미터다. ✅ 소스 검증 (`RemoveOrphanFilesProcedure.java:71-73, 147, 195`)

| 항목 | 내용 |
|------|------|
| 기본값 | `false` — Hadoop `FileSystem` 기반 디렉터리 재귀 순회 (디렉터리마다 LIST) |
| `true` | `SupportsPrefixOperations.listPrefix()` — **평면 `ListObjectsV2`** (1000 키/요청) |
| 전제 조건 | `table.io()`가 `SupportsPrefixOperations`여야 함 (`DeleteOrphanFilesSparkAction.java:308-317`) |

`hour(ts)` + `par_a` 파티셔닝이면 하루에 디렉터리가 24 × 4개씩 늘어난다. 3일치 스캔이면 디렉터리 수백 개 × LIST다. 평면 listing으로 바꾸면 이것도 크게 준다.

> 다만 이건 **이번 전환의 필수 항목이 아니다.** 기본값이 `false`라 S3FileIO로 바꿔도 listing 동작은 그대로다. 삭제 부하부터 잡고, orphan listing은 별도 건으로 측정 후 판단하는 걸 권한다 (섹션 7).

### 2.3 정량 비교표

| 항목 | HadoopFileIO + S3AFileSystem | S3FileIO |
|------|------------------------------|----------|
| 파일 1개 삭제 요청 수 | HEAD 1 + DELETE 1 + LIST 1 (+ 조건부 PUT 1) | 250개당 `DeleteObjects` 1 |
| bulk delete API 사용 | ❌ (인터페이스만 구현, 내부는 단건 루프) | ✅ |
| 삭제 실행 위치 | driver | driver (동일) |
| 삭제 병렬도 설정 | `iceberg.hadoop.delete-file-parallelism` (Hadoop conf) | `s3.delete.num-threads` |
| `max_concurrent_deletes` | 무시됨 (WARN 로그) | 무시됨 (WARN 로그) — 단 bulk라 불필요 |
| 삭제 중 객체 생성(마커 PUT) | 발생 | 없음 |
| prefix listing 지원 | 있음(`fs.listFiles` 재귀) | 있음(평면 `ListObjectsV2`) |
| SDK | AWS SDK v1/v2 혼재 계열, Hadoop 관리 | AWS SDK v2 (Iceberg 1.10.1 기준 BOM **2.33.0**) |

---

## 3. 전환 명분 판단 — 대안과의 비교

"S3FileIO 말고 다른 방법으로 부하를 줄일 수 있나"를 먼저 따져야 명분이 선다.

| 대안 | 효과 | 판정 |
|------|------|------|
| **A. `fs.s3a.directory.marker.retention=keep`** | 삭제 후 부모 디렉터리 마커 생성 로직(③ LIST + ④ PUT)을 억제. 파일당 요청 3~4 → 2 | ⚠️ 부분적. **DELETE 요청 수 자체는 그대로**다. 감소율 최대 약 33~50%. S3FileIO의 99.6%와는 자릿수가 다르다. 📘 |
| **B. snapshot 보존 기간/expire 주기 조정** | 한 번에 지우는 양을 나누거나 미룸 | ❌ 총 삭제량은 동일하다. 부하를 시간축으로 펴는 것뿐이고, 보존 3일은 재처리 DAG의 영수증 확인 전제라 줄일 수도 없다 (`reprocessing-dag-design.md` §8.3) |
| **C. `iceberg.hadoop.delete-file-parallelism` 상향** | 삭제가 빨리 끝남 | ❌ **역효과.** Job duration은 줄지만 MinIO 초당 요청은 오히려 늘어난다. 지금 문제는 총량과 순간 부하 둘 다다 |
| **D. Iceberg upstream 패치 (HadoopFileIO가 Hadoop BulkDelete API 사용)** | 근본적으로 옳음 | ❌ 실현 시점을 우리가 통제 못 한다. 커스텀 빌드 유지 부담도 크다 |
| **E. S3FileIO 전환** | 요청 수 −99.6%, 마커 PUT 소멸 | ✅ **채택** |

**전환 명분은 충분하다.** 근거는 세 가지다.

1. **개선폭이 자릿수 차이다.** 대안 A로 요청을 1/3 줄이는 것과, E로 1/250 줄이는 것은 같은 성격의 선택지가 아니다.
2. **되돌리는 비용이 거의 0이다.** 데이터 마이그레이션이 없다(섹션 4.1~4.2). 설정 한 줄을 빼면 원상복구된다. 이 정도로 롤백이 싼 변경에서 "일단 안전하게 유지"는 선택이 아니라 관성이다.
3. **Iceberg 생태계의 표준 경로다.** AWS/Cloudflare R2/MinIO 등 S3 호환 스토리지에서 Iceberg를 쓸 때 `S3FileIO`(또는 `ResolvingFileIO`)가 권장 구성이고, `HadoopFileIO`는 하위 호환용 기본값에 가깝다. 📘

### 3.1 명분에서 빼야 할 것

정직하게 짚으면, **읽기/쓰기 성능이 좋아진다는 보장은 없다.** ⚠️ 미검증
- S3A는 readahead, vectored IO, 입력 스트림 캐싱 등 수년간 축적된 읽기 최적화가 있다.
- S3FileIO의 이득은 **삭제와 메타데이터 연산**에 집중되어 있다.
- 따라서 append Job과 Compaction의 성능은 **전환 후 A/B로 확인해야 하는 항목**이지, 전환의 근거가 아니다. 다행히 우리에게는 `dcu/GB`라는 해상도 좋은 판정 지표가 이미 있다(`compaction-tuning-guide.md`).

---

## 4. 사이드 이펙트 분석

### 4.1 [핵심] 기존 `s3a://` 경로가 그대로 동작한다 ✅ 소스 검증

가장 큰 우려는 "테이블 메타데이터에 `s3a://`로 박힌 절대 경로를 S3FileIO가 못 읽으면 어쩌나"일 것이다. **문제없다.**

```java
// S3URI.java:60-91 (Iceberg 1.10.1)
/**
 * ...
 * <p>The URI supports any valid URI schemes to be backwards compatible with s3a and s3n, and also
 * allows users to use S3FileIO with other S3-compatible object storage services like GCS.
 */
S3URI(String location, Map<String, String> bucketToAccessPointMapping) {
  ...
  String[] schemeSplit = location.split(SCHEME_DELIM, -1);
  ValidationException.check(
      schemeSplit.length == 2, "Invalid S3 URI, cannot determine scheme: %s", location);
  this.scheme = schemeSplit[0];      // ← scheme을 화이트리스트 검증하지 않는다
  ...
}
```

**scheme을 보관만 하고 검증하지 않는다.** `s3a://bucket/key`는 bucket=`bucket`, key=`key`로 파싱되어 그대로 S3 API에 전달된다. `ResolvingFileIO`의 매핑 테이블도 같은 사실을 뒷받침한다.

```java
// ResolvingFileIO.java:60-67
private static final Map<String, String> SCHEME_TO_FILE_IO =
    ImmutableMap.of(
        "s3", S3_FILE_IO_IMPL,
        "s3a", S3_FILE_IO_IMPL,     // ← s3a도 S3FileIO로 보낸다
        "s3n", S3_FILE_IO_IMPL,
        ...);
```

따라서 다음이 **모두 불필요**하다:

- ❌ 기존 테이블의 metadata.json 재작성
- ❌ manifest 파일 경로 rewrite
- ❌ HMS의 테이블 location 변경 (`s3a://...` 유지)
- ❌ 테이블 재생성 / 데이터 재적재
- ❌ 기존 snapshot 무효화

### 4.2 혼용과 롤백이 안전하다

FileIO는 **읽는 쪽의 런타임 선택**일 뿐, 테이블에 "이 파일은 S3FileIO가 썼다" 같은 흔적을 남기지 않는다. 파일은 그냥 Parquet 객체다. 그래서:

- **혼용 가능**: append Job은 S3A, expire Job은 S3FileIO로 동시에 운영해도 된다. → 섹션 5의 단계적 전환이 성립하는 근거
- **롤백**: `io-impl` 설정을 제거하면 즉시 원복. 전환 중 S3FileIO로 쓴 파일도 S3A가 문제없이 읽는다
- **동시성**: 커밋 안전성은 여전히 HMS의 compare-and-swap이 보장한다. FileIO 교체는 여기에 관여하지 않는다 (`reprocessing-dag-design.md` §2.2의 전제는 유지된다)

### 4.3 Trino는 영향받지 않는다 ✅

Trino의 Iceberg 커넥터는 **자체 파일시스템 구현**(`fs.native-s3.enabled` 등)을 쓰며 Iceberg Java 라이브러리의 `FileIO` 설정을 공유하지 않는다. Spark 쪽 `io-impl`을 바꿔도 Trino 설정은 건드릴 필요가 없고 조회 동작도 변하지 않는다. `trino-query-guide.md`의 내용도 그대로 유효하다.

### 4.4 인증/엔드포인트 설정이 이원화된다 ⚠️ 주의

`S3FileIO`는 **`fs.s3a.*` 설정을 전혀 읽지 않는다.** 별도의 `s3.*` / `client.*` 프로퍼티 체계를 쓴다. 그렇다고 `fs.s3a.*`를 지우면 안 된다 — **Iceberg 테이블이 아닌 경로는 여전히 S3A가 담당**하기 때문이다.

| 경로 | 전환 후 담당 | 필요한 설정 |
|------|--------------|-------------|
| Iceberg 테이블 데이터/메타데이터 | **S3FileIO** | `s3.*`, `client.*` |
| 원천 avro 파일 읽기 | S3AFileSystem | `fs.s3a.*` (유지) |
| `get_jobs`가 올리는 경로 목록 텍스트 파일 | S3AFileSystem | `fs.s3a.*` (유지) |
| Spark event log / checkpoint (S3 사용 시) | S3AFileSystem | `fs.s3a.*` (유지) |

**즉 두 설정이 공존해야 한다.** 한쪽만 바꾸고 다른 쪽을 지우는 실수가 이 전환에서 가장 흔한 사고다.

### 4.5 [최대 위험] AWS SDK v2 checksum과 MinIO 버전 궁합 ⚠️ 미검증 — 반드시 사전 확인

Iceberg 1.10.1은 **AWS SDK v2 BOM 2.33.0**을 쓴다 ✅ (`gradle/libs.versions.toml`). SDK v2는 **2.30 이후 요청 checksum 계산이 기본 활성화**(`requestChecksumCalculation = WHEN_SUPPORTED`)되어, `PutObject`/`DeleteObjects` 등에 CRC32 체크섬 헤더/트레일러를 붙인다. 📘

**구버전 MinIO는 이 요청을 거부한다.** 증상은 보통 이렇게 나온다:

- `501 Not Implemented`
- `XAmzContentChecksumMismatch`
- `InvalidRequest` / `MissingContentMD5`

대응은 둘 중 하나다:

| 대응 | 방법 | 비고 |
|------|------|------|
| **MinIO 업그레이드** | CRC32 full-object checksum 지원 버전 이상으로 | 근본 해결. 스토리지 팀 협의 필요 |
| **SDK 동작 하향** | 환경변수 `AWS_REQUEST_CHECKSUM_CALCULATION=when_required`<br>`AWS_RESPONSE_CHECKSUM_VALIDATION=when_required`<br>(또는 JVM 옵션 `-Daws.requestChecksumCalculation=when_required`) | 즉시 적용 가능. Pod spec의 driver/executor 양쪽에 |

> **PoC의 첫 단계는 이것 하나만 확인하는 것이어야 한다.** 이게 막히면 나머지 계획이 전부 무의미하고, 통과하면 나머지는 대부분 설정 문제다. 확인 방법은 섹션 6.2.

### 4.6 staging 디렉터리와 K8s 로컬 볼륨 ⚠️ 확인 필요

**`s3.staging-dir`은 "업로드 전 파트 파일을 쌓아두는 로컬 디스크 버퍼"다.** S3FileIO는 파일을 쓸 때 메모리에 다 들고 있다가 한 번에 올리지 않는다. `multiPartSize`(기본 32MB)만큼 로컬 임시 파일에 채우고, 다 차면 다음 파일로 넘어가면서 채워진 파트를 multipart upload로 올린다. ✅ 소스 검증

```java
// S3OutputStream.java:213-228 (Iceberg 1.10.1)
private void newStream() throws IOException {
  ...
  currentStagingFile = File.createTempFile("s3fileio-", ".tmp", stagingDirectory);
  ...
  stagingFiles.add(new FileAndDigest(currentStagingFile, currentPartMessageDigest));
}
```

| 프로퍼티 | 기본값 | 의미 |
|----------|--------|------|
| `s3.staging-dir` | `java.io.tmpdir` (컨테이너에서는 보통 `/tmp`) | 파트 파일 임시 경로 |
| `s3.multipart.part-size-bytes` | 32MB | 파트 1개 크기 = 임시 파일 1개 크기 |
| `s3.multipart.threshold` | 1.5 | 이 배수를 넘으면 multipart 전환 |

**S3A에도 같은 개념이 있다.** `fs.s3a.fast.upload.buffer`의 기본값이 `disk`이고, 그때 쓰는 경로가 `fs.s3a.buffer.dir`다. 즉 **`s3.staging-dir` ≡ `fs.s3a.buffer.dir`**로 보면 된다. 📘

#### 우리 환경에서의 결론 — 명시하지 않는 것이 현상 유지다 ✅ 확인 완료

`fs.s3a.buffer.dir`의 기본값은 다음과 같다. ✅ 소스 검증 (`core-default.xml`, Hadoop 3.4.1)

```xml
<property>
  <name>fs.s3a.buffer.dir</name>
  <value>${env.LOCAL_DIRS:-${hadoop.tmp.dir}}/s3a</value>
  <description>... Yarn container path will be used as default value on yarn applications,
    otherwise fall back to hadoop.tmp.dir</description>
</property>
```

`LOCAL_DIRS`는 **YARN이 넣어주는 환경변수**이고 **Kubernetes에는 없다** — Spark on K8s는 `SPARK_LOCAL_DIRS`를 쓴다. 따라서 `hadoop.tmp.dir`(기본 `/tmp/hadoop-${user.name}`)로 폴백한다.

현재 환경을 대입하면 이렇다:

| 항목 | 실제 경로 | 근거 |
|------|-----------|------|
| 마운트한 hostPath 볼륨 `spark-local-dir-1` | `SPARK_LOCAL_DIRS` (shuffle / spill 전용) | Spark on K8s는 `spark-local-dir-` 접두 볼륨을 로컬 디렉터리로 자동 연결한다 ✅ (`LocalDirsFeatureStep.scala:39, 60, 84`) |
| `java.io.tmpdir` | **`/tmp`** (컨테이너 기본값) | `LocalDirsFeatureStep`은 `java.io.tmpdir`을 건드리지 않는다 ✅ (해당 문자열 자체가 없음) |
| `fs.s3a.buffer.dir` (현재, 미설정) | **`/tmp/hadoop-<user>/s3a`** | 위 폴백 |
| `s3.staging-dir` (전환 후, 미설정 시) | **`/tmp`** | `java.io.tmpdir` |

**즉 지금도 `/tmp`, 전환 후에도 `/tmp`다. 둘 다 명시하지 않으면 동작이 달라지지 않는다.** 마운트한 hostPath는 shuffle 전용이고 S3 업로드 버퍼와는 애초에 무관했다.

> **판단**: `s3.staging-dir`을 **명시하지 않는 것이 맞다.** 명시하면 오히려 지금과 다른 경로로 바뀌어 변경 폭이 늘어난다. 전환의 목적은 삭제 요청 감소이므로, 무관한 변수를 같이 건드리지 않는 편이 A/B 판정에도 유리하다.

#### 그래도 언젠가는 봐야 할 것 ⚠️

- **Phase 1(maintenance)에서는 완전히 무관하다.** `expire_snapshots`와 `remove_orphan_files`는 데이터 파일을 쓰지 않는다. 쓰는 것은 metadata.json 정도(KB~MB)로 multipart 임계값(48MB) 근처에도 못 간다. **staging 디렉터리를 아예 쓰지 않는다**
- **Phase 2(Compaction) / Phase 3(append)에서 실제 사용이 시작된다.** 이때 `/tmp`의 성격(overlay filesystem인지 emptyDir인지)과 ephemeral-storage limit을 확인해야 한다
- 다만 점유량은 생각보다 작다. 파트 파일은 업로드가 끝나는 즉시 삭제된다 ✅ (`S3OutputStream.java` — `whenComplete`에서 `Files.deleteIfExists(f.toPath())`). 즉 **파일 크기가 아니라 "동시에 떠 있는 파트 수 × 파트 크기"만큼**만 점유한다

### 4.7 읽기/쓰기 성능은 미측정이다 ⚠️

섹션 3.1에서 짚은 대로다. 특히 **Compaction의 `sort` 전략은 데이터를 2번 읽는다**(`compaction-tuning-guide.md`). 읽기 경로가 바뀌면 여기서 차이가 드러난다. 그래서 섹션 5의 전환 순서에서 Compaction을 maintenance보다 뒤에 둔다.

판정 기준은 기존 지표를 그대로 쓴다:

| 지표 | 기준 |
|------|------|
| `dcu/GB` | 주 지표. 현재 0.00219 |
| `초/GB` | 보조. 현재 2.41 |
| **노이즈 기준선** | **±15%** — 이보다 작은 차이는 판정 불가 (T4 vs T5 사례) |

### 4.8 기존 디렉터리 마커 객체가 남는다 (영향 미미)

S3A가 만들어 둔 0바이트 디렉터리 마커 객체는 전환 후에도 남는다. S3FileIO는 이를 만들지도 지우지도 않는다.

- 조회/쓰기에 영향 없음
- `remove_orphan_files`의 기본 경로(Hadoop listing)는 이를 파일로 보지 않으므로 영향 없음
- 단, 섹션 2.2의 **`prefix_listing => true`를 켜면 이 마커들이 목록에 나타날 수 있다** ⚠️ — orphan으로 오탐할 위험이 있으니 이 옵션은 별도 검증 후 도입한다

### 4.9 영향 없는 것들 체크리스트

| 항목 | 영향 |
|------|------|
| 테이블 스키마 / 파티션 스펙 / Sort Order | 없음 |
| 기존 snapshot 이력, time travel | 없음 |
| `write.distribution-mode=range`, `target-file-size-bytes` 등 테이블 프로퍼티 | 없음 |
| Compaction 전략(`sort`), `max-concurrent-file-group-rewrites` 등 확정 설정 | 없음 |
| 재처리 DAG의 batch_id 영수증(snapshot summary) 방식 | 없음 |
| HMS 카탈로그 커밋 동시성 | 없음 |
| Trino 조회 | 없음 |
| Airflow DAG 로직 | 없음 (Spark conf만 변경) |

---

## 5. 전환 방법

### 5.0 Phase 0 — 사전 준비 (변경 없음)

**① baseline 계측.** 전환 효과를 숫자로 말하려면 지금을 재둬야 한다.

```bash
# MinIO API 호출 실시간 관찰 (expire snapshots 실행 시간대에)
mc admin trace --verbose <alias> | grep -E "DeleteObject|ListObjects|HeadObject"

# 또는 audit log를 API별로 집계
```

기록할 값: expire snapshots 1회당 `DeleteObject` / `ListObjectsV2` / `HeadObject` 건수, Job duration, MinIO CPU/네트워크 피크.

**② jar 준비.** `iceberg-aws-bundle` 이 driver/executor classpath에 있어야 한다. 📘

| 항목 | 내용 |
|------|------|
| 필요 jar | `org.apache.iceberg:iceberg-aws-bundle:1.10.1` |
| 이유 | 기존 `iceberg-spark-runtime`에는 AWS SDK v2가 포함되어 있지 않다 |
| 배치 방법 | Spark 이미지의 `$SPARK_HOME/jars/`에 포함 (K8s 환경에서 `--packages`로 매번 받는 것은 비권장) |
| 주의 | 버전을 Iceberg와 **정확히 일치**시킬 것 |

**③ MinIO checksum 호환성 확인** (섹션 4.5). 이게 Phase 0의 실질적 게이트다.

### 5.1 설정 값

```properties
# ── Iceberg FileIO (신규) ─────────────────────────────────────
spark.sql.catalog.<카탈로그>.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.<카탈로그>.s3.endpoint=http://<minio-service>:9000
spark.sql.catalog.<카탈로그>.s3.path-style-access=true
spark.sql.catalog.<카탈로그>.client.region=us-east-1

# ── 삭제 배치 (이번 전환의 목적) ──────────────────────────────
spark.sql.catalog.<카탈로그>.s3.delete.batch-size=1000
spark.sql.catalog.<카탈로그>.s3.delete.num-threads=8

# ── 설정하지 않는 것 ──────────────────────────────────────────
#   s3.staging-dir       : 현재도 /tmp, 미설정 시 전환 후에도 /tmp → 현상 유지 (섹션 4.6)
#   s3.acl               : 익명 읽기/쓰기를 여는 값이므로 옮기지 않는다 (섹션 5.1.1)
#   s3.access-key-id 등  : Spark UI에 노출되므로 환경변수로 (아래)

# ── Phase 2(Compaction) 진입 시 함께 (섹션 5.1.3) ─────────────
#   spark.sql.catalog.<카탈로그>.s3.multipart.part-size-bytes=67108864

# ── 기존 S3A 설정은 그대로 유지 (섹션 4.4) ────────────────────
spark.hadoop.fs.s3a.endpoint=...
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.access.key=...
spark.hadoop.fs.s3a.secret.key=...
```

| 설정 | 값 | 근거 |
|------|----|------|
| `io-impl` | `org.apache.iceberg.aws.s3.S3FileIO` | 이번 전환의 본체 |
| `s3.path-style-access` | `true` | MinIO는 virtual-host 스타일 미지원이 일반적. 기본값이 `false`라 **반드시 명시** ✅ (`S3FileIOProperties.java:240`) |
| `client.region` | 아무 값 (`us-east-1`) | AWS SDK v2는 region이 없으면 클라이언트 생성 자체가 실패한다. MinIO는 값을 무시한다 📘 |
| `s3.delete.batch-size` | `1000` | 최대값. 250이 기본이나 요청 수를 4배 더 줄인다. MinIO의 DeleteObjects 한도도 1000 ✅ (`DELETE_BATCH_SIZE_MAX`) |
| `s3.delete.num-threads` | `8` | 기본값이 `availableProcessors()`라 K8s에서 예측이 어렵다(섹션 5.1.2). IO bound이므로 코어 수와 무관하게 명시한다 |

**인증 정보 주입 방식** — `s3.access-key-id` / `s3.secret-access-key`를 Spark conf에 직접 쓰는 것은 **비권장**한다. Spark UI의 Environment 탭에 그대로 노출된다. 대신 K8s Secret → 환경변수를 쓴다. AWS SDK v2의 기본 자격증명 체인이 자동으로 읽는다. 📘

```yaml
# SparkApplication spec (driver / executor 양쪽)
env:
  - name: AWS_ACCESS_KEY_ID
    valueFrom: { secretKeyRef: { name: <minio-secret>, key: access-key } }
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom: { secretKeyRef: { name: <minio-secret>, key: secret-key } }
  - name: AWS_REGION
    value: us-east-1
  # 섹션 4.5 대응이 필요한 경우에만
  - name: AWS_REQUEST_CHECKSUM_CALCULATION
    value: when_required
  - name: AWS_RESPONSE_CHECKSUM_VALIDATION
    value: when_required
```

> **`ResolvingFileIO` 대안**: `io-impl`을 `org.apache.iceberg.io.ResolvingFileIO`로 두면 scheme별로 위임한다(`s3a` → S3FileIO, 그 외 → HadoopFileIO). 나중에 다른 스토리지가 섞일 가능성이 있으면 이쪽이 유연하다. 지금은 전부 `s3a://` 단일이므로 **어느 쪽을 써도 동작은 같다.** 명시성을 위해 `S3FileIO` 직접 지정을 권한다.

### 5.1.1 기존 `fs.s3a.*` 설정 대응표

현재 쓰고 있는 S3A 설정을 어떻게 처리할지 정리한다. **공통 원칙은 "기존 `fs.s3a.*`는 그대로 두고, 필요한 것만 `s3.*`에 추가"**다 (섹션 4.4).

| 기존 설정 | S3FileIO 대응 | 판정 |
|-----------|---------------|------|
| `fs.s3a.endpoint` | `s3.endpoint` (스킴 포함) | 이전 |
| `fs.s3a.path.style.access=true` | `s3.path-style-access=true` | 이전 (기본값이 `false`라 필수) |
| `fs.s3a.access.key` / `fs.s3a.secret.key` | 환경변수 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` 권장 | 이전 (방식 변경) |
| `fs.s3a.connection.ssl.enabled=false` | **없음** | **불필요** — `s3.endpoint`에 `http://` 포함으로 해결 |
| `fs.s3a.aws.credentials.provider=SimpleAWSCredentialsProvider` | **없음** | **불필요** — Iceberg 자체 순서를 따름 |
| `fs.s3a.acl.default=PublicReadWrite` | `s3.acl=public-read-write` | **옮기지 말 것** — 아래 참조 |
| `fs.s3a.buffer.dir` | `s3.staging-dir` | 이전 (섹션 4.6) |
| `fs.s3a.connection.maximum` | `http-client.apache.max-connections` 등 | 필요 시 (기본값으로 시작) |

#### `fs.s3a.connection.ssl.enabled=false`

엔드포인트에 스킴이 없을 때 http/https 중 무엇을 쓸지 정하는 설정이다. ✅ 소스 검증

```java
// DefaultS3ClientFactory.java:358-371 (Hadoop 3.4.1)
boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);
String protocol = secureConnections ? "https" : "http";
...
if (!endpoint.contains("://")) {        // ← 스킴이 이미 있으면 그대로 사용
  endpoint = String.format("%s://%s", protocol, endpoint);
}
```

즉 `fs.s3a.endpoint`를 `minio:9000`처럼 스킴 없이 적었기 때문에 필요했던 설정이다. S3FileIO에는 대응 프로퍼티가 없고 필요도 없다 — **`s3.endpoint=http://minio:9000`으로 끝난다.**

#### `fs.s3a.aws.credentials.provider=SimpleAWSCredentialsProvider`

S3A가 자격증명을 어디서 읽을지 고정하는 설정이다. `SimpleAWSCredentialsProvider`는 `fs.s3a.access.key` / `fs.s3a.secret.key`**만** 보며, 환경변수·IAM role·EC2 메타데이터는 보지 않는다. 기본값이 여러 provider의 체인이라, 명시하면 "설정에 적힌 키만 쓴다"로 못박고 불필요한 메타데이터 조회도 없앤다.

**S3FileIO에는 1:1 대응을 만들 필요가 없다.** Iceberg가 자체 순서를 따른다. ✅ 소스 검증 (`AwsClientProperties.java:211-235`)

| 순위 | 조건 | 결과 |
|------|------|------|
| 1 | `s3.access-key-id` + `s3.secret-access-key` 존재 | `StaticCredentialsProvider` (= SimpleAWSCredentialsProvider와 동등) |
| 2 | `client.credentials-provider` 지정 | 해당 클래스를 동적 로드 |
| 3 | 아무것도 없음 | `DefaultCredentialsProvider` (환경변수 → 시스템 프로퍼티 → 프로파일 → 컨테이너/IMDS) |

**권장은 3번 경로**다. K8s Secret → 환경변수만 주고 Iceberg 쪽 설정은 비워두면 체인 첫 단계에서 잡히므로 IMDS 조회 같은 건 발생하지 않는다.

> **함정**: `SimpleAWSCredentialsProvider`를 `client.credentials-provider`에 그대로 넣으면 안 된다. 그것은 **Hadoop 클래스**이고, Iceberg는 SDK v2의 `AwsCredentialsProvider` 구현을 요구한다 (`AwsClientProperties.java:285-291`에서 타입 검사 후 예외).

#### `fs.s3a.acl.default=PublicReadWrite` ⚠️ 보안 재검토 대상

S3A가 객체 생성 시 붙이는 **canned ACL**이다. `PublicReadWrite`는 canned ACL 중 가장 개방적인 값으로, **익명 사용자에게 읽기와 쓰기를 모두 허용**한다. 실제 AWS S3였다면 심각한 노출이다.

지금 문제가 되지 않는 이유는 **MinIO가 S3 ACL을 사실상 지원하지 않기** 때문일 가능성이 높다. MinIO는 접근 제어를 IAM/버킷 정책으로 하며 ACL 헤더는 무시하거나 제한적으로만 처리한다. 즉 **이 설정이 현재 아무 효과도 내지 못하고 있을 공산이 크다.** ⚠️ 우리 MinIO에서 확인 필요

**전환 시 권장: 옮기지 말고 제거를 검토한다.** Iceberg는 `s3.acl`이 없으면 ACL 헤더를 붙이지 않는다 ✅ (`S3FileIOProperties.java:215-216` — "If not set, ACL will not be set for requests"). 제거 후 문제가 생긴다면(다른 시스템이 익명 접근으로 읽고 있었다면) 그때 추가한다. 애초에 왜 설정됐는지 히스토리 확인을 권한다.

굳이 옮긴다면 **표기법이 다르다.** Iceberg는 SDK v2 enum value를 요구한다 ✅ (`S3FileIOProperties.java:638-641`).

```properties
# ❌ s3.acl=PublicReadWrite   → ObjectCannedACL.fromValue()가 UNKNOWN_TO_SDK_VERSION을 리턴
#                               → Preconditions 검증 실패로 예외
# ✅ s3.acl=public-read-write
```

#### `client.region`은 실질적으로 필수다

표기에 주의한다. `s3.` 접두어가 아니라 **`client.region`**이다 (`spark.sql.catalog.<카탈로그>.client.region`).

Iceberg 프로퍼티로서는 선택이지만, region 자체는 어딘가에서 반드시 해결되어야 한다. ✅ 소스 검증

```java
// AwsClientProperties.java:145-149
void applyClientRegionConfiguration(BuilderT builder) {
  if (clientRegion != null) {        // ← 없으면 아예 설정하지 않는다
    builder.region(Region.of(clientRegion));
  }
}
```

주지 않으면 SDK 기본 region 체인(`AWS_REGION` → `aws.region` → `~/.aws/config` → 컨테이너/EC2 메타데이터)이 돌고, **전부 실패하면 클라이언트 생성 시점에 `SdkClientException: Unable to load region from any of the providers in the chain`으로 죽는다.** K8s Pod에는 저 중 아무것도 없으므로 `client.region` 또는 `AWS_REGION` 중 **하나는 반드시 줘야 한다.**

**S3A에서는 왜 필요 없었나** — S3A가 대신 채워주고 있었다. ✅ 소스 검증

```
// DefaultS3ClientFactory.java:258-263 (Hadoop 3.4.1, 주석)
 * If region is configured via fs.s3a.endpoint.region, use it.
 * If no region is configured, try to parse region from endpoint.
 * If no region is configured, and it could not be parsed from the endpoint,
 *     set the default region as US_EAST_2 and enable cross region access.
```

값 자체는 무엇이든 상관없다(MinIO는 검증하지 않고 서명 계산에만 쓴다). 관례상 `us-east-1`. ⚠️ 단 MinIO에 `MINIO_SITE_REGION`(구 `MINIO_REGION`)이 설정돼 있다면 **그 값과 맞춰야** 서명 불일치를 피한다.

### 5.1.2 maintenance Job 리소스 산정

maintenance Job에서 **executor와 driver의 역할이 다르다.** ✅ 소스 검증

| 단계 | 실행 위치 | 근거 |
|------|-----------|------|
| 삭제 대상 산출 (manifest 스캔 + anti-join) | **executor** (분산) | `BaseSparkAction.java:151-170` — manifest 목록을 `repartition` 후 `flatMap(new ReadManifest(...))`로 병렬 읽기 |
| 실제 삭제 요청 | **driver 단독** | `ExpireSnapshotsSparkAction.java:222-228` — `collectAsList()` / `toLocalIterator()` 후 driver JVM |

따라서 **executor를 0으로 둘 수 없다.** manifest 스캔이 실제 Spark Job이므로 executor가 없으면 진행되지 않는다. 다만 읽는 것이 데이터 파일이 아니라 **manifest**이므로, Compaction처럼 데이터 양에 비례해 키울 이유는 없다.

#### 현재 설정과 권장

| 설정 | 현재 | Phase 1 권장 | 근거 |
|------|------|--------------|------|
| `driver cores` | 1 | **1 유지** | 삭제는 네트워크 IO bound다. `s3.delete.num-threads`를 명시하면 코어 1개로도 동시 요청 8개를 충분히 흘린다 |
| `driver memory` | 1g | **1g 유지** | `collectAsList()`가 담는 것은 경로 문자열이다. 파일 4만 개라도 수십 MB 수준 |
| `executor cores` | 4 | **4 유지** | 전환 A/B의 교란 변수를 만들지 않는다 |
| `executor instances` | 4 | **4 유지 → 측정 후 조정** | 아래 참조 |
| `s3.delete.num-threads` | — | **8 (명시)** | 기본값이 `availableProcessors()`라 예측이 어렵다 (아래 참조) |

**`driver cores`를 올리지 않아도 되는 이유**: 전환 후 삭제 요청은 파일 4만 개 기준 `DeleteObjects` 40건(batch 1000)이다. 스레드 8개면 5라운드, 요청당 1~2초로 잡아도 10초 안쪽이다. 현재 6~12분에 비하면 무시할 수준이라 **코어를 늘려 얻을 것이 없다.** (스레드 풀 크기는 코어 수와 무관하게 지정할 수 있고, 네트워크 대기 중인 스레드는 CPU를 쓰지 않는다.)

> ⚠️ **`availableProcessors()`의 함정 — 현재 상태 점검용**
>
> `driver cores = 1`은 K8s의 **request**이지 **limit**이 아니다. JVM의 컨테이너 인식은 cgroup의 CPU **quota**(= limit)를 읽으므로, `coreLimit`(`spark.kubernetes.driver.limit.cores`)을 설정하지 않았다면 **`availableProcessors()`가 노드 전체 코어 수를 반환한다.**
>
> 그렇다면 현재 `iceberg.hadoop.delete-file-parallelism`의 기본값(`availableProcessors × 4`)이 노드 코어가 32일 때 **128 스레드**가 된다. 각 스레드가 파일 1개씩, 요청 3개씩 쏘고 있다는 뜻이다. **관측된 MinIO 부하 급증의 원인 중 하나일 수 있다.** ⚠️ driver Pod spec의 `limits.cpu` 유무를 확인할 것
>
> 어느 쪽이든 결론은 같다 — **`s3.delete.num-threads`는 기본값에 맡기지 말고 명시한다.**

#### executor 수는 전환 후에 조정한다

지금 줄이면 FileIO 전환과 리소스 변경이 겹쳐 A/B 판정이 불가능해진다. 전환 후 Spark UI에서 다음을 보고 판단한다:

| 확인 대상 | 보는 법 |
|-----------|---------|
| executor 구간 | `ReadManifest` flatMap stage와 `except`(shuffle) stage의 소요 시간 |
| **driver 삭제 구간** | **stage로 잡히지 않는다.** Job 전체 duration − stage 소요 시간 합계 = 삭제 시간. Spark UI상 Job이 끝난 것처럼 보이는데 Pod이 살아 있는 구간이 이것이다 |
| idle cores | DataFlint. executor 구간이 짧은데 16코어를 잡고 있으면 축소 대상 |

전환 후에는 삭제 구간이 거의 사라지므로 **manifest 스캔 구간이 duration의 대부분**이 된다. 그때 idle cores를 보고 `instances`를 4 → 2로 줄이는 식으로 접근한다. `contentFileDS`가 `spark.sql.shuffle.partitions`로 repartition하므로 태스크 수 자체는 부족하지 않다.

### 5.1.3 multipart 업로드 설정 — 기본값이 S3A와 다르다

`fs.s3a.multipart.*`를 설정한 적이 없더라도 **대응되는 기본값은 존재하며, Iceberg의 기본값과 다르다.** ✅ 소스 검증

| 항목 | S3A (현재, 미설정 시) | S3FileIO (전환 후, 미설정 시) |
|------|----------------------|------------------------------|
| 파트 크기 | `fs.s3a.multipart.size` = **64MB** | `s3.multipart.part-size-bytes` = **32MB** |
| multipart 전환 임계값 | `fs.s3a.multipart.threshold` = **128MB** | `s3.multipart.threshold` = 1.5 (배수) → **48MB** |
| 업로드 스레드 | `fs.s3a.threads.max` 등 | `s3.multipart.num-threads` = `availableProcessors()` |

출처: `core-default.xml`(Hadoop 3.4.1), `S3FileIOProperties.java:195-206, 603`.

#### 32MB가 하는 일

```java
// S3OutputStream.java:183-208 (요약)
while (stream.getCount() + remaining > multiPartSize) {   // 32MB 채워지면
  ...
  newStream();      // 새 staging 파일로 회전
  uploadParts();    // 완성된 파트를 비동기 업로드
}
```

1. 쓰기 데이터가 staging 파일에 쌓인다
2. 32MB가 차면 새 staging 파일로 넘어가고, 방금 채운 파일을 **비동기로 `UploadPart` 전송**한다 (`CompletableFuture.supplyAsync(..., executorService)`, 풀 이름 `iceberg-s3fileio-upload-%d`)
3. 업로드가 끝나면 **staging 파일을 즉시 삭제**한다
4. `close()` 시점에 남은 파트를 올리고 `CompleteMultipartUpload`

즉 **업로드가 쓰기와 겹쳐서 진행된다.** 파일을 다 쓴 뒤 통째로 올리는 방식이 아니다.

#### 성능을 올릴 수 있는가

**가능은 하지만, 이번 전환의 이득 영역은 아니다.** 판단 근거를 순서대로 정리하면:

| 질문 | 답 |
|------|-----|
| Phase 1(maintenance)에 영향이 있나 | **없다.** expire/orphan은 데이터 파일을 쓰지 않으므로 multipart 경로 자체를 타지 않는다 |
| Phase 2/3에서는 | 512MB 목표 파일 기준 **32MB면 16파트, 64MB면 8파트**. 파트 수가 절반이면 `UploadPart` 요청도 절반이다 |
| 그럼 올려야 하나 | 올린다기보다 **`64MB`로 맞추는 것이 "현상 유지"다.** S3A가 지금 64MB로 동작 중이므로, 기본값을 그대로 두면 오히려 파트가 절반 크기로 쪼개지는 변경이 된다 |
| 체감될 만한 차이인가 | ⚠️ 불확실. 노이즈 기준선 ±15%를 넘길지는 측정해야 안다. Compaction 튜닝에서 확인된 병목은 file group 분할이었지 업로드가 아니었다 |

**권장**: Phase 2 진입 시 `s3.multipart.part-size-bytes=67108864`(64MB)를 함께 설정해 **현재 S3A 동작과 동일하게 맞춘다.** 그 이상의 튜닝(128MB 등)은 A/B로 별도 검증한다.

```properties
# Phase 2(Compaction) 진입 시
spark.sql.catalog.<카탈로그>.s3.multipart.part-size-bytes=67108864   # 64MB, S3A 현재값과 동일
```

> 파트를 키울 때의 대가: 업로드 실패 시 재전송 단위가 커지고, `동시 파트 수 × 파트 크기`만큼 로컬 디스크 점유가 늘어난다. S3의 파트 수 상한(10,000개)은 32MB 기준 320GB라 우리 파일 크기에서는 고려 대상이 아니다.

### 5.2 Phase 1 — maintenance Job에만 적용 (권장 시작점)

**expire_snapshots / remove_orphan_files를 실행하는 Spark Job의 conf에만** 위 설정을 넣는다. append DAG과 Compaction DAG은 손대지 않는다.

이 순서를 쓰는 이유:

| 이유 | 설명 |
|------|------|
| **문제가 있는 곳이 정확히 여기다** | MinIO 부하의 원인이 maintenance의 대량 삭제다. 효과가 가장 크고 즉시 측정된다 |
| **blast radius가 최소다** | maintenance는 실패해도 데이터 정합성에 영향이 없다. 다음 날 다시 돌면 된다. append가 실패하면 Job History 상태 관리까지 얽힌다 |
| **읽기 성능 리스크가 없다** | expire는 메타데이터만 읽는다. 대용량 Parquet 스캔이 없어 섹션 4.7의 미지수를 피해 간다 |
| **혼용이 안전하다** | 섹션 4.2에서 확인한 대로, 같은 테이블을 서로 다른 FileIO가 다뤄도 문제없다 |

검증 항목은 섹션 6. **최소 1주일 운영**하며 지켜본다.

### 5.3 Phase 2 — Compaction DAG

Phase 1이 안정되면 확대한다. 여기서부터는 **대용량 읽기/쓰기가 걸리므로 성능 A/B가 필수**다.

| 단계 | 내용 |
|------|------|
| 1 | hourly Compaction 1개 테이블에만 적용 |
| 2 | 최소 5회 측정 — `dcu/GB`, `초/GB` 비교 |
| 3 | 판정: **±15% 이내면 "차이 없음"**. 개선/악화 판정은 그 밖에서만 |
| 4 | 출력 파일 크기 분포 확인 (`384MB 미만 파일 3개 이상`이 모니터링 기준) |
| 5 | 이상 없으면 hourly 전체 → daily 확대 |

> Compaction 확대 시점에 `daily Compaction의 rewrite-all 낭비` 확인(작업 5 후속 과제)을 같이 하면 측정을 한 번에 끝낼 수 있다.

### 5.4 Phase 3 — append DAG

가장 마지막이다. 5분 주기로 도는 Job이라 문제가 생기면 즉시 누적된다.

- 적용 전 `spark-tuning-guide.md`의 벤치마크 재실행
- 적용 후 최소 하루치 관찰 — Job History `FAILURE` 건수 추이가 1차 지표

> **Phase 3를 반드시 해야 하는 건 아니다.** append는 삭제를 거의 하지 않으므로 이번 전환의 이득이 없다. **설정 일관성**이 목적이라면 진행하고, 이득이 없다고 판단하면 append는 S3A로 남겨도 된다. 섹션 4.2에서 확인한 대로 혼용은 안전하다.

### 5.5 롤백 절차

```
1. 해당 Job의 spark conf에서 io-impl 및 s3.* 설정 제거
2. Job 재실행
```

데이터 정리 작업 없음. 전환 중 S3FileIO로 쓴 파일도 HadoopFileIO가 그대로 읽는다. **되돌리기 비용이 사실상 0이라는 점이 이 전환의 가장 큰 장점이다.**

---

## 6. 검증 방법

### 6.1 어떤 FileIO가 실제로 쓰이고 있는지 확인

**방법 A — driver 로그** (가장 확실)

```bash
# 전환 전이라면 이 WARN이 보인다 (max_concurrent_deletes를 넘기는 경우)
kubectl logs <driver-pod> | grep "max_concurrent_deletes only works with FileIOs"
# → "... This table is currently using org.apache.iceberg.hadoop.HadoopFileIO ..."

# 전환 후: 같은 WARN의 클래스명이 S3FileIO로 바뀐다
```

**방법 B — Spark SQL로 직접 확인**

```sql
-- Spark shell / notebook
spark.sql("SELECT 1").show()
-- Scala/PySpark에서:
--   spark.sessionState.catalogManager.catalog("<카탈로그>")
--     .asInstanceOf[org.apache.iceberg.spark.SparkCatalog]
--   → 로드한 테이블의 io() 클래스명 확인
```

**방법 C — Spark UI Environment 탭**에서 `spark.sql.catalog.<카탈로그>.io-impl` 유무 확인 (설정 여부만 알 수 있고, 실제 로드 성공 여부는 알 수 없다)

### 6.2 MinIO 요청 패턴 확인 (핵심 검증)

expire snapshots 실행 중에:

```bash
mc admin trace --verbose <alias> | grep -E "DeleteObject|DeleteObjects|ListObjects|HeadObject"
```

| 기대 결과 | 판정 |
|-----------|------|
| `DeleteObjects`(복수형) 요청이 보이고, `DeleteObject`(단수형)가 거의 없음 | ✅ 전환 성공 |
| `DeleteObject` 단수형이 여전히 대량 | ❌ io-impl이 적용되지 않음 → 6.1 재확인 |
| `HeadObject` / `ListObjectsV2`가 삭제 건수만큼 발생 | ❌ 여전히 S3A 경로 |
| `501` / `XAmzContentChecksumMismatch` | ⚠️ 섹션 4.5 checksum 문제 |

### 6.3 데이터 정합성 확인

전환 전후로 동일해야 하는 값들:

```sql
-- ① snapshot 이력
SELECT committed_at, snapshot_id, operation FROM <카탈로그>.<db>.<table>.snapshots
ORDER BY committed_at DESC LIMIT 20;

-- ② 파일 수와 총 크기
SELECT count(*) AS file_count, sum(file_size_in_bytes) AS total_bytes
FROM <카탈로그>.<db>.<table>.files;

-- ③ 파티션별 집계 (expire 대상 밖 파티션은 변동이 없어야 한다)
SELECT partition, record_count, file_count FROM <카탈로그>.<db>.<table>.partitions
ORDER BY partition;

-- ④ 실제 row 수 (샘플 파티션)
SELECT count(*) FROM <카탈로그>.<db>.<table> WHERE ts >= ... AND ts < ...;
```

추가로 **Trino에서 같은 쿼리를 실행해 동일 결과**가 나오는지 확인한다 (섹션 4.3 검증).

### 6.4 성공 판정 기준

| 지표 | 목표 |
|------|------|
| expire snapshots의 MinIO 요청 수 | **99% 이상 감소** |
| expire snapshots duration | 6~12분 → 유의미하게 단축 ⚠️ (요청 수 감소분만큼 줄 것으로 예상하나 미검증) |
| MinIO CPU / 네트워크 피크 | 전환 전 대비 하락 |
| 데이터 정합성 (6.3) | 전부 일치 |
| Trino 조회 | 변화 없음 |

---

## 7. 미확인 사항 및 후속 과제

| 항목 | 내용 | 우선순위 |
|------|------|----------|
| **MinIO 버전과 SDK 2.33 checksum 호환성** | Phase 0의 게이트. 이게 막히면 MinIO 업그레이드가 선행되어야 한다 (섹션 4.5) | **최우선** |
| **실제 삭제 파일 수** | 섹션 1.4의 추정치를 실측으로 대체해야 보고가 된다. `mc admin trace` 또는 driver 로그의 `Deleted N total files` | 높음 |
| **읽기/쓰기 성능 영향** | Compaction `dcu/GB` A/B (섹션 5.3). 노이즈 기준선 ±15% | 높음 |
| **`s3.delete.num-threads` 적정값** | 기본값(driver 코어 수)은 너무 작다. 8로 시작하되 MinIO 순간 부하를 보며 조정 | 중간 |
| **`remove_orphan_files`의 `prefix_listing => true`** | LIST 요청을 추가로 크게 줄일 수 있으나, 기존 S3A 디렉터리 마커 오탐 위험 검증 필요 (섹션 2.2, 4.8) | 중간 |
| **`fs.s3a.acl.default=PublicReadWrite`의 실제 효력** | MinIO에서 ACL이 무시되는지 확인. 무시된다면 전환과 무관하게 제거 대상이고, 효력이 있다면 익명 읽기/쓰기가 열려 있다는 뜻이라 더 시급하다 (섹션 5.1.1) | **높음(보안)** |
| **driver Pod의 `limits.cpu` 설정 여부** | 없다면 `availableProcessors()`가 노드 전체 코어를 반환해 현재 삭제 스레드가 예상보다 훨씬 많을 수 있다 — MinIO 부하 급증의 원인 후보 (섹션 5.1.2) | 높음 |
| **maintenance Job의 executor 수** | 삭제는 driver에서만 일어나므로 축소 여지가 있으나, **전환과 동시에 바꾸면 A/B 판정이 불가능하다.** 전환 후 Spark UI로 stage 구간을 보고 조정 (섹션 5.1.2) | 중간 (전환 후) |
| **`/tmp`의 성격과 ephemeral-storage limit** | Phase 1에는 무관(데이터 파일을 쓰지 않음). Phase 2/3 진입 전 확인 (섹션 4.6) | 중간 (Phase 2 전) |
| **`s3.multipart.part-size-bytes` 조정 효과** | 64MB로 맞추면 S3A 현재 동작과 동일해진다. 그 이상은 A/B 필요 (섹션 5.1.3) | 낮음 (Phase 2) |
| **Spark 4.1.1 ↔ iceberg-spark-runtime 4.0 조합** | 현재 어떤 runtime jar를 쓰는지 확인하고 `iceberg-aws-bundle` 버전을 정확히 맞출 것 | 중간 |
| **maintenance 스케줄 재조정** | expire duration이 줄면 `reprocessing-dag-design.md` §6.2의 슬롯에 여유가 생긴다. 재배치 재검토 가능 | 낮음 (전환 후) |
| **daily maintenance DAG 통합** | 기존 후속 과제. FileIO 전환과 함께 하면 측정을 한 번에 끝낼 수 있다 | 낮음 |

---

## 8. 참고 자료

### 검증에 사용한 소스 (버전 고정)

| 파일 | 위치 | 확인 내용 |
|------|------|-----------|
| `ExpireSnapshotsSparkAction.java` | `iceberg 1.10.1` / `spark/v4.0/.../actions/` | :222-228 driver 삭제, :257-272 bulk 분기 |
| `HadoopFileIO.java` | `iceberg 1.10.1` / `core/.../hadoop/` | :48 `DelegateFileIO`, :101-110 단건 삭제, :177-198 가짜 bulk |
| `S3FileIO.java` | `iceberg 1.10.1` / `aws/.../s3/` | :88-92 인터페이스, :230-300 배치 삭제, :465 스레드 풀 |
| `S3FileIOProperties.java` | `iceberg 1.10.1` / `aws/.../s3/` | :299-313 batch size, :230-258 endpoint/인증, :212 staging |
| `S3URI.java` | `iceberg 1.10.1` / `aws/.../s3/` | :60-91 scheme 무검증 (s3a 호환) |
| `ResolvingFileIO.java` | `iceberg 1.10.1` / `core/.../io/` | :60-67 scheme 매핑 |
| `HiveCatalog.java` | `iceberg 1.10.1` / `hive-metastore/.../hive/` | :119-123 기본 FileIO |
| `ExpireSnapshotsProcedure.java` | `iceberg 1.10.1` / `spark/v4.0/.../procedures/` | :133-143 `max_concurrent_deletes` 무시 |
| `RemoveOrphanFilesProcedure.java` | `iceberg 1.10.1` / `spark/v4.0/.../procedures/` | :71-73, :195 `prefix_listing` |
| `DeleteOrphanFilesSparkAction.java` | `iceberg 1.10.1` / `spark/v4.0/.../actions/` | :255-272 동일 삭제 분기, :308-330 listing 분기 |
| `S3AFileSystem.java` | `hadoop 3.4.1` / `hadoop-tools/hadoop-aws/` | :3581-3606 delete 흐름, :3624-3648 마커 생성 |
| `BulkDeleteOperation.java` | `hadoop 3.4.1` / `hadoop-tools/hadoop-aws/.../impl/` | S3A의 bulk delete API 존재 확인 |
| `AwsClientProperties.java` | `iceberg 1.10.1` / `aws/.../aws/` | :145-149 region 미설정 시 동작, :211-235 자격증명 결정 순서 |
| `S3OutputStream.java` | `iceberg 1.10.1` / `aws/.../s3/` | :183-208 파트 회전, :213-228 staging 파일 생성, `uploadParts()` 비동기 업로드 및 업로드 후 삭제 |
| `LocalDirsFeatureStep.scala` | `spark 4.0.0` / `resource-managers/kubernetes/` | :39, :60, :84 `spark-local-dir-*` 볼륨 → `SPARK_LOCAL_DIRS`. `java.io.tmpdir`은 건드리지 않음 |
| `BaseSparkAction.java` | `iceberg 1.10.1` / `spark/v4.0/.../actions/` | :151-170 manifest 병렬 스캔(executor 작업) |
| `DefaultS3ClientFactory.java` | `hadoop 3.4.1` / `hadoop-tools/hadoop-aws/` | :258-263 region 폴백(US_EAST_2), :358-371 ssl.enabled |
| `core-default.xml` | `hadoop 3.4.1` / `hadoop-common/` | `fs.s3a.buffer.dir`·`hadoop.tmp.dir` 기본값, `fast.upload.buffer=disk`, `multipart.size=64M`, `multipart.threshold=128M` |
| `gradle/libs.versions.toml` | `iceberg 1.10.1` | `awssdk-bom = 2.33.0`, `hadoop3 = 3.4.1` |

### 공식 문서

- Iceberg AWS Integration: https://iceberg.apache.org/docs/latest/aws/
- Iceberg Spark Configuration: https://iceberg.apache.org/docs/latest/spark-configuration/
- Iceberg Spark Procedures (`expire_snapshots`, `remove_orphan_files`): https://iceberg.apache.org/docs/latest/spark-procedures/
- Hadoop S3A Connector: https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/index.html
- Hadoop S3A Directory Markers: https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/directory_markers.html
- AWS SDK for Java v2 — Checksums: https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html

### 관련 내부 문서

- `pipeline/reprocessing-dag-design.md` §6.2 — maintenance 스케줄 배치 (expire 6~12분, orphan 5~9분)
- `tuning/compaction-tuning-guide.md` — `dcu/GB` 판정 지표, 노이즈 기준선 15%
- `schema/trino-query-guide.md` — Trino 조회 (본 전환의 영향 없음)
