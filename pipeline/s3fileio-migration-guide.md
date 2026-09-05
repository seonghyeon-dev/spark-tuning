# Iceberg FileIO 전환 가이드 — HadoopFileIO(S3AFileSystem) → S3FileIO

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | expire_snapshots의 MinIO 부하 원인 검증 및 S3FileIO 전환 타당성/절차 정리 |
| 대상 독자 | 데이터 엔지니어, 운영팀, 스토리지 담당자 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Iceberg 1.10.1(카탈로그: HMS), Airflow 3.2.2 |
| ⚠️ Spark 런타임 | **실측: Spark 3.5.8 / Scala 2.12 / Hadoop 3.3.4** (driver Pod의 jar 기준, 섹션 5.0.2). Spark 4에서 maintenance 함수 오류로 **임시 다운그레이드 중**이며 추후 Spark 4로 복귀 예정 |
| 검증 기준 | Apache Iceberg `apache-iceberg-1.10.1` 태그 소스(**`spark/v3.5`·`spark/v4.0` 모듈 동일 로직 확인**), Apache Hadoop `rel/release-3.3.4`·`rel/release-3.4.1` 소스 |
| 최종 수정일 | 2026-08-24 |

### 근거 수준 라벨

| 라벨 | 의미 |
|------|------|
| ✅ 소스 검증 | 해당 버전의 실제 소스 코드로 확인한 사실 |
| 📘 공식 문서/관행 | 공식 문서 또는 널리 쓰이는 설정 관행 |
| ⚠️ 미검증 | 우리 환경에서 측정/확인이 필요한 항목 |

### 목차

- [0. 결론 요약](#0-결론-요약)
- [1. 현상 검증 — expire_snapshots가 MinIO에 요청을 쏟는 구조](#1-현상-검증--expire_snapshots가-minio에-요청을-쏟는-구조) — **1.0 배경 개념(보고용)**
- [2. S3FileIO로 바꾸면 무엇이 달라지는가](#2-s3fileio로-바꾸면-무엇이-달라지는가)
- [3. 전환 명분 판단 — 대안과의 비교](#3-전환-명분-판단--대안과의-비교)
- [4. 사이드 이펙트 분석](#4-사이드-이펙트-분석)
- [5. 전환 방법](#5-전환-방법)
- [6. 검증 방법](#6-검증-방법) — 6.5 실측 결과
- [7. 미확인 사항 및 후속 과제](#7-미확인-사항-및-후속-과제)
- [8. 참고 자료](#8-참고-자료)
- [9. 부록 — Iceberg 1.11.0 / Spark 4.1 업그레이드 검토](#9-부록--iceberg-1110--spark-41-업그레이드-검토)

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

### 1.0 배경 개념 — `io-impl`이란 무엇이고, 왜 설정이 두 벌 필요한가

> 보고 시 이 절만 읽어도 전환의 의미가 전달되도록 정리한다.

#### `FileIO`는 Iceberg가 스토리지와 대화하는 "통역사"다

Iceberg 테이블은 결국 **오브젝트 스토리지에 놓인 파일 묶음**이다 — metadata.json, manifest list, manifest, 그리고 데이터 파일(Parquet). Iceberg가 이 파일들을 다루려면 스토리지에 접근해야 한다.

**`FileIO`는 그 "파일을 읽고·쓰고·지우는 동작"을 추상화한 인터페이스다.** 실질적인 메서드는 몇 개 되지 않는다.

```java
InputFile  newInputFile(String path);    // 읽기
OutputFile newOutputFile(String path);   // 쓰기
void       deleteFile(String path);      // 삭제
void       deleteFiles(Iterable<String> paths);  // 일괄 삭제 (SupportsBulkOperations)
Iterable<FileInfo> listPrefix(String prefix);    // 목록     (SupportsPrefixOperations)
```

**`io-impl`은 "이 인터페이스의 구현체로 무엇을 쓸 것인가"를 지정하는 설정이다.** 테이블도 데이터도 바뀌지 않는다. **누가 어떤 방식으로 S3에 요청을 보내느냐만 바뀐다.**

| | `HadoopFileIO` (기본값) | `S3FileIO` |
|---|---|---|
| 실제 통신 경로 | Iceberg → **Hadoop `FileSystem`** → `S3AFileSystem` → AWS SDK v1 → S3 | Iceberg → AWS SDK v2 → S3 |
| 추상화 단계 | **2단** | **1단** |
| 설정 네임스페이스 | `fs.s3a.*` | `s3.*`, `client.*` |
| 일괄 삭제 | 인터페이스는 있으나 **내부는 단건 루프** (섹션 1.2) | `DeleteObjects` API 실사용 |

#### 그럼 기존에는 왜 `s3a`를 썼나

두 가지다.

1. **선택한 것이 아니라 기본값이다.** `io-impl`을 설정하지 않으면 HMS 카탈로그는 **무조건** `HadoopFileIO`를 쓴다 ✅ (`HiveCatalog.java:119-123`, 섹션 1.1). 즉 "S3A를 골랐다"기보다 **"아무것도 고르지 않아서 기본값이 됐다"**가 정확하다.
2. **Spark 생태계의 오래된 표준 경로다.** Hive 시절부터 Spark에서 S3에 접근하는 표준은 S3A였고, 지금도 **원천 avro 읽기는 이 방식**이다. 나쁜 선택이 아니었다.

다만 S3A의 목적은 **"S3를 파일시스템처럼 보이게 하는 것"**이다. 오브젝트 스토리지에는 디렉터리가 없는데, POSIX 디렉터리 시맨틱을 흉내 낸다. 그 흉내의 비용이 이번 문제였다.

```
S3A의 파일 1개 삭제 = HEAD(존재 확인) + DELETE + LIST(부모가 비었나?) + PUT(비었으면 마커 생성)
```

**Iceberg는 디렉터리가 필요 없다.** 지울 파일의 전체 경로 목록을 이미 손에 들고 있다. 그런데 S3A를 거치면 매번 디렉터리 확인 비용을 낸다. 관측된 `listObject` 폭주의 정체가 이것이다 (섹션 1.3).

#### 왜 `s3.*` 설정을 새로 넣어야 하나

**`S3FileIO`는 Hadoop을 전혀 사용하지 않기 때문이다.** AWS SDK v2로 S3와 직접 통신하므로 Hadoop 설정(`fs.s3a.*`)을 읽을 방법 자체가 없다. 엔드포인트·인증정보·path-style 여부를 **자기 네임스페이스로 다시 알려줘야 한다.**

이것은 "설정 중복"이 아니라 **"서로 다른 두 클라이언트에게 각각 알려주는 것"**이다.

#### ⚠️ 그리고 `fs.s3a.*`는 지우면 안 된다 — 구조적인 이유

**`io-impl`은 Iceberg 테이블에만 적용된다.** 우리 시스템에서 S3에 접근하는 주체는 둘이다.

```
Airflow DAG
  │
  ├── Spark가 직접 읽음:  s3a://.../원천.avro         ← S3AFileSystem  (fs.s3a.*)
  │        spark.read.format("avro").load(...)
  │        └─ Iceberg를 거치지 않는다. Spark DataSource가 Hadoop FileSystem을 직접 호출
  │
  └── Iceberg가 읽고 씀:  s3a://.../warehouse/TABLE_A  ← S3FileIO      (s3.*, client.*)
           df.writeTo("catalog.db.TABLE_A").append()
           └─ io-impl이 적용되는 것은 여기뿐
```

| 접근 주체 | 대상 | 클라이언트 | 필요한 설정 |
|-----------|------|-----------|-------------|
| **Iceberg** | 테이블의 데이터·메타데이터 파일 | `S3FileIO` (전환 후) | `s3.*`, `client.*` |
| **Spark 자체** | 원천 avro, 경로 목록 텍스트 파일 | `S3AFileSystem` | `fs.s3a.*` |

`spark.read.format("avro").load("s3a://...")`는 Iceberg를 거치지 않는다. **`io-impl`은 이 경로에 아무런 영향이 없다.** 따라서 `fs.s3a.*`를 지우면 **원천 avro를 읽지 못해 append가 실패한다.**

> **결론: 두 설정이 공존하는 것이 맞다. 과도기적 중복이 아니라 구조적으로 그렇다.** 원천 avro를 Iceberg가 아닌 Spark가 직접 읽는 한 계속 그렇다.

#### 관리 포인트를 하나로 줄이는 방법 (선택)

값 자체는 같은 MinIO를 가리키므로, **인증정보만큼은 한 곳에서 관리**할 수 있다. Hadoop 3.3.4의 `fs.s3a.aws.credentials.provider` 기본 체인에 `EnvironmentVariableCredentialsProvider`가 포함되어 있다 ✅ (`core-default.xml`).

```xml
<value>
  org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider,
  org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,
  com.amazonaws.auth.EnvironmentVariableCredentialsProvider,   ← 이것
  org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider
</value>
```

즉 `fs.s3a.aws.credentials.provider` 설정을 **제거해 기본값으로 되돌리면**, S3A도 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` 환경변수를 읽는다. S3FileIO(SDK v2)도 같은 환경변수를 읽으므로 **K8s Secret 하나로 양쪽이 커버된다.**

엔드포인트는 여전히 `fs.s3a.endpoint`와 `s3.endpoint` 두 벌이 필요하다. ⚠️ 단 이 변경도 동작을 바꾸므로 **A/B가 끝난 뒤 별건으로** 적용한다.

### 1.0.1 자주 나오는 질문 (회의 대응)

#### Q1. S3A는 왜 SDK v1을 쓰고 S3FileIO는 v2를 쓰나

**둘 다 AWS SDK for Java다. 버전 선택의 문제가 아니라 만들어진 시점의 문제다.**

| | 등장 시점 | 당시 상황 | 결과 |
|---|---|---|---|
| **S3A** (Hadoop) | 2010년대 초 | AWS SDK v1(`com.amazonaws.*`)만 존재 | v1 위에 구현 |
| **S3FileIO** (Iceberg) | 2021년경 | v2(`software.amazon.awssdk.*`)가 이미 표준 | 처음부터 v2 |

AWS SDK v2는 2018년 GA된 **완전히 새로 쓴 라이브러리**다. 패키지명·API·논블로킹 IO 지원이 전부 달라 v1과 호환되지 않는다. Hadoop은 이 마이그레이션을 오래 미루다 **3.4.0에서야 v2로 전환**했다.

```
hadoop-aws 3.3.4  →  com.amazonaws:aws-java-sdk-bundle      (v1)  ← 현재 우리 환경
hadoop-aws 3.4.x  →  software.amazon.awssdk:bundle          (v2)
```
✅ 소스 검증 (`hadoop-aws-3.3.4.pom`, `hadoop-aws-3.4.1.pom`)

**즉 "S3A는 v1을 고집한다"가 아니라 "우리가 쓰는 Hadoop 3.3.4가 v2 전환 이전 버전이다"가 정확하다.** Spark 4.1(Hadoop 3.4.2)로 올라가면 S3A도 v2가 되어 이 차이는 사라진다 (섹션 9.4-①).

#### Q2. SDK는 AWS SDK를 말하는 것이고, FileIO에 포함된 라이브러리인가

**AWS SDK for Java가 맞다. 그리고 FileIO에 포함되어 있지 않다.**

`iceberg-aws` 모듈(= `S3FileIO` 코드)은 SDK를 **참조만 하고 포함하지 않는다.** 그래서 별도로 `iceberg-aws-bundle`을 넣어야 했고, 넣지 않았을 때 `NoClassDefFoundError`가 난 것이다 (섹션 5.0.1~5.0.2).

| jar | 담고 있는 것 | 누가 쓰나 |
|-----|-------------|-----------|
| `iceberg-spark-runtime` | Iceberg 코드 (`iceberg-aws` = **S3FileIO 코드** 포함) — **SDK 없음** | Iceberg 전반 |
| `iceberg-aws-bundle` | **AWS SDK v2** (s3, kms, glue, dynamodb, sts …) | `S3FileIO`가 호출 |
| `aws-java-sdk-bundle` | **AWS SDK v1** | `hadoop-aws`(S3A)가 호출 |

비유하자면 `S3FileIO`는 **"SDK를 사용하는 코드"**이고 SDK는 **"실제로 HTTP 요청을 만들어 보내는 라이브러리"**다. 설계도와 부품의 관계이며, 부품은 따로 조달해야 한다.

#### Q3. avro read할 때만 `HadoopFileIO`가 필요한 것 아닌가

**용어를 하나 분리해야 한다. `HadoopFileIO`와 `S3AFileSystem`은 다른 것이다.**

| | 정체 | 소속 | 전환 후 |
|---|---|---|---|
| `HadoopFileIO` | Iceberg의 **FileIO 구현체** 중 하나 | Iceberg | **더 이상 안 쓴다** (`S3FileIO`로 교체됨) |
| `S3AFileSystem` | Hadoop의 **FileSystem 구현체** | Hadoop | **계속 쓴다** (avro read 등) |

**avro read는 `HadoopFileIO`를 거치지 않는다.** Iceberg를 아예 거치지 않기 때문이다.

```
[Iceberg 계층]                                                    ┌─ AWS SDK v1 ─┐
  FileIO ─┬─ HadoopFileIO ──→ Hadoop FileSystem ─→ S3AFileSystem ─┤              │
          │   (전환 전)                                            │              ├─→ MinIO
          └─ S3FileIO ──────────────────────────→ AWS SDK v2 ─────┼──────────────┘
              (전환 후)                                            │
[Spark 계층]                                                      │
  DataSource(avro) ──────────→ Hadoop FileSystem ─→ S3AFileSystem ┘
```

즉 정확한 문장은 **"avro read에 필요한 것은 `HadoopFileIO`가 아니라 `S3AFileSystem`(과 `fs.s3a.*` 설정)이다"**가 된다. 전환으로 사라지는 것은 `HadoopFileIO`이고, `S3AFileSystem`은 그대로 남는다.

#### Q4. maintenance Job은 avro를 안 읽는데 `fs.s3a.*`가 필요한가

**필요하다. 그것도 Job이 무엇을 하는지와 전혀 무관하게 필요하다.**

이유는 Iceberg가 아니라 **Spark 자체**에 있다. `fs.s3a.*`를 제거하고 실행하면 다음 오류로 실패한다.

```
ERROR SparkContext: Error initializing SparkContext.
java.nio.file.AccessDeniedException: s3a://bucket/logs/spark:
  org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException:
  No AWS Credentials provided by TemporaryAWSCredentialsProvider ...
```

**`Error initializing SparkContext` — Iceberg 코드가 실행되기 한참 전에 죽는다.** 원인은 `spark.eventLog.dir`이 `s3a://` 스킴이기 때문이다. ✅ 소스 검증

```scala
// SparkContext.scala:627-633 (Spark 3.5.8) — SparkContext 생성자 내부
_eventLogger =
  if (isEventLogEnabled) {
    val logger = new EventLoggingListener(_applicationId, _applicationAttemptId,
                                          _eventLogDir.get, _conf, _hadoopConfiguration)
    logger.start()          // ← 여기서 로그 파일을 실제로 생성한다
    ...
```

```scala
// EventLoggingListener.scala:76-83
/** Creates the log file in the configured log directory. */
def start(): Unit = {
  logWriter.start()
  initEventLog()
}
```

`logWriter.start()`가 Hadoop `FileSystem`으로 `s3a://bucket/logs/spark/` 아래에 이벤트 로그 파일을 만든다. 자격증명이 없으면 `NoAuthWithAWSException` → **SparkContext 생성 실패 → Job이 시작조차 못 한다.**

> ⚠️ **따라서 "Job별로 필요 여부가 다르다"는 판단은 성립하지 않는다.** `spark.eventLog.enabled=true`이고 `spark.eventLog.dir`이 `s3a://`인 한, **append든 expire든 orphan이든 Compaction이든 전부 `fs.s3a.*`가 필요하다.** Iceberg 계층의 분석(아래)은 그보다 한 단계 아래 이야기이고, 애초에 거기까지 도달하지 못한다.

참고로 `spark.history.fs.logDirectory`는 **History Server(별도 프로세스)**가 읽는 설정이라 이 오류의 직접 원인은 아니다. 다만 같은 위치를 가리키므로 History Server 쪽에도 S3A 자격증명이 필요하다.

##### 참고 — Iceberg 계층만 놓고 보면 (실무적 결론은 위가 우선한다)

`spark.eventLog.dir`을 로컬이나 다른 스토리지로 옮긴다고 가정했을 때, Iceberg 코드가 Hadoop `FileSystem`을 쓰는지는 Job마다 다르다. ✅ 소스 검증

| Job | Iceberg가 Hadoop `FileSystem`을 쓰나 | 근거 |
|-----|------------------------------------|------|
| append | **쓴다** — 원천 avro read | Spark DataSource → Hadoop FS |
| `expire_snapshots` | **안 쓴다** | `hadoopConf`·`FileSystem` 참조가 소스에 **0건** |
| `remove_orphan_files` | **쓴다** | `usePrefixListing` 기본값 `false` → `listDirRecursivelyWithHadoop` (`DeleteOrphanFilesSparkAction.java:118, 124, 329`) |
| Compaction | 데이터 입출력은 FileIO 경유 | ⚠️ 미검증 |

**그래도 Job별로 설정을 갈라놓지 않는다.** eventLog 문제로 어차피 전부 필요하고, 설령 그것을 해결하더라도 얻는 것이 없다.

- **비용이 0이다.** `S3AFileSystem`은 `s3a://` 경로에 실제로 접근할 때만 인스턴스화된다. 미사용 설정은 자원을 쓰지 않는다
- **관리상 손해다.** 카탈로그 설정 템플릿이 Job별로 분기되면 나중에 `prefix_listing`을 켜거나 구성이 바뀔 때 조용히 깨진다

#### Q5. Iceberg manifest도 avro인데, 그것 때문에 `fs.s3a.*`나 `spark-avro`가 필요한 것 아닌가

**아니다. manifest avro는 완전히 다른 경로로 읽는다.** ✅ 소스 검증

```java
// BaseSparkAction.java:419-429 (ReadManifest)
public CloseableIterator<FileInfo> entries(ManifestFileBean manifest) {
  FileIO io = table.getValue().io();                        // ← FileIO를 통해 읽는다
  ...
  return CloseableIterator.transform(
      ManifestFiles.read(manifest, io, specs).select(proj).iterator(), ...);
}
```

| | 원천 avro (append 입력) | Iceberg manifest (avro) |
|---|---|---|
| 읽는 주체 | **Spark DataSource** | **Iceberg 자체 reader** (`org.apache.iceberg.avro`) |
| 경로 | Spark → Hadoop `FileSystem` → `S3AFileSystem` | Iceberg → `FileIO` → **`S3FileIO`** (전환 후) |
| `spark-avro` 필요? | **필요** | **불필요** — Avro 라이브러리는 `iceberg-spark-runtime`에 이미 포함 |
| `fs.s3a.*` 필요? | **필요** | **불필요** |

즉 **"avro 파일이니까 같은 경로로 읽는다"가 아니다.** 포맷이 같을 뿐, 읽는 주체와 스토리지 접근 경로가 다르다. `expire_snapshots`가 manifest를 대량으로 읽으면서도 `hadoopConf` 참조가 0건인 이유가 바로 이것이다.

#### Q6. `spark.eventLog.dir`의 스킴을 `s3a://`에서 `s3://`로 바꾸면 S3A를 뺄 수 있나

**안 된다. 여기에는 개념 혼동이 하나 있다.**

##### `s3.*`(Iceberg 프로퍼티)와 `s3://`(URI 스킴)은 전혀 다른 것이다

| | 정체 | 읽는 주체 |
|---|---|---|
| `s3.endpoint`, `s3.path-style-access` … | **Iceberg 카탈로그 프로퍼티** | `S3FileIO` |
| `s3://`, `s3a://` | **URI 스킴** | Hadoop `FileSystem`이 `fs.<scheme>.impl`로 구현체를 찾음 |

이름이 비슷할 뿐 **아무 관계가 없다.** URI 스킴을 바꾼다고 해서 Spark가 `S3FileIO`를 쓰게 되지는 않는다.

##### Spark의 eventLog는 `FileIO`라는 개념 자체를 모른다

`spark.eventLog.dir`을 처리하는 것은 Spark 코어이고, Spark 코어는 **Hadoop `FileSystem`만 안다.** Iceberg 라이브러리를 사용하지 않으므로 `S3FileIO`가 존재한다는 사실조차 모른다. 스킴을 무엇으로 바꾸든 Hadoop `FileSystem` 계층에서 해결된다.

##### 그리고 Hadoop 3.x에는 `s3://` 구현체가 없다 ✅ 소스 검증

`core-default.xml`(Hadoop 3.3.4)에는 `fs.s3a.impl`만 정의되어 있고 **`fs.s3.impl`은 존재하지 않는다.**

```xml
<property>
  <name>fs.s3a.impl</name>
  <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
</property>
```

구형 `s3://`(S3 block filesystem)와 `s3n://`은 **Hadoop 3.0에서 제거**됐다(`core-default.xml`에 관련 클래스 참조 **0건**). 따라서 `spark.eventLog.dir=s3://...`로 바꾸면 `No FileSystem for scheme "s3"`로 실패한다.

억지로 `fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`을 설정하면 동작은 하지만 **결국 S3A로 되돌아오고 `fs.s3a.*` 설정을 그대로 읽는다.** 이름만 바뀌고 얻는 것이 없다.

##### S3A를 완전히 제거할 수 있나 — 없다

| 의존 지점 | 제거 가능? |
|-----------|-----------|
| `spark.eventLog.dir` | 이론상 가능 — 로컬/PVC로 옮기면 된다. 다만 History 중앙 수집을 잃는다 |
| **원천 avro read** | **불가능** |

`spark.read.format("avro").load("s3a://...")`는 **Spark DataSource**이고, Spark DataSource는 Hadoop `FileSystem`만 사용한다. **Iceberg의 `S3FileIO`를 Spark DataSource에 끼워 넣는 방법은 없다** — 서로 다른 프로젝트의 서로 다른 인터페이스다.

> **원천 데이터를 Spark로 S3에서 읽는 한 S3A는 남는다.** Spark 4.1(Hadoop 3.4.2)로 올라가도 마찬가지다. SDK만 v2로 통일될 뿐 구조는 그대로다.

##### "설정이 2벌"이 아니라 "클라이언트가 2개"다

한 JVM 안에서 S3에 말을 거는 라이브러리가 둘이고, 둘은 **설정을 공유하지 않는다.** 같은 서버에서 도는 애플리케이션 두 개가 같은 DB를 쓰더라도 각자 커넥션 설정을 갖는 것과 같다.

##### 실질적으로 줄이는 방법 — 자격증명은 한 곳으로 통합된다

중복의 실체는 **자격증명 2벌**인데, 이것은 없앨 수 있다 (섹션 1.0 말미).

**현재**

```properties
# S3A
spark.hadoop.fs.s3a.endpoint=minio:9000
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.access.key=<KEY>                    # ← 자격증명 ①
spark.hadoop.fs.s3a.secret.key=<SECRET>                 # ←
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider

# Iceberg
spark.sql.catalog.<카탈로그>.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.<카탈로그>.s3.endpoint=http://minio:9000
spark.sql.catalog.<카탈로그>.s3.path-style-access=true
spark.sql.catalog.<카탈로그>.s3.access-key-id=<KEY>      # ← 자격증명 ②
spark.sql.catalog.<카탈로그>.s3.secret-access-key=<SECRET>
spark.sql.catalog.<카탈로그>.client.region=us-east-1
```

**통합 후**

```yaml
# K8s Secret → 환경변수 (driver/executor 양쪽) — 자격증명은 여기 한 곳뿐
env:
  - name: AWS_ACCESS_KEY_ID     { secretKeyRef: ... }
  - name: AWS_SECRET_ACCESS_KEY { secretKeyRef: ... }
  - name: AWS_REGION            value: us-east-1
```

```properties
# S3A — fs.s3a.aws.credentials.provider를 "제거"하면 기본 체인이 환경변수를 읽는다
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.path.style.access=true

# Iceberg — 자격증명/region 미지정 시 SDK v2 기본 체인이 환경변수를 읽는다
spark.sql.catalog.<카탈로그>.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.<카탈로그>.s3.endpoint=http://minio:9000
spark.sql.catalog.<카탈로그>.s3.path-style-access=true
```

**자격증명은 한 곳(K8s Secret)으로 통합되고, 남는 중복은 endpoint 1줄뿐이다.** `connection.ssl.enabled`는 endpoint에 `http://`를 명시하면 불필요하고(섹션 5.1.1), `client.region`도 `AWS_REGION`으로 대체된다.

##### K8s Secret 등록과 참조

Secret은 **네임스페이스 단위 리소스**다. Spark Job이 실행되는 네임스페이스에 만들어야 하고, 다른 네임스페이스에서는 보이지 않는다 (dev/prod가 분리돼 있다면 각각 만든다).

```bash
# 방법 ① kubectl 명령 (빠름)
kubectl create secret generic minio-credentials \
  --namespace=<spark-job-네임스페이스> \
  --from-literal=access-key='<ACCESS_KEY>' \
  --from-literal=secret-key='<SECRET_KEY>'
```

```yaml
# 방법 ② YAML manifest (GitOps/버전관리 시)
apiVersion: v1
kind: Secret
metadata:
  name: minio-credentials
  namespace: <spark-job-네임스페이스>
type: Opaque
stringData:            # stringData를 쓰면 base64 인코딩을 직접 하지 않아도 된다
  access-key: "<ACCESS_KEY>"
  secret-key: "<SECRET_KEY>"
```

SparkApplication에서 참조할 때는 **driver와 executor 양쪽 모두**에 넣는다. executor도 manifest·데이터 파일을 읽으므로 자격증명이 필요하다.

```yaml
spec:
  driver:
    env: &awsEnv
      - name: AWS_ACCESS_KEY_ID
        valueFrom: { secretKeyRef: { name: minio-credentials, key: access-key } }
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom: { secretKeyRef: { name: minio-credentials, key: secret-key } }
      - name: AWS_REGION
        value: us-east-1
  executor:
    env: *awsEnv
```

확인:

```bash
kubectl get secret minio-credentials -n <네임스페이스>
kubectl exec <driver-pod> -- printenv | grep AWS_        # 값이 주입됐는지
```

> ⚠️ **base64는 암호화가 아니다.** Secret YAML(`stringData` 포함)을 그대로 git에 올리면 평문 노출과 다름없다. 매니페스트를 버전관리한다면 SealedSecrets·External Secrets Operator·Vault 같은 도구를 쓰거나, Secret만 `kubectl create`로 별도 관리한다.

⚠️ 이 정리는 **동작을 바꾸는 변경**이므로 다른 변경과 같은 배포에 섞지 말고 단독으로 적용해 확인한다 (성능과는 무관하므로 측정을 기다릴 필요는 없다).

#### Q7. 스킴(`s3://` / `s3a://` / `s3n://`)은 무엇이고 성능 차이가 있나

##### "S3AFileSystem은 s3a, S3FileIO는 s3"가 아니다

앞의 절반만 맞다.

| | 스킴에 묶여 있나 |
|---|---|
| `S3AFileSystem` | **묶여 있다** — `fs.s3a.impl`로 `s3a://`에 매핑된다 |
| `S3FileIO` | **묶여 있지 않다** |

**증거는 우리 환경 자체다.** 전환 후에도 테이블 경로는 여전히 `s3a://`인데 `S3FileIO`가 정상 처리하고 있다. `S3URI`가 스킴을 검증조차 하지 않기 때문이다 (섹션 4.1). `S3FileIO`에게 스킴은 **bucket과 key를 뽑아내기 위한 문자열 앞부분**일 뿐이다.

스킴을 누가 해석하는지가 핵심이다.

| 계층 | 스킴의 역할 |
|------|-------------|
| **Hadoop `FileSystem`** | **구현체를 고르는 키다.** `fs.<scheme>.impl`을 찾아 클래스를 로드한다 |
| **Iceberg `FileIO`** | **거의 의미 없다.** 구현체는 이미 `io-impl`로 정해져 있고, 스킴은 URI 파싱용이다 |

##### 스킴별 성능 차이는 Hadoop 2.x 시절 이야기다

세 스킴은 **서로 다른 구현체**였고, 그래서 실제로 성능이 달랐다.

| 스킴 | 구현체 | 특징 | Hadoop 3.x 현재 |
|------|--------|------|-----------------|
| `s3://` | `S3FileSystem` | S3를 **블록 스토리지처럼** 사용. 파일을 조각내 저장해 S3에서 직접 열 수 없었다 | **제거됨** |
| `s3n://` | `NativeS3FileSystem` | 파일을 원본 그대로 저장. 대신 파일 크기 제한, 멀티파트 업로드 미지원 | **제거됨** |
| `s3a://` | `S3AFileSystem` | `s3n`의 후속. 멀티파트 업로드, 병렬 IO, 컬럼 포맷용 random IO 최적화 | **유일하게 남음** |

공식 문서가 명확하다. ✅ (`hadoop-aws` 3.3.4 `index.md`)

> ### Other S3 Connectors
> There other Hadoop connectors to S3. **Only S3A is actively maintained by the Hadoop project itself.**
> 1. Apache's Hadoop's original `s3://` client. **This is no longer included in Hadoop.**
> 2. Amazon EMR's `s3://` client. This is from the Amazon EMR team, who actively maintain it.
> 3. Apache's Hadoop's `s3n:` filesystem client. **This connector is no longer available: users must migrate to the newer `s3a:` client.**

**즉 "스킴에 따라 성능이 다르다"는 셋이 공존하던 시절의 사실이고, Hadoop 3.x에는 `s3a`밖에 없어 비교 자체가 성립하지 않는다.** 블로그 글들이 그 시절 내용을 담고 있는 것이다.

##### ⚠️ AWS EMR의 `s3://`는 완전히 다른 것이다

위 문서 2번 항목이다. EMR의 `s3://`는 **EMRFS**라는 아마존 자체 구현이고 지금도 활발히 관리된다. AWS 문서가 "EMR에서는 `s3://`를 쓰라"고 하는 이유다. **vanilla Spark + MinIO인 우리와는 무관하다.**

##### "`s3`가 레거시"인가 — 맥락에 따라 정반대다

| 맥락 | `s3://`의 의미 |
|------|---------------|
| **Hadoop** | 제거된 레거시. `s3a://`가 표준 |
| **AWS EMR** | EMRFS. **권장 스킴** |
| **Iceberg / Trino 등** | 자체 S3 클라이언트를 쓰므로 스킴이 큰 의미 없음. 관례적으로 `s3://` 표기를 많이 쓴다 |

같은 `s3://`라는 글자가 세 맥락에서 다른 것을 가리키기 때문에, 블로그 글들이 서로 모순돼 보인다.

##### 우리는 `s3a://`를 유지한다

| 이유 | 설명 |
|------|------|
| **바꿀 실익이 0이다** | `S3FileIO`는 스킴을 무시하고 bucket/key만 쓴다. 성능이 동일하다 |
| **바꾸는 비용이 크다** | 기존 테이블 메타데이터에 절대 경로가 `s3a://`로 박혀 있다. 스킴을 바꾸려면 metadata rewrite가 필요한데, **이번 전환이 "마이그레이션 불필요"였던 이유가 정확히 그것을 안 했기 때문이다** (섹션 4.1) |
| **어차피 S3A가 필요하다** | 원천 avro와 eventLog는 Hadoop `FileSystem`이 처리하고, 거기서 `s3a://`는 **유일한 선택지**다 (Q6) |

> 신규 시스템을 처음부터 설계한다면 Hadoop `FileSystem`을 쓰지 않는 순수 Iceberg 스택에서는 `s3://`가 관례다. 다만 Spark를 쓰는 한 Hadoop `FileSystem`이 따라오므로 `s3a://`가 실용적이다.

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

> ⚠️ **개별 SDK jar로 대체하지 말 것 — S3만 넣으면 실패한다.** 상세는 아래 5.0.1.

**③ MinIO checksum 호환성 확인** (섹션 4.5). 이게 Phase 0의 실질적 게이트다.

### 5.0.1 `NoClassDefFoundError: .../services/kms/...` — S3만 쓰는데 KMS가 왜 필요한가

**증상**

```
NoClassDefFoundError: software/amazon/awssdk/services/kms/model/EncryptionAlgorithmSpec
```

(`glue`, `dynamodb` 쪽 클래스로 나타날 수도 있다.)

**원인** ✅ 소스 검증

`S3FileIO.initialize()`가 클라이언트 팩토리를 만드는 경로가 S3 전용이 아니다.

```java
// S3FileIO.java:495
Object clientFactory = S3FileIOAwsClientFactories.initialize(properties);

// S3FileIOAwsClientFactories.java:41-47
public static <T> T initialize(Map<String, String> properties) {
  String factoryImpl = PropertyUtil.propertyAsString(properties, S3FileIOProperties.CLIENT_FACTORY, null);
  if (Strings.isNullOrEmpty(factoryImpl)) {
    return (T) AwsClientFactories.from(properties);   // ← s3.client-factory 미설정 시 여기로
  }
  ...
}
```

그리고 `AwsClientFactories.from()`이 돌려주는 `DefaultAwsClientFactory`는 **`AwsClientFactory` 인터페이스**를 구현하는데, 그 인터페이스의 메서드 시그니처가 이렇다.

```java
// AwsClientFactory.java:23-25, 61
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
...
KmsClient kms();
GlueClient glue();
DynamoDbClient dynamo();
```

JVM이 이 클래스를 링크하려면 **메서드 시그니처에 등장하는 타입을 전부 해결해야 한다.** 즉 KMS·Glue·DynamoDB를 한 줄도 쓰지 않아도 **해당 클래스가 classpath에 존재해야 한다.** MinIO에 S3만 쓰는 우리 상황도 예외가 아니다.

**`iceberg-aws-bundle`은 정확히 이 이유로 서비스 모듈을 전부 담고 있다.** ✅ 소스 검증 (`aws-bundle/build.gradle:27-42`)

```gradle
implementation platform(libs.awssdk.bom)
implementation "software.amazon.awssdk:apache-client"
implementation "software.amazon.awssdk:auth"
implementation "software.amazon.awssdk:s3"
implementation "software.amazon.awssdk:kms"          // ← 이것
implementation "software.amazon.awssdk:glue"         // ← 이것
implementation "software.amazon.awssdk:sts"
implementation "software.amazon.awssdk:dynamodb"     // ← 이것
implementation "software.amazon.awssdk:iam"
implementation "software.amazon.awssdk:sso"
implementation "software.amazon.awssdk:lakeformation"
```

**해결**

`iceberg-aws` + 개별 SDK jar 조합을 버리고 **`iceberg-aws-bundle:1.10.1` 단일 jar**를 쓴다. 버전은 Iceberg와 정확히 일치시킨다.

**확인 명령**

```bash
# 1) 현재 classpath에 어떤 aws 관련 jar가 있는지
kubectl exec <driver-pod> -- ls $SPARK_HOME/jars | grep -iE "iceberg|awssdk|aws-java|bundle"

# 2) kms 클래스가 실제로 들어 있는지 (0이면 이 문제가 맞다)
kubectl exec <driver-pod> -- sh -c \
  'for j in $SPARK_HOME/jars/*.jar; do \
     n=$(unzip -l "$j" 2>/dev/null | grep -c "software/amazon/awssdk/services/kms/"); \
     [ "$n" -gt 0 ] && echo "$n  $j"; \
   done'
```

**주의 — 섞어 쓰지 말 것**

`iceberg-aws-bundle`은 `org.apache.http`와 `io.netty`를 relocate(shade)한다 ✅ (`aws-bundle/build.gradle:62-63`). 여기에 별도의 `software.amazon.awssdk:*` jar나 구버전 `iceberg-aws`를 함께 넣으면 클래스가 이중으로 존재해 더 찾기 어려운 충돌이 난다. **bundle 하나로 통일한다.**

| 구성 | 판정 |
|------|------|
| `iceberg-aws-bundle:1.10.1` 단독 | ✅ 권장 |
| `iceberg-aws` + `awssdk:s3` + `awssdk:auth` 등 개별 | ❌ 이 에러의 원인 |
| `iceberg-aws-bundle` + 개별 `awssdk:*` 혼재 | ❌ 중복 클래스 충돌 위험 |

> 참고: `s3.client-factory`로 커스텀 팩토리를 지정해도 해결되지 않는다. 그 인터페이스(`S3FileIOAwsClientFactory`)를 구현하더라도, 기본 경로를 벗어나기 전에 이미 같은 링크 문제가 발생할 수 있고 무엇보다 우회할 이유가 없다.

### 5.0.2 실제 진단 결과 — driver Pod의 jar 구성 ✅ 확인 완료

`$SPARK_HOME/jars`에서 확인된 관련 jar는 두 개뿐이었다.

```
aws-java-sdk-bundle-1.12.262.jar
iceberg-spark-runtime-3.5_2.12-1.10.1.jar
```

여기서 두 가지가 드러난다.

#### ① AWS SDK **v2가 하나도 없다** — 이번 에러의 직접 원인

`aws-java-sdk-bundle`은 **AWS SDK v1**이다. 패키지가 `com.amazonaws.*`이며, `software.amazon.awssdk.*`는 단 하나도 들어 있지 않다. `S3FileIO`가 요구하는 것은 v2이므로 KMS뿐 아니라 **S3 클라이언트 클래스조차 없는 상태**다.

버전도 정확히 들어맞는다. `aws-java-sdk-bundle 1.12.262`는 **Hadoop 3.3.4가 고정한 버전**이다. ✅ 소스 검증

```xml
<!-- hadoop-project-3.3.4.pom -->
<aws-java-sdk.version>1.12.262</aws-java-sdk.version>
```

| hadoop-aws 버전 | 의존하는 AWS SDK |
|-----------------|------------------|
| **3.3.4** | `com.amazonaws:aws-java-sdk-bundle` (**v1**) ← 우리 환경 |
| 3.4.1 | `software.amazon.awssdk:bundle` (v2) |

`iceberg-spark-runtime`에는 **`iceberg-aws`가 포함되어 있지만 AWS SDK는 포함되어 있지 않다** ✅ (`spark/v3.5/build.gradle:241` — `implementation project(':iceberg-aws')`). 그래서 `S3FileIO` 클래스 자체는 로드되지만 SDK 클래스 링크에서 실패한다. **`iceberg-aws-bundle`이 별도 artifact로 존재하는 이유가 정확히 이것이다.**

#### ✅ SDK v1과 v2는 공존해도 된다

기존 `aws-java-sdk-bundle-1.12.262.jar`를 **제거할 필요가 없다.** 오히려 제거하면 안 된다 — Hadoop 3.3.4의 S3A(원천 avro 읽기)가 그것을 쓴다.

| | 패키지 | 사용처 |
|---|---|---|
| SDK v1 (`aws-java-sdk-bundle`) | `com.amazonaws.*` | Hadoop S3A — 원천 avro 읽기, 경로 목록 파일 |
| SDK v2 (`iceberg-aws-bundle`) | `software.amazon.awssdk.*` | Iceberg S3FileIO — 테이블 데이터/메타데이터 |

**패키지 네임스페이스가 완전히 달라 충돌하지 않는다.** Hadoop 3.3.x + Iceberg S3FileIO 조합의 표준 구성이다. (shading도 겹치지 않는다 — v1 bundle은 httpclient를 `com.amazonaws.thirdparty.*`로, `iceberg-aws-bundle`은 `org.apache.iceberg.aws.shaded.*`로 relocate한다.)

**조치**: `iceberg-aws-bundle-1.10.1.jar`를 `$SPARK_HOME/jars/`에 **추가**한다. 기존 jar는 그대로 둔다.

```
$SPARK_HOME/jars/
├── aws-java-sdk-bundle-1.12.262.jar          # 유지 (S3A용)
├── iceberg-spark-runtime-3.5_2.12-1.10.1.jar # 유지
└── iceberg-aws-bundle-1.10.1.jar             # ★ 추가
```

> `iceberg-aws-bundle`은 Spark 버전과 무관한 artifact다. Scala 버전 접미사도 없다. Iceberg 버전(1.10.1)만 맞추면 된다.

#### ② ⚠️ Spark 런타임이 문서와 다르다 — 별건으로 확인 필요

`iceberg-spark-runtime-**3.5_2.12**`는 **Spark 3.5 / Scala 2.12**용이다. 기존 문서들이 기재한 **Spark 4.1.1**(Scala 2.13)과 맞지 않는다.

Scala 2.12와 2.13은 바이너리 호환되지 않으므로 **Spark 4.1.1에서 이 jar는 동작할 수 없다.** 그런데 expire snapshots가 실제로 돌고 있었으므로(6~12분 실측), **이 Pod의 실제 런타임은 Spark 3.5.x로 보는 것이 타당하다.** `aws-java-sdk-bundle 1.12.262`(= Hadoop 3.3.4 = Spark 3.5.x 번들 버전)도 같은 결론을 가리킨다.

**이번 전환에는 영향이 없다** — 확인한 Iceberg 로직이 두 모듈에서 동일하기 때문이다. ✅ 소스 대조

| 확인 항목 | `spark/v4.0` | `spark/v3.5` | 동일 여부 |
|-----------|--------------|--------------|-----------|
| bulk delete 분기 | `ExpireSnapshotsSparkAction.java:257-272` | **:257-271** | ✅ 동일 |
| driver 삭제 (`collectAsList`) | `:222-228` | **:223-229** | ✅ 동일 |
| `max_concurrent_deletes` 무시 | `ExpireSnapshotsProcedure.java:133-143` | **:128-138** | ✅ 동일 |
| `prefix_listing` 파라미터 | `RemoveOrphanFilesProcedure.java:71-73, 195` | **:67, 141, 189** | ✅ 동일 |

Hadoop 쪽도 삭제 경로가 같다. ✅ (`S3AFileSystem.java` 3.3.4 기준 :3162 `delete` → :3172 `innerGetFileStatus(ALL)` → `DeleteOperation` → :3178 `maybeCreateFakeParentDirectory` → :3209 `s3Exists(DIRECTORIES)` LIST + PUT). **섹션 1.3의 "파일 1개 = 요청 3개" 분석은 그대로 유효하다.**

`fs.s3a.*` 기본값도 실질적으로 동일하다:

| 설정 | 3.3.4 | 3.4.1 | 우리 환경에서의 결과 |
|------|-------|-------|---------------------|
| `multipart.size` | 64M | 64M | 동일 |
| `multipart.threshold` | 128M | 128M | 동일 |
| `buffer.dir` | `${hadoop.tmp.dir}/s3a` | `${env.LOCAL_DIRS:-${hadoop.tmp.dir}}/s3a` | K8s엔 `LOCAL_DIRS`가 없으므로 **결과 경로 동일** (`/tmp/hadoop-<user>/s3a`) |
| `fast.upload.buffer` | disk | disk | 동일 |

> **배경 (확인됨)**: Spark 4에서 Scala 코드로 maintenance 함수를 실행할 때 오류가 발생해 **Spark 3.5.8로 임시 다운그레이드**한 상태다. 추후 해결되면 Spark 4로 복귀할 예정이다. 따라서 다른 문서들의 "Spark 4.1.1" 기재는 **목표 버전이지 현재 운영 버전이 아니다** — 구분해서 표기할 필요가 있다. Spark 4 복귀 시 jar 변경 사항은 섹션 5.0.3 말미를 참조한다.

### 5.0.3 jar 배치 — 이미지의 `$SPARK_HOME/jars`에 넣는다

#### 필요한 라이브러리는 하나뿐이다

| 항목 | 값 |
|------|-----|
| artifact | `org.apache.iceberg:iceberg-aws-bundle:1.10.1` |
| 파일명 | `iceberg-aws-bundle-1.10.1.jar` |
| 크기 | **약 60MB** (62,673,230 bytes) ✅ Maven Central 확인 |
| Scala 접미사 | **없음** — `_2.12` / `_2.13` 구분이 존재하지 않는다 |
| Spark 버전 의존성 | **없음** — Spark 3.5든 4.0이든 동일한 jar |
| 맞춰야 할 것 | **Iceberg 버전만** (`iceberg-spark-runtime`과 동일하게 1.10.1) |

#### 결론: 이미지 빌드 시 `$SPARK_HOME/jars/`에 넣는다

```dockerfile
# 예시
ARG ICEBERG_VERSION=1.10.1
RUN curl -fsSL -o $SPARK_HOME/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar
```

기존 jar는 **그대로 둔다.**

```
$SPARK_HOME/jars/
├── aws-java-sdk-bundle-1.12.262.jar          # 유지 — S3A(원천 avro 읽기)가 쓴다
├── iceberg-spark-runtime-3.5_2.12-1.10.1.jar # 유지
└── iceberg-aws-bundle-1.10.1.jar             # ★ 추가
```

#### 왜 `pom.xml`이 아닌가

| 방식 | 판정 | 이유 |
|------|------|------|
| **이미지 `$SPARK_HOME/jars/`** | ✅ **권장** | system classpath라 **driver·executor 양쪽에 자동으로** 올라간다. `iceberg-spark-runtime`이 이미 이 방식이므로 구성이 일관된다 |
| `pom.xml` + fat jar (shade) | ❌ | ① 60MB가 **매 submit마다 전송**된다 ② `iceberg-aws-bundle`은 이미 `org.apache.http`/`io.netty`를 relocate한 jar인데, maven-shade가 이를 **다시 재배치하면 relocation이 깨질 수 있다** ③ `iceberg-spark-runtime`에 이미 들어 있는 `iceberg-aws` 클래스와 **중복**된다 |
| `--packages` / `spark.jars.packages` | ❌ | driver·executor가 **매 실행마다 Maven 저장소에 접근**해야 한다. K8s 폐쇄망·재현성·기동 시간 모두 불리하다 |
| `--jars` / `spark.jars` | △ | 동작은 하지만 user classloader로 올라가 `userClassPathFirst` 등과 얽힌다. 상시 필요한 라이브러리를 매번 붙일 이유가 없다 |

#### `pom.xml`에는 무엇을 넣나 — 아마 아무것도 필요 없다

maintenance Job이 SQL 프로시저만 호출한다면(`CALL <카탈로그>.system.expire_snapshots(...)`), **애플리케이션 코드는 `org.apache.iceberg.aws.*`를 전혀 참조하지 않는다.** 컴파일 의존성이 없으므로 pom에 추가할 것이 없다. 순수하게 **런타임 classpath** 문제다.

코드에서 `S3FileIO`나 `AwsProperties`를 직접 import하는 경우에만 pom에 넣되, **반드시 `provided`로 한다** (런타임 jar는 이미지가 제공하므로 패키징에 포함시키지 않는다).

```xml
<!-- 코드에서 직접 참조할 때만. 대부분은 불필요하다 -->
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-aws-bundle</artifactId>
  <version>1.10.1</version>
  <scope>provided</scope>
</dependency>
```

#### 배치 후 검증

```bash
# 1) jar가 올라갔는지
kubectl exec <driver-pod> -- ls -la $SPARK_HOME/jars/ | grep iceberg

# 2) SDK v2 클래스가 실제로 로드 가능한지 (숫자가 나오면 정상)
kubectl exec <driver-pod> -- sh -c \
  'unzip -l $SPARK_HOME/jars/iceberg-aws-bundle-1.10.1.jar | grep -c "software/amazon/awssdk/services/kms/"'

# 3) v1도 그대로 있는지 (S3A용)
kubectl exec <driver-pod> -- sh -c \
  'unzip -l $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar | grep -c "com/amazonaws/services/s3/"'
```

#### Spark 4 업그레이드 시 무엇이 바뀌나

**`iceberg-aws-bundle`은 바꿀 필요가 없다.** Spark/Scala와 무관한 artifact이기 때문이다.

| jar | Spark 3.5 (현재) | Spark 4.0 |
|-----|------------------|-----------|
| `iceberg-spark-runtime` | `-3.5_2.12-1.10.1` | **`-4.0_2.13-1.10.1`로 교체** |
| `iceberg-aws-bundle` | `-1.10.1` | **동일 (그대로)** |
| `aws-java-sdk-bundle` (S3A) | v1 `1.12.262` (Hadoop 3.3.4) | Spark 4는 Hadoop 3.4.x → **`software.amazon.awssdk:bundle`(v2)로 바뀐다**. 이때는 S3A와 S3FileIO가 같은 SDK v2를 쓰게 되므로 **버전 정합성 확인 필요** ⚠️ |

> ⚠️ **Spark 4 전환 시 참고 — Iceberg 1.10.1은 Spark 4.1을 지원하지 않는다.** ✅ 확인
>
> Iceberg 1.10.1의 Spark 모듈은 `spark/v3.4`, `spark/v3.5`, `spark/v4.0` 세 개뿐이며 `spark/v4.1`은 존재하지 않는다(빌드 대상 버전: `spark34=3.4.4`, `spark35=3.5.6`, `spark40=4.0.0`).
>
> 즉 **Spark 4.1.1 + `iceberg-spark-runtime-4.0_2.13`은 지원 조합이 아니다.** maintenance 함수 실행 시 발생했던 에러의 원인일 가능성이 있다. Iceberg 버전업을 기다리기보다 **Spark 4.0.x로 맞춰 재시도**해보는 것이 더 빠른 경로일 수 있다. (⚠️ 가설 — 당시 에러 메시지 확인 필요)

### 5.0.4 이미지 구성 — 추가하는 것은 jar 하나뿐이다

운영 이미지가 타 팀 소유라 자체 이미지를 파생 빌드해야 하는 상황이다. **이때 원칙은 하나다.**

> **이번 이미지에서 바꾸는 것은 `iceberg-aws-bundle-1.10.1.jar` 추가, 그것 하나뿐이어야 한다.**

FileIO 전환 효과를 측정하는 것이 목적이므로, 이미지에 다른 변경이 섞이면 그 측정이 무의미해진다.

#### Dockerfile

```dockerfile
# ① 베이스는 "현재 운영 중인 그 이미지" — 새로 apache/spark를 받지 않는다
FROM <타팀-레지스트리>/<이미지>:<현재-운영-태그>

# ② 공식 Spark 이미지는 USER spark(비root)로 끝나므로 root로 전환
#    (apache/spark-docker의 Dockerfile 마지막이 `USER spark`)
USER root

ARG ICEBERG_VERSION=1.10.1
ARG BUNDLE_SHA512=a649f50fd8508b3e179002ecbc28b3ae3de374c6851ce2ed203fe29d3bbf7794780075bd8ad8f41655d4f8684ff064f58d32218d329b0937bce034199afb900a

# ③ jar 추가 (사내 저장소가 있으면 COPY, 없으면 curl)
RUN set -eux; \
    curl -fsSL -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    echo "${BUNDLE_SHA512}  /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" | sha512sum -c -; \
    chmod 644 /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# ④ 원래 USER로 복귀 (공식 이미지는 spark, UID 185)
USER spark
```

`COPY` 방식을 쓴다면 ③만 바꾼다.

```dockerfile
COPY --chown=root:root jars/iceberg-aws-bundle-1.10.1.jar /opt/spark/jars/
```

#### ⚠️ 예전 Dockerfile에서 가져오면 안 되는 것

과거 테스트용 Dockerfile에는 다음이 있었다.

```dockerfile
FROM apache/spark:3.5.6-java
ENV TZ=Asia/Seoul
# jars: hadoop-common-3.4.1, hadoop-aws-3.4.1, bundle-2.32.29,
#       hadoop-client-api-3.4.1, hadoop-client-runtime-3.4.1, ...
RUN rm -f /opt/spark/jars/hadoop-client-api-3.3.4.jar
RUN rm -f /opt/spark/jars/hadoop-client-runtime-3.3.4.jar
```

| 항목 | 판정 | 이유 |
|------|------|------|
| `FROM apache/spark:3.5.6-java` | ❌ | 운영은 **3.5.8**이다. 3.5.6으로 내리면 Spark 버전이 바뀌고, 타 팀 이미지의 커스터마이징(설정·패치·유저)도 전부 잃는다 |
| **Hadoop 3.3.4 → 3.4.1 교체** (`hadoop-*` jar 5종 + `rm`) | ❌ **절대 금지** | 아래 상세 |
| `bundle-2.32.29.jar` (S3A용 SDK v2) | ❌ | 위 Hadoop 교체와 한 세트다. Hadoop 3.3.4를 유지하면 필요 없다 |
| `iceberg-spark-runtime-3.5_2.12-**1.9.2**` | ❌ | 베이스 이미지에 이미 **1.10.1**이 있다. 버전을 내리면 안 된다 |
| `iceberg-aws-bundle-**1.9.2**` | ❌ → **1.10.1** | `iceberg-spark-runtime`과 **정확히 일치**시켜야 한다 |
| `spark-avro_2.12-3.5.6.jar` | ⚠️ **추가하지 말고 확인만** | 원천 avro를 지금도 읽고 있으므로 베이스 이미지에 이미 있거나 앱 jar가 제공하는 중이다. 버전이 3.5.6이라 3.5.8과도 안 맞는다 |
| `postgresql-42.7.7.jar` | ⚠️ 추가하지 않음 | 용도 불명. 베이스 이미지에 필요한 것이 있다면 이미 들어 있다 |
| `ENV TZ=Asia/Seoul` | ❌ **제거 확정** | 운영 Pod의 TZ가 **UTC**로 확인됐다(2026-08-26). 이를 Asia/Seoul로 바꾸면 FileIO 전환과 무관한 시각 관련 동작 변경이 섞인다. 아래 상세 |

#### Hadoop 교체를 금지하는 이유

1. **S3A 구현이 통째로 바뀐다.** Hadoop 3.3.4는 AWS SDK **v1**, 3.4.1은 **v2**를 쓴다(섹션 5.0.2). 즉 **원천 avro 읽기 경로가 바뀐다.** FileIO 전환 효과와 섞여 A/B 판정이 불가능해진다
2. **MinIO checksum 리스크를 미리 당겨온다.** S3A까지 SDK v2가 되면 checksum 문제가 avro 읽기에도 번진다(섹션 9.4-①). 이건 Spark 4 업그레이드 때 감당할 항목이지 지금 감당할 항목이 아니다
3. **클래스 중복 위험.** `hadoop-client-api`/`hadoop-client-runtime`은 shaded fat jar인데, 여기에 unshaded `hadoop-common`을 얹으면 같은 클래스가 두 벌 존재한다. Spark 3.5.x는 Hadoop 3.3.4로 빌드·테스트된 배포판이다

> **가장 중요한 오해 방지: `S3FileIO`는 Hadoop 버전과 무관하다.** 자체적으로 AWS SDK v2(`iceberg-aws-bundle`)를 들고 다니며 `fs.s3a.*`도 Hadoop `FileSystem`도 쓰지 않는다. **Hadoop 3.3.4 위에서 그대로 동작한다.** S3FileIO를 쓰려고 Hadoop을 올릴 필요가 전혀 없다.

#### `ENV TZ`를 조심해야 하는 이유

대상 테이블의 `ts`는 **`timestamp_ntz`**이고 파티션이 `hour(ts)`다. 컨테이너 TZ 변경은 Spark session timezone·avro 파싱·로그 시각 등에 영향을 줄 수 있다.

**확인 결과 운영 Pod의 TZ는 `UTC`다.** 현재 파이프라인이 이 상태로 정상 동작 중이므로 **UTC가 곧 검증된 설정**이다. `ENV TZ=Asia/Seoul`은 파생 이미지에서 제외한다.

#### 이미지 배포와 설정 전환은 분리한다

**jar를 추가하는 것만으로는 아무 일도 일어나지 않는다.** `io-impl`을 설정하지 않으면 Iceberg는 여전히 `HadoopFileIO`를 쓴다(섹션 1.1). 따라서 두 단계로 나눌 수 있고, 각각 독립적으로 롤백된다.

| 단계 | 변경 | 성격 | 롤백 |
|------|------|------|------|
| **1. 이미지 교체** | jar 1개 추가 | **무해(inert)** — 동작 변화 없음 | 이전 이미지 태그로 되돌림 |
| **2. `io-impl` 설정** | maintenance Job의 conf | 실제 전환 | 설정 제거 |

이미지를 먼저 배포해 기존 Job들이 정상 동작하는지 확인한 뒤, maintenance Job에만 `io-impl`을 켜는 순서가 가장 안전하다.

#### 빌드 후 검증

```bash
# ① iceberg 관련 jar 구성 — runtime과 bundle의 버전이 같아야 한다
kubectl exec <driver-pod> -- ls $SPARK_HOME/jars | grep -iE "iceberg|aws"
# 기대:
#   aws-java-sdk-bundle-1.12.262.jar            (기존, S3A용 SDK v1)
#   iceberg-spark-runtime-3.5_2.12-1.10.1.jar   (기존)
#   iceberg-aws-bundle-1.10.1.jar               (추가됨)

# ② Hadoop 버전이 그대로인지 (3.3.4여야 한다)
kubectl exec <driver-pod> -- ls $SPARK_HOME/jars | grep hadoop-client

# ③ SDK v2 클래스 적재 가능 확인
kubectl exec <driver-pod> -- sh -c \
  'unzip -l $SPARK_HOME/jars/iceberg-aws-bundle-1.10.1.jar | grep -c "software/amazon/awssdk/services/kms/"'
```

### 5.0.5 라이브러리를 어디에 둘 것인가 — 배치 원칙

#### 세 개의 축을 구분한다

혼동하기 쉬운데, **Maven scope / fat jar / Spark classpath는 서로 다른 축**이다.

**축 ①: Maven scope — 빌드 시점 이야기**

| scope | 컴파일에 사용 | **fat jar에 포함** |
|-------|--------------|-------------------|
| `compile` (기본) | O | **O** |
| `provided` | O | **X** — "런타임 환경이 제공한다"는 선언 |
| `runtime` | X | O |
| `test` | 테스트만 | X |

**축 ②: fat jar란**

- **thin jar**: 내가 쓴 코드만 (수십 KB ~ 수 MB)
- **fat jar (uber jar)**: 내 코드 + 모든 의존성을 하나로 합친 jar. `maven-shade-plugin` 또는 `maven-assembly-plugin`이 만든다
- `shade`는 추가로 **패키지 relocation**(`com.google.guava` → `myapp.shaded.guava`)을 해서 버전 충돌을 피할 수 있다. `iceberg-aws-bundle`이 `org.apache.http`를 relocate한 것이 정확히 이 기법이다 (섹션 5.0.1)

**축 ③: Spark 런타임 classpath에 올라가는 경로**

| 경로 | 위치 | driver/executor | 전송 비용 |
|------|------|-----------------|-----------|
| `$SPARK_HOME/jars/` | 이미지 | 둘 다 | **없음** (노드 이미지 캐시) |
| 애플리케이션 jar | spark-submit 인자 | 둘 다 | 매 실행 전송 |
| `--jars` / `--packages` | submit 옵션 | 둘 다 | 매 실행 전송(+의존성 해석) |

#### 배치 판단 기준

| 라이브러리 성격 | 위치 | pom scope | 예시 |
|-----------------|------|-----------|------|
| Spark 자체 | 이미 이미지에 있음 | `provided` | `spark-core`, `spark-sql` |
| **Spark 버전에 묶인 것** | **이미지** | `provided` | **`spark-avro`**, `spark-hadoop-cloud` |
| 인프라 공통 (모든 Job이 씀) | **이미지** | `provided` | `iceberg-spark-runtime`, `iceberg-aws-bundle`, `hadoop-aws` |
| 이 앱만 쓰는 비즈니스 라이브러리 | **fat jar** | `compile` | 사내 공통 모듈, JSON/유틸 |

**성능 관점의 결론**: 실행 성능(쿼리 속도)에는 **차이가 없다.** classpath 위치는 클래스 로딩 시점에만 영향을 준다. 차이가 나는 것은 **Job 기동 시간**뿐이고, 수 MB 수준이면 무시할 만하다. `--packages`만은 예외로 매번 의존성 해석과 다운로드가 필요해 느리고 폐쇄망에 취약하다.

#### ⚠️ 절대 피할 것 — 같은 라이브러리를 양쪽에 두기

이미지와 fat jar 양쪽에 같은 라이브러리가 있으면 클래스가 두 벌 존재한다. 기본적으로 system classpath(이미지)가 이기지만 `spark.driver.userClassPathFirst=true` 같은 설정으로 뒤집힐 수 있고, **버전이 다르면 `NoSuchMethodError` 같은 재현하기 어려운 오류**가 난다.

> **원칙: 한 라이브러리는 한 곳에만 둔다.**

#### `spark-avro`의 경우

**`spark-avro`는 Spark 배포판에 포함되어 있지 않다.** ✅ 공식 문서 확인

> The `spark-avro` module is external and not included in `spark-submit` or `spark-shell` by default.
> — Spark 3.5.8 `docs/sql-data-sources-avro.md`

(`assembly/pom.xml`에서도 `connect` 프로파일 아래에만 등장한다.)

따라서 현재 어딘가에서 공급되고 있다. 어디인지 먼저 확인한다.

```bash
# ① 앱 jar에 포함되어 있나 (= fat jar 방식)
unzip -l target/<app>.jar | grep -i "org/apache/spark/sql/avro"

# ② pom에 fat jar 플러그인이 있나
grep -E "maven-shade-plugin|maven-assembly-plugin" pom.xml

# ③ 이미지에 있나
kubectl exec <pod> -- ls $SPARK_HOME/jars | grep -i avro

# ④ CRD에서 주고 있나
kubectl get sparkapplication <name> -o yaml | grep -A5 "deps:"
```

| 결과 | 현재 방식 | 올바른 pom scope |
|------|-----------|------------------|
| ①에만 있음 | fat jar | `compile` (현재 그대로 맞다) |
| ③에만 있음 | 이미지 | **`provided`** — `compile`이면 fat jar에도 들어가 중복된다 |
| ①③ 둘 다 | **중복** | 정리 필요 |

**권장 방향은 이미지**다. `spark-avro_2.12-3.5.8`은 Spark 3.5.8과 짝이어야 하는데, fat jar에 넣어두면 이미지의 Spark를 올릴 때 앱 jar가 뒤처져 **버전 드리프트**가 생긴다. 실제로 과거 테스트 Dockerfile에는 `spark-avro_2.12-**3.5.6**`이 들어 있었고 현재 런타임은 **3.5.8**이다 — 정확히 그 드리프트다.

> **다만 지금 옮기지 않는다.** 현재 정상 동작 중이고 FileIO 전환 A/B가 진행 중이다. Spark 4 업그레이드 때 어차피 `spark-avro_2.13-4.1.1`로 교체해야 하므로, **그 시점에 이미지로 옮기는 것이 자연스럽다.**

#### 체크섬(`sha512`)은 무엇이고 필요한가

Maven Central은 모든 artifact 옆에 `.jar.sha512` / `.jar.sha1`을 함께 배포한다. 받은 파일이 원본과 **바이트 단위로 같은지** 확인하는 값이다.

| 목적 | 설명 |
|------|------|
| **손상된 다운로드 탐지** | 60MB 파일이 사내 프록시에서 잘려 받아지면 `NoClassDefFoundError` 같은 **엉뚱한 증상**으로 나타나 원인 추적이 어렵다. 실제로 이번에 겪은 에러와 구분이 안 된다 |
| 공급망 무결성 | 미러/중간 경로에서의 변조 탐지 |

**테스트 단계에서 `wget`으로 받아 넣는 것은 문제없다.** 정상 동작을 확인했다면 파일은 온전하다. 다만 **운영 이미지 빌드에는 넣는 것을 권한다** — Dockerfile이 매번 같은 파일을 받는다는 보장(재현성)이 생긴다.

```bash
# 받은 파일 검증
sha512sum iceberg-aws-bundle-1.10.1.jar
curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar.sha512
# 두 값이 같으면 정상
```

1.10.1의 값은 다음과 같다.

```
sha512: a649f50fd8508b3e179002ecbc28b3ae3de374c6851ce2ed203fe29d3bbf7794780075bd8ad8f41655d4f8684ff064f58d32218d329b0937bce034199afb900a
sha1:   9c02c851a2356f287f040ed784170c758369d134
```

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
> **현재 상태 (확인 완료)**: Compaction Job은 `coreLimit`을 `cores`와 동일하게 1로 지정해 뒀으나, **expire snapshots Job에는 지정되어 있지 않다.** 즉 expire의 driver JVM은 노드 전체 코어를 보고 있고, 삭제 스레드가 `노드 코어 × 4`로 생성된다. **두 Job의 MinIO 부하 성격이 다른 이유가 여기 있을 수 있다.**
>
> 어느 쪽이든 결론은 같다 — **`s3.delete.num-threads`는 기본값에 맡기지 말고 명시한다.**

#### ⚠️ `coreLimit` 적용 순서 — baseline 계측 전에 바꾸지 말 것

expire Job에 `coreLimit=1`을 넣는 것은 맞는 방향이지만, **그 자체가 삭제 동작을 크게 바꾸는 변경**이다.

| 상태 | `availableProcessors()` | `iceberg.hadoop.delete-file-parallelism` (기본 `×4`) |
|------|------------------------|----------------------------------------------------|
| 현재 (`coreLimit` 미설정) | 노드 전체 코어 (예: 32) | **128 스레드** |
| `coreLimit=1` 적용 후 | 1 | **4 스레드** |

즉 `coreLimit=1`만 넣어도 MinIO의 **순간 요청률(RPS)이 크게 떨어진다.** 총 요청 수는 그대로이므로 expire duration은 오히려 늘어난다.

**따라서 계측 순서를 다음 중 하나로 고정한다:**

| 방식 | 순서 | 얻는 것 |
|------|------|---------|
| **A. 분리 측정 (권장)** | ① 현재 상태 baseline 계측 → ② `coreLimit=1` 적용 후 1회 계측 → ③ S3FileIO 전환 후 계측 | 데이터 포인트 3개. "부하의 몇 %가 스레드 폭주 탓이었나"를 분리해서 답할 수 있다 |
| **B. 일괄 적용** | ① 현재 상태 baseline 계측 → ② `coreLimit=1` + S3FileIO 동시 적용 후 계측 | 빠르지만 두 효과가 섞인다. 최종 결과만 필요하면 충분 |

**어느 쪽이든 baseline 계측은 `coreLimit` 변경 전에 해야 한다.** 바꾼 뒤에 재면 "전환 전" 숫자가 이미 개선된 값이라 전환 효과가 과소평가된다.

> **`coreLimit=1` 적용 후에는 `s3.delete.num-threads` 명시가 더 중요해진다.** `availableProcessors()`가 1이 되므로 기본값도 1이다 — `DeleteObjects` 40건을 순차로 보내게 된다. 8로 지정하면 5라운드로 끝난다.

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

### 6.5 실측 결과 (2026-08-27, 운영환경 expire snapshots) ✅

#### 기능 검증

`append`, `expire_snapshots`, `remove_orphan_files`, `rewrite_manifests`, `Compaction` **전부 정상 처리 확인.**

#### MinIO API 호출 (peak req/s)

| API | S3A만 | S3FileIO 추가 | 변화 |
|-----|-------|---------------|------|
| `deleteObject` | **481 req/s** | **17.4 req/s** | **−96.4%** |
| `listObjectV2` | **680 req/s** | **281 req/s** | **−58.7%** |

> **총 요청 수는 이보다 더 줄었다.** 위는 **peak rate**이고 Job duration이 동시에 3.5배 짧아졌으므로, 총량 기준으로는 `deleteObject`가 대략 **−99%** 수준이다 (섹션 1.4의 예측 −99.6%와 정합). 정확한 총량은 audit log 집계가 필요하다 ⚠️

#### DataFlint 지표

| 지표 | S3A만 | S3FileIO 추가 | 변화 |
|------|-------|---------------|------|
| **duration** | 13.5분 | **3.8분** | **−72%** |
| **dcu** | 0.2002 | **0.0550** | **−72.5%** |
| input | 20.74 MiB | 24.58 MiB | +18% |
| output | 0 B | 0 B | — |
| memory usage | 64.32% | 61.40% | −3%p |
| shuffle read | 334.65 MiB | 241.35 MiB | −28% |
| shuffle write | 241.83 MiB | 141.17 MiB | −41% |
| spill | 0 B | 0 B | — |
| **idle cores** | 89.59% | **90.25%** | +0.7%p |
| DataFlint alerts | 18개 | **6개** | −12 |

#### 해석

**① 개선은 Spark stage가 아니라 "보이지 않는 driver 삭제 구간"에서 났다.**

`input`이 오히려 **18% 늘었는데** duration은 **72% 줄었다.** 두 실행의 처리량이 애초에 같지 않다는 뜻이므로 엄밀한 통제 실험은 아니지만, **더 많은 데이터를 3.5배 빨리 처리했다**는 점에서 결론은 오히려 강화된다.

stage 지표(shuffle read/write)는 같은 자릿수에 머무는 반면 duration만 급감했다. 이는 **executor 구간이 아니라 driver 삭제 구간이 사라졌다**는 뜻이며, 섹션 5.1.2에서 예측한 `Job duration − stage 소요 합계 = 삭제 시간` 구조와 정확히 일치한다.

**② `dcu`와 `duration`이 같은 비율(−72%)로 움직였다.**

`dcu ∝ cores × duration`인데 cores를 바꾸지 않았으므로 두 값이 함께 움직이는 것이 정상이다. 노이즈 기준선 **±15%를 압도적으로 초과**하므로 실제 효과로 판정한다.

**③ `idle cores` 90%는 그대로다 — 이것이 다음 조치의 신호다.**

executor 4개 × 4코어 = 16코어인데 90%가 놀고 있다. 전환 전에는 "driver가 삭제하는 동안 executor가 논다"는 설명이 가능했지만, **삭제 구간이 사라진 지금도 90%라면 순수하게 과다 할당**이다. 섹션 5.1.2의 예고대로 **executor 축소가 다음 작업**이다.

**④ 남은 alert 4종 중 실제로 조치할 것은 하나다.**

| alert | 판정 |
|-------|------|
| **idle cores too high** | ✅ **조치 대상** — executor 축소 |
| executor memory over-provisioned | △ 보류 — 61.4% / spill 0B는 여유가 있다는 뜻이나, 3.8분짜리 Job에서 얻을 이득이 작다 |
| long filter condition | ✕ 무시 — `expiredFileDS = deleteCandidateFileDS.except(validFileDS)`의 구조적 특성 |
| broadcast small table in sort merge join | ✕ 무시 — 위 `except`가 sort-merge join으로 풀린 것. 3.8분 규모에서 실익 없음 |

#### ⚠️ 측정 해석 시 주의

| 항목 | 내용 |
|------|------|
| **MinIO 지표는 클러스터 전체일 가능성** | append(5분 주기)·Compaction이 동시에 돌고 있다면 그 트래픽이 섞인다. 잔존 `deleteObject` 17.4 req/s와 `listObjectV2` 281 req/s의 상당 부분이 **다른 Job의 것**일 수 있다 |
| **`DeleteObjects`(복수형) 지표 확인** | bulk delete가 실제로 쓰였다는 직접 증거다. MinIO가 이를 별도 지표로 노출하는지 확인할 것 (섹션 6.2) |
| 두 실행의 처리량 차이 | `input` +18% — 통제된 A/B가 아니다. 다만 효과 크기가 이를 압도한다 |

## 7. 미확인 사항 및 후속 과제

| 항목 | 내용 | 우선순위 |
|------|------|----------|
| **MinIO 버전과 SDK 2.33 checksum 호환성** | Phase 0의 게이트. 이게 막히면 MinIO 업그레이드가 선행되어야 한다 (섹션 4.5) | **최우선** |
| **실제 삭제 파일 수** | 섹션 1.4의 추정치를 실측으로 대체해야 보고가 된다. `mc admin trace` 또는 driver 로그의 `Deleted N total files` | 높음 |
| **읽기/쓰기 성능 영향** | Compaction `dcu/GB` A/B (섹션 5.3). 노이즈 기준선 ±15% | 높음 |
| **`s3.delete.num-threads` 적정값** | 기본값(driver 코어 수)은 너무 작다. 8로 시작하되 MinIO 순간 부하를 보며 조정 | 중간 |
| **`remove_orphan_files`의 `prefix_listing => true`** | LIST 요청을 추가로 크게 줄일 수 있으나, 기존 S3A 디렉터리 마커 오탐 위험 검증 필요 (섹션 2.2, 4.8) | 중간 |
| **`fs.s3a.acl.default=PublicReadWrite`의 실제 효력** | MinIO에서 ACL이 무시되는지 확인. 무시된다면 전환과 무관하게 제거 대상이고, 효력이 있다면 익명 읽기/쓰기가 열려 있다는 뜻이라 더 시급하다 (섹션 5.1.1) | **높음(보안)** |
| ~~driver Pod의 `limits.cpu` 설정 여부~~ | ✅ 확인 완료 — Compaction은 `coreLimit=1`, **expire snapshots는 미설정**(노드 코어 × 4 = 삭제 스레드). `coreLimit=1` 적용 예정이나 **baseline 계측 후에** 적용할 것 (섹션 5.1.2) | 완료 |
| **maintenance Job의 executor 수** | 삭제는 driver에서만 일어나므로 축소 여지가 있으나, **전환과 동시에 바꾸면 A/B 판정이 불가능하다.** 전환 후 Spark UI로 stage 구간을 보고 조정 (섹션 5.1.2) | 중간 (전환 후) |
| **`/tmp`의 성격과 ephemeral-storage limit** | Phase 1에는 무관(데이터 파일을 쓰지 않음). Phase 2/3 진입 전 확인 (섹션 4.6) | 중간 (Phase 2 전) |
| **다른 문서의 Spark 버전 기재** | 현재 운영은 **Spark 3.5.8**(임시), 목표가 4.x다. 다른 가이드들이 4.1.1을 현재 값처럼 적고 있어 구분 표기가 필요하다 (섹션 5.0.2) | 중간(문서 정합성) |
| **Spark 4 복귀 시 조합** | Iceberg 1.10.1은 **Spark 4.1 미지원**(`spark/v4.0`까지만 존재). 당시 maintenance 함수 오류가 이 때문인지 확인하고, Spark 4.0.x 재시도를 검토 (섹션 5.0.3) | 중간(후속) |
| **`s3.multipart.part-size-bytes` 조정 효과** | 64MB로 맞추면 S3A 현재 동작과 동일해진다. 그 이상은 A/B 필요 (섹션 5.1.3) | 낮음 (Phase 2) |
| **Spark 4.1.1 ↔ iceberg-spark-runtime 4.0 조합** | 현재 어떤 runtime jar를 쓰는지 확인하고 `iceberg-aws-bundle` 버전을 정확히 맞출 것 | 중간 |
| **maintenance 스케줄 재조정** | expire duration이 줄면 `reprocessing-dag-design.md` §6.2의 슬롯에 여유가 생긴다. 재배치 재검토 가능 | 낮음 (전환 후) |
| **daily maintenance DAG 통합** | 기존 후속 과제. FileIO 전환과 함께 하면 측정을 한 번에 끝낼 수 있다 | 낮음 |

---

## 8. 참고 자료

### 검증에 사용한 소스 (버전 고정)

아래 표의 `위치` 열은 다음 base URL에 이어 붙이면 원본으로 연결된다. 태그 고정이므로 줄 번호가 바뀌지 않는다.

| 프로젝트 | base URL |
|----------|----------|
| Iceberg 1.10.1 | `https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/` |
| Iceberg 1.11.0 | `https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/` |
| Hadoop 3.3.4 | `https://github.com/apache/hadoop/blob/rel/release-3.3.4/` |
| Hadoop 3.4.1 | `https://github.com/apache/hadoop/blob/rel/release-3.4.1/` |
| Spark 3.5.8 | `https://github.com/apache/spark/blob/v3.5.8/` |

주요 근거의 직접 링크:

- [`HadoopFileIO.deleteFiles()` — 가짜 bulk delete](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/hadoop/HadoopFileIO.java#L176-L194)
- [`S3FileIO.deleteFiles()` — 실제 `DeleteObjects` 일괄 삭제](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/aws/src/main/java/org/apache/iceberg/aws/s3/S3FileIO.java#L230-L300)
- [`ExpireSnapshotsSparkAction` — bulk 분기와 driver 삭제](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/ExpireSnapshotsSparkAction.java#L223-L272)
- [`S3URI` — scheme 무검증 (s3a 호환의 근거)](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/aws/src/main/java/org/apache/iceberg/aws/s3/S3URI.java#L60-L91)
- [`HiveCatalog` — `io-impl` 미설정 시 `HadoopFileIO` 고정](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveCatalog.java#L119-L123)
- [`S3AFileSystem.delete()` — 파일 1개 삭제의 요청 3개](https://github.com/apache/hadoop/blob/rel/release-3.3.4/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L3162-L3212)
- [`SparkContext` — eventLog 초기화 (S3A 필수의 근거)](https://github.com/apache/spark/blob/v3.5.8/core/src/main/scala/org/apache/spark/SparkContext.scala#L627-L633)


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
| `S3AFileSystem.java` | **`hadoop 3.3.4`** (실제 환경) | :3162 delete, :3172 HEAD, :3178·:3209 마커 LIST/PUT — 3.4.1과 동일 구조 |
| `hadoop-project-3.3.4.pom` | `hadoop 3.3.4` | `aws-java-sdk.version = 1.12.262` (환경 식별 근거) |
| `spark/v3.5/build.gradle` | `iceberg 1.10.1` | :241 `iceberg-spark-runtime`이 `iceberg-aws`는 포함하되 AWS SDK는 미포함 |
| `spark/v3.5/.../ExpireSnapshotsSparkAction.java` | `iceberg 1.10.1` | :223-229, :257-271 — v4.0 모듈과 동일 로직 |
| `BulkDeleteOperation.java` | `hadoop 3.4.1` / `hadoop-tools/hadoop-aws/.../impl/` | S3A의 bulk delete API 존재 확인 |
| `AwsClientProperties.java` | `iceberg 1.10.1` / `aws/.../aws/` | :145-149 region 미설정 시 동작, :211-235 자격증명 결정 순서 |
| `AwsClientFactory.java` | `iceberg 1.10.1` / `aws/.../aws/` | :23-25, :61 KMS/Glue/DynamoDB가 메서드 시그니처에 등장 (KMS jar 필수 원인) |
| `S3FileIOAwsClientFactories.java` | `iceberg 1.10.1` / `aws/.../aws/` | :41-47 `s3.client-factory` 미설정 시 `AwsClientFactories.from()`으로 폴백 |
| `aws-bundle/build.gradle` | `iceberg 1.10.1` | :27-42 번들 포함 모듈(kms/glue/dynamodb 등), :62-63 relocate 대상 |
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

---

## 9. 부록 — Iceberg 1.11.0 / Spark 4.1 업그레이드 검토

### 9.1 결론

**업그레이드에 찬성한다. 특히 Iceberg 1.11.0이 막혀 있던 Spark 4 문제의 직접적인 해답이다.**

다만 **FileIO 전환과 동시에 진행하지 않는다.** 두 변경을 겹치면 전환 효과를 측정할 수 없다 (섹션 9.5).

### 9.2 검증된 사실

| 항목 | 확인 내용 | 출처 |
|------|-----------|------|
| **Iceberg 1.11.0이 Spark 4.1을 정식 지원** | 릴리스 노트 Spark 항목 첫 줄이 `Support Spark 4.1 (#14155)`. `spark/v4.1` 모듈 존재 | ✅ `site/docs/releases.md`, 태그 소스 |
| 빌드 대상 Spark 버전 | `spark41 = "4.1.1"` — **목표 버전과 정확히 일치** | ✅ `gradle/libs.versions.toml` (1.11.0) |
| 런타임 jar 존재 | `iceberg-spark-runtime-4.1_2.13` 은 **1.11.0에만** 존재 | ✅ Maven Central |
| 1.10.1의 Spark 4.1 지원 | **없음** (`spark/v4.0`까지) — 기존 오류의 유력한 원인 | ✅ 태그 소스 |
| 릴리스 시점 | 1.11.0 = 2026-05-19 | ✅ 릴리스 노트 |
| JDK 요구사항 | Spark 4.1.1 `java.version = 17` — 현재 JDK 17로 충족 | ✅ `spark-parent_2.13-4.1.1.pom` |
| Scala | Spark 4.1.1 `scala.version = 2.13.17` — 2.13 계열 일치 | ✅ 동일 pom |

즉 **`iceberg-spark-runtime-4.1_2.13-1.11.0`이라는 조합이 지금 처음으로 성립한다.** "해결된 버전이 나오면 올린다"고 하신 그 버전이 이미 나와 있다.

### 9.3 기존 Iceberg 테이블 사이드 이펙트 — 사실상 없다 ✅

가장 중요한 근거는 format-version이다.

```java
// TableMetadata.java — 1.10.1과 1.11.0이 동일
static final int DEFAULT_TABLE_FORMAT_VERSION = 2;
static final int SUPPORTED_TABLE_FORMAT_VERSION = 4;
```

| 우려 | 실제 |
|------|------|
| 기존 테이블이 자동으로 format v3/v4로 올라가나 | **아니다.** Iceberg는 테이블 메타데이터에 선언된 `format-version`을 따른다. 라이브러리 업그레이드가 이를 바꾸지 않으며, 올리려면 `ALTER TABLE ... SET TBLPROPERTIES('format-version'='3')`을 **명시적으로** 실행해야 한다 |
| 새로 만드는 테이블은 | **여전히 v2다.** 기본값이 1.10.1과 동일하다 |
| 데이터 파일(Parquet) 포맷이 바뀌나 | 아니다 |
| 기존 snapshot / time travel | 영향 없음 |
| 파티션 스펙 / Sort Order / 테이블 프로퍼티 | 영향 없음 |
| HMS 카탈로그 등록 정보 | 영향 없음 |

**테이블은 그대로 있고 읽고 쓰는 라이브러리만 바뀐다.** FileIO 전환과 같은 성격이다.

### 9.4 진짜 위험은 테이블이 아니라 스택에 있다

#### ⚠️ ① Hadoop 3.3.4 → 3.4.2: S3A의 AWS SDK가 v1에서 v2로 바뀐다

이것이 **가장 큰 항목**이다. Spark 4.1.1은 Hadoop 3.4.2를 번들한다.

| | 현재 (Spark 3.5.8) | 업그레이드 후 (Spark 4.1.1) |
|---|---|---|
| Hadoop | 3.3.4 | **3.4.2** |
| S3A의 AWS SDK | v1 `com.amazonaws:aws-java-sdk-bundle` | **v2 `software.amazon.awssdk:bundle`** ✅ (`hadoop-aws-3.4.2.pom`) |
| Iceberg의 AWS SDK | 2.33.0 (1.10.1) | 2.44.4 (1.11.0) ✅ (`libs.versions.toml`) |

파급 두 가지:

1. **SDK v2가 한 JVM에 두 벌 올라온다.** 지금은 v1(S3A)과 v2(S3FileIO)가 패키지가 달라 평화롭게 공존하지만, 업그레이드 후에는 **둘 다 `software.amazon.awssdk.*`**다. Hadoop이 가져오는 버전과 `iceberg-aws-bundle 1.11.0`의 2.44.4가 충돌할 수 있다. ⚠️ 클래스패스 우선순위 확인 필요
2. **MinIO checksum 이슈가 S3A로도 번진다.** 지금까지 섹션 4.5의 checksum 문제는 S3FileIO만의 문제였다(S3A는 SDK v1이라 무관). 업그레이드 후에는 **원천 avro 읽기까지 SDK v2를 타므로**, MinIO 호환성이 안 맞으면 파이프라인 전체가 멈춘다. **즉 MinIO checksum 확인은 이번 전환뿐 아니라 Spark 4 업그레이드의 선결 조건이기도 하다**

#### ⚠️ ② `Remove deprecations for 1.11.0` (#14059)

릴리스 노트의 Deprecation 항목에 **`AWS, Core, Data, Spark: Remove deprecations for 1.11.0`**이 있다. deprecated API가 실제로 제거됐다는 뜻이다.

**maintenance 함수를 실행하는 Scala 코드가 Iceberg API를 직접 호출한다면 컴파일/런타임 실패 가능성이 있다.** SQL 프로시저(`CALL ... expire_snapshots`)만 쓴다면 영향이 없다. 코드에서 `org.apache.iceberg.*`를 얼마나 직접 참조하는지에 따라 공수가 갈린다 — **업그레이드 전에 확인할 첫 번째 항목이다.**

함께 확인할 것: **Java 11 지원 중단** (JDK 17이므로 무관), **Spark 3.4 지원 deprecate** (무관).

#### ⚠️ ③ Trino 호환성

Trino가 같은 테이블을 조회한다. v2 테이블에 새 기능을 켜지 않는 한 안전하지만, Iceberg 1.11.0이 새로운 optional 메타데이터(partition statistics, content stats 등)를 쓰기 시작하면 구버전 Trino 커넥터가 읽지 못할 수 있다. **Trino의 Iceberg 커넥터 버전을 확인하고, 업그레이드 후 Trino 조회를 반드시 회귀 테스트한다.** ⚠️

#### ⚠️ ④ 튜닝 결과 재검증

작업 1/5의 확정 설정은 **Iceberg 1.10.1 + Spark 3.5 기준 실측값**이다. 릴리스 노트에 `Fix BinPackRewriteFilePlanner producing incorrect output file count with max-files-to-rewrite (#15576)` 같은 Compaction 계획 로직 변경이 포함되어 있다.

| 재검증 대상 | 현재 기준값 |
|-------------|-------------|
| Compaction `dcu/GB` | 0.00219 |
| Compaction `초/GB` | 2.41 |
| `max-concurrent-file-group-rewrites` | 10 |
| `num-executors` | 12 (C=0.32 동적 산정식 포함) |
| append Job 벤치마크 | `spark-tuning-guide.md` |

Spark 3.5 → 4.1은 실행 엔진 자체가 바뀌므로 **AQE 동작, ANSI 모드 기본값 등이 달라질 수 있다.** 노이즈 기준선 ±15%로 재측정이 필요하다.

#### ⚠️ ⑤ Scala 버전 확인

현재 런타임 jar가 `iceberg-spark-runtime-**3.5_2.12**`인데 Scala 2.13을 쓰신다고 하셨다. **Spark 3.5.8 이미지가 Scala 2.12 빌드일 가능성이 높다.** 애플리케이션 jar가 2.13으로 빌드되어 있다면 현재 조합이 성립하지 않으므로, 어느 쪽이 맞는지 먼저 확인해야 한다. (SQL만 실행하는 Job이라면 문제가 드러나지 않았을 수 있다.)

### 9.5 업그레이드하면 얻는 것 — maintenance 관련

부수적이지만 이번 작업과 직결되는 개선이 여럿 있다.

| 변경 | 의미 |
|------|------|
| **`Refresh table in ListMetadataFiles to prevent incorrect orphan file deletion` (#16324)** | **`remove_orphan_files`의 오삭제 방지 수정.** 데이터 안전성 항목이므로 우리 환경에 해당하는지 확인할 가치가 있다 ⚠️ |
| `enable stream-results option for remove orphan files` (#14278) | orphan에도 `stream-results` 적용 가능 → driver 메모리 부담 감소 |
| `Support cleanupMode in snapshot expiration` (#14287), `expire-snapshots with cleanupLevel=None` (#14695) | expire의 파일 정리 동작을 제어할 수 있다 |
| `Fix BinPackRewriteFilePlanner ... max-files-to-rewrite` (#15576) | Compaction 출력 파일 수 계산 버그 수정 |
| `AWS: Add scheduled refresh for the S3FileIO held storage credentials` (#15678) | 장기 실행 Job의 자격증명 갱신 |
| `Don't Use table FileIO for Spark Checkpoints` (#15239) | FileIO 분리 관련 정리 |
| `Add sort_by parameter to rewrite_manifests procedure` (#15467) | manifest 정리 옵션 |

### 9.6 권장 순서

**FileIO 전환(Phase 1)을 먼저 끝내고, 그다음 버전업한다.**

| 단계 | 내용 | 이유 |
|------|------|------|
| **1** | 현재 스택(Spark 3.5.8 + Iceberg 1.10.1)에 `iceberg-aws-bundle-1.10.1` 추가 → **Phase 1 측정 완료** | MinIO 부하는 **지금 겪고 있는 운영 문제**다. 전환은 저위험·즉시 롤백 가능하므로 먼저 해소한다 |
| **2** | 측정 결과 확정 (요청 수 −99.6% 검증) | 이 숫자가 **버전과 무관한 구조적 개선**이므로 한 번 증명하면 재증명이 필요 없다 |
| **3** | Scala 코드의 Iceberg API 직접 참조 범위 조사 (§9.4-②) | 업그레이드 공수를 여기서 가늠한다 |
| **4** | Iceberg 1.11.0 + Spark 4.1.1로 업그레이드 | jar는 `iceberg-spark-runtime-4.1_2.13-1.11.0` + `iceberg-aws-bundle-1.11.0` |
| **5** | 회귀 검증 — Trino 조회, Compaction/append 벤치마크 재측정 | §9.4-③④ |

**동시에 하지 말아야 하는 이유**: FileIO 전환 효과(`DeleteObjects` 요청 수)와 버전업 효과(엔진 변경)가 섞이면 어느 쪽이 무엇을 했는지 분리할 수 없다. 특히 §9.4-①에서 S3A까지 SDK v2로 바뀌므로, 문제가 생겼을 때 원인 후보가 두 배가 된다.

> **반론도 성립한다**: Spark 3.5.8은 어차피 임시 환경이므로 거기서 잰 baseline은 곧 폐기된다는 관점이다. 그렇다면 버전업을 먼저 하고 FileIO 전환을 한 번만 측정하는 것도 합리적이다. **다만 그동안 MinIO 부하는 계속된다.** 부하가 당장 견딜 만하고 Spark 4 전환 일정이 이미 잡혀 있다면 이쪽을 택해도 된다.

### 9.7 업그레이드 시 jar 구성

```
# 현재 (Spark 3.5.8 + Iceberg 1.10.1)
$SPARK_HOME/jars/
├── aws-java-sdk-bundle-1.12.262.jar            # S3A용 (SDK v1)
├── iceberg-spark-runtime-3.5_2.12-1.10.1.jar
└── iceberg-aws-bundle-1.10.1.jar               # 이번에 추가

# 업그레이드 후 (Spark 4.1.1 + Iceberg 1.11.0)
$SPARK_HOME/jars/
├── bundle-<Hadoop 3.4.2가 가져오는 버전>.jar    # S3A용 (SDK v2) — Spark 배포판에 포함
├── iceberg-spark-runtime-4.1_2.13-1.11.0.jar   # ★ 교체 (Spark·Scala 버전 변경)
└── iceberg-aws-bundle-1.11.0.jar               # ★ 버전만 교체
```

`iceberg-aws-bundle`은 여전히 **Scala 접미사도 Spark 버전 의존성도 없다.** Iceberg 버전만 맞추면 된다.

⚠️ **SDK v2 중복 확인**: 업그레이드 후 `software.amazon.awssdk` 클래스가 두 jar에 존재하게 된다. 실제 로드되는 버전을 확인해야 한다.

```bash
kubectl exec <driver-pod> -- sh -c \
  'for j in $SPARK_HOME/jars/*.jar; do \
     n=$(unzip -l "$j" 2>/dev/null | grep -c "software/amazon/awssdk/services/s3/S3Client.class"); \
     [ "$n" -gt 0 ] && echo "$j"; \
   done'
```

### 9.8 확인이 필요한 항목

| 항목 | 우선순위 |
|------|----------|
| Scala 코드의 Iceberg API 직접 참조 범위 (§9.4-②) | **최우선** — 업그레이드 공수를 결정한다 |
| MinIO checksum 호환성 | **최우선** — FileIO 전환과 Spark 4 업그레이드 **양쪽의 게이트** (§9.4-①) |
| 현재 Spark 3.5.8 이미지의 Scala 버전 (2.12 / 2.13) | 높음 (§9.4-⑤) |
| Trino Iceberg 커넥터 버전과 호환성 | 높음 (§9.4-③) |
| 업그레이드 후 SDK v2 중복 해소 | 높음 (§9.7) |
| Compaction / append 튜닝값 재검증 | 중간 (§9.4-④) |
| `remove_orphan_files` 오삭제 버그(#16324) 해당 여부 | 중간 (§9.5) |
| 과거 Spark 4 오류 메시지 확보 | 중간 — 1.11.0으로 해소되는지 확인 근거 |
