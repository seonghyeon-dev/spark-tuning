# Spark Job 설정 튜닝 가이드 문서 작성

## 프로젝트 목표
`spark-tuning-guide-draft.md` 초안을 기반으로, Confluence에 바로 복사/붙여넣기 가능한 **Spark Job 설정 튜닝 가이드 완성본**(`spark-tuning-guide.md`)을 작성한다.

## Code Style Rules
- 커밋 메시지는 한글로 작성
- 결과값과 설명은 무조건 한글로 작성

## 기술 스택
- Spark 4.1.1
- Iceberg 1.10.1
- Airflow 3.1.7
- Kubernetes 클러스터 (Spark Pod 실행 환경)
- S3 (MinIO) 스토리지
- Oracle DB (처리 대상 상태 관리)
- SparkKubernetesOperator (kubeflow)

## 작업 순서

1. `project-context.md`를 읽고 워크플로우/아키텍처를 파악한다.
2. `spark-tuning-guide-draft.md`를 읽고 초안 구조를 파악한다.
3. 아래 공식 문서 URL들을 웹 검색하여 각 설정의 공식 설명, 기본값, 권장사항을 조사한다.
4. 초안의 <!-- --> 주석 지침에 따라 각 섹션의 내용을 채운다.
5. 완성된 문서를 `spark-tuning-guide.md`로 저장한다.

## 참고해야 할 공식 문서 URL
- Spark 4.1.1 Configuration: https://spark.apache.org/docs/4.1.1/configuration.html
- Spark 4.1.1 SQL Performance Tuning: https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html
- Spark on Kubernetes: https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html
- Iceberg 1.10.1 Spark Configuration: https://iceberg.apache.org/docs/1.10.1/spark-configuration/

## 작성 대상 Spark 옵션 (7개)

각 옵션에 대해 아래 항목을 모두 작성:
- 공식 문서 기반 설명 (무엇을 제어하는 설정인지)
- 기본값 (공식 문서 기준)
- 권장값 및 그 이유
- 우리 워크플로우(avro → Iceberg append)에서의 의미
- 근거 수준 라벨 (아래 3가지 중 하나)

| 옵션 | 특이사항 |
|------|----------|
| `driver-cores` | 일반적으로 많이 사용하는 적절한 값 제시 |
| `driver-memory` | 일반적으로 많이 사용하는 적절한 값 제시 |
| `executor-cores` | 일반적으로 많이 사용하는 적절한 값 제시 |
| `executor-memory` | 일반적으로 많이 사용하는 적절한 값 제시 |
| `num-executors` | 현재: avro 총 크기 / 70MB + 1. **근거 부족하므로 개선 필요 명시** |
| `spark.sql.shuffle.partitions` | 현재: 70. **근거 부족하므로 개선 필요 명시** |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | 현재: true. 공식 문서 기반 설명 |

## 근거 수준 라벨 (3단계)
- **✅ 공식 권장**: 공식 문서에서 권장하거나 명시한 값
- **📘 일반적 관행**: 커뮤니티에서 널리 사용. 출처 표기
- **⚠️ 테스트 기반 (개선 필요)**: 테스트로 결정한 값. 명확한 근거 부족. 벤치마크/프로파일링으로 재검증 필요

## 문서 포맷 요구사항
- 한국어 작성
- Confluence 호환 마크다운 (표, 코드블록, 헤더, 인용블록 등)
- 각 설정마다 "왜 이 값인가" 설명 필수
- 개선 필요 항목은 `> ⚠️ **개선 필요**` 인용블록으로 표시
- SparkKubernetesOperator 설정 예시 코드 포함
- spark-submit 명령어 예시 포함
- 설정 근거 요약 표 포함
- 개선 로드맵 섹션 포함

## 최종 산출물
- `spark-tuning-guide.md`: Confluence 복사/붙여넣기용 완성 문서
