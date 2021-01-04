# Spark
## 데이터 셋
### **RDD( 탄력적 분산 데이터셋)**
- 클러스터의 여러 노드에 분산할 수 있는 객체들의 컬렉션
- 한번 생성되면 수정이 불가
- RDD 생성하는 법
  - (1) SparkContext를 이용하여 외부( HDFS, 스파크 셸에서 생성한 로컬 캑체 컬렉션 , ...)로부터 생성
  - (2) 이미 만들어진 RDD를 필터링, 집계, 조인 등의 가공으로 새로 생성

- Action 관련 함수와 Transformation 관련 함수
  - Action 관련 함수
    - rdd를 만든다고 클러스터에서 어떤 분산처리 실행되는 것이 아님. rdd는 연산의 중간 단계들의 논리적 데이터 셋이며 **분산처리는 rdd상에서 action이 이루어질때** 발생
      - foreach()
      - collect()
      - take()
      - count()
      - reduce() 
        - map()과 같은 역할
    - Transformation 관련 함수
      - map()
      - flatMap()
      - filter()
      ...
