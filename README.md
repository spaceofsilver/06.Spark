# Spark
## 데이터 셋
### RDD( 탄력적 분산 데이터셋)
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
    
### 데이터 프레임
- 맵리듀스에는 없는 형태
- 데이터 프레임의 스키마와 데이터 첫 몇 줄을 살펴보아야 함.
- 데이터 프레임 생성 방법
  - (1) RDD를 **toDF()** 를 이용하여 생성
  - (2) SQLContext의 멤버함수로 데이터 프레임 만들기
    - sqlCtx.createDataFrame( rdd , ['컬럼명'] )
  - (3) Pandas 데이터 프레임으로 형성
    - spark 데이터 프레임을 toPandas()로 판다스형 데이터 프레임으로 변환
    - **RDD.collect( )와 같은 개념!** 따라서 필터링 다 한 후 사용하는 것이 맞다
    
- sql 구문 이용
  - (1) sql 쿼리
    - HIVE ql 함수
      - https://rfriend.tistory.com/213
      - https://docs.microsoft.com/ko-kr/azure/hdinsight/hadoop/hdinsight-use-hive
    - Spark SQL 구문 라이브러리
      - https://spark.apache.org/docs/latest/api/sql/index.html
  - (2) sql쿼리로 데이터 분석 실행
    - Spark SQL을 사용하기 위해선 분산 데이터 프레임에 이름을 부여해야함
<br>

        df.createOrReplaceTempView( '이름' )
        sql = 'select * from my'
        sqlDF = sqlCtx.sql( sql )
        sqlDF.show() # spark 데이터 프레임을 반환

