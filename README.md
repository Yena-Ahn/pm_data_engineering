### 2.1 Pandas

data is a content play log from contents.

```python3
{
  "company_token|player_token|slug": str, # 식별자 grouping
  "answers": dict, # 사용자의 질문에 대한 답변 데이터 { 질문ID: 답변ID }
  "result_id": str, # 해당 로그의 ID
  "companyToken": str, # 식별값
  "playerToken": str, # 식별값
  "pomangUserCookie": str, # 유저 식별값
  "slug": str, # 컨텐츠 식별값
  "resultCode": str,
  "timestamp": int, # 타임스탬프
  "created_at": str, # ISOString DateTime
  "userAgent": str # 접속 환경
  "rawUrl": str # 해당 로그가 발생한 요청 URL
}
```

Preprocessing these data:

- 1. Play times per content each date.
  - Create a week-hour heatmap to analyse time where visitors mostly visit the website.
  - Store data in CSV format with a graph.
- 2. Use the result code to find tendencies of the main users.
  - userAgent shows which device users use to access the website.
  - Analyse data with resultCode, contents, and user agent.
  - Store data in CSV format with a graph.


### 2.2 Airflow


```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.3/docker-compose.yaml'
```

- Airflow environment setting
  - docker-compose
  - Create an Airflow DAG
    - Create a Dag which creates `hello_world.txt` file which contains "Hello world!" text every Friday 6p.m. (KR time)
    - Create a DAG to create `{date_time_string}.txt` file every hour with the current time as its file name.
    - Create a Dag to perform 2.1Pandas above

