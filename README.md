# 푸망 채용 프로세스

안녕하세요. 푸망입니다. 푸망에서는 훌륭한 인재분들을 모시기 위해 채용 프로세스 중에 과제 전형을 실시하고 있습니다.
실무에서 사용되는 기술을 기반으로 준비된 과제를 진행해주시면 됩니다.

지원해주신 직무는 **DE(데이터 엔지니어링)**입니다. 혹시 잘못된 직무의 과제가 나왔을 경우 바로 알려주시기 바랍니다.

## 1. 제출

준비된 과제를 모두 진행해주시고 PullRequest(이하 PR)을 만들어주시면 됩니다. PR을 만드는 방법은 아래와 같습니다.

1. 현재 이 레포지토리를 내려받는다. `git clone`
2. 브랜치를 생성한다. `git branch ${brnach name}`
3. 생성한 브랜치로 이동한다. `git checkout $[branch name}`
4. 과제를 진행후 커밋한다. `git commit ~~`
5. 브랜치를 이 레포지토리로 푸시한다. `git push origin ${branch name}`
6. 상단의 `Pull Requests` 탭을 클릭 후 생성한다.

더 자세한 설명은 [깃헙 풀리퀘스트 설명](https://docs.github.com/ko/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)을 참조하세요



## 2. 과제

DE에게 필요한 역량은 데이터의 처리와 데이터 파이프라이닝 환경 구축입니다. 아래의 내용을 잘 읽고 해결해주시기 바랍니다.


### 2.1 Pandas

`1_pandas` 디렉토리 하위에 있는 `data`를 활용하여 다음의 문제를 해결하여 주시면 됩니다. 각 문제의 번호로 디렉토리를 생성하고 코드와 결과물을 넣어주시면 됩니다.

ex) 1번 문제 : `1_pandas/1/code.py`, `1_pandas/1/answer.json` 파일 생성

data는 푸망 컨텐츠에서 발생한 컨텐츠 플레이 로그입니다.

요청-응답 로그는 다음의 구조를 가지고 있습니다.
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

이 데이터를 분석이 가능한 형태로 가공하고자 합니다.

- [ ] 1. 컨텐츠 별 날짜별로 플레이 횟수를 알고 싶습니다. 
  - [ ] 컨텐츠마다 날짜별로 플레이 횟수를 알고 싶습니다.
  - [ ] 사용자들이 주로 접속하는 시간대가 있을 것 같습니다. 주-시간별 히트맵을 만들어주세요
  - [ ] csv 형태의 데이터를 저장하세요.
  - [ ] 그래프와 함께 첨부하세요
- [ ] 2. 결과 값은 resultCode에 저장됩니다. 결과 코드를 이용하여 주 사용층에 대한 경향성을 알 수 있을 것 같습니다.
  - [ ] userAgent를 이용하여 어떤 디바이스에서 사용자가 접근하는 지 알 수가 있습니다.
  - [ ] resultCode로 컨텐츠, user agent 등으로 분석이 가능한 데이터 형태를 만들어주세요.
  - [ ] csv 형태의 데이터를 저장하세요.
  - [ ] 그래프를 첨부하세요.


### 2.2 Airflow

Airflow는 컨테이너 환경에서 돌아가는 것이 환경 구성이 더 용이합니다. [Airflow 공식 문서](https://airflow.apache.org/docs/apache-airflow/stable/index.html)를 확인하고 환경 구축 및 샘플 작업을 추가해주세요.

`2_airflow`에 있는 `docker-compose.yml` 파일은 Airflow에서 제공합니다.

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.3/docker-compose.yaml'
```

- [ ] Airflow 환경 구축
  - [ ] Airflow 환경 셋업, docker-compose를 사용합니다.
  - [ ] Airflow DAG 생성 후 실행
    - [ ] 매주 금요일 18시(한국 기준)에 "Hello world!"라는 텍스트가 입려된 `hello_world.txt` 파일을 생성하는 DAG를 생성하세요.
    - [ ] 매 1시간마다 현재 시간의 이름으로 `{date_time_string}.txt` 파일을 생성하는 DAG를 생성하세요.
    - [ ] 위 `2.1Pands` 1번 과제를 실행하는 DAG를 생성하세요.


## 주의사항

필요 없는 파일이 커밋에 포함되지 않게 해주시기 바랍니다.


## 마무리

과제는 1주일이라는 시간이 조금 빠듯하게 준비했습니다. 질문은 언제든지 명함에 써있는 번호로 주시기 바랍니다. 감사합니다.
