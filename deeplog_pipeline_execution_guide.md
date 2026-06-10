# 🚀 DeepLog 실시간 로그 이상 탐지 파이프라인 전체 실행 가이드

이 가이드는 DeepLog 모델의 로컬 오프라인 학습부터 GCP/GKE 환경의 KServe 실시간 서빙, Kafka 데이터 파이프라인 및 Prometheus/Grafana/Alertmanager(이메일 경보)를 활용한 모니터링 환경을 엔드투엔드로 구축하는 실전 운영 메뉴얼입니다.

---

## 📋 1. 사전 요구사항 (Prerequisites)

GCP 인프라 구축 및 파이프라인 실행을 위해 로컬 환경에 아래 도구들이 설치 및 설정되어 있어야 합니다.

*   **CLI 도구**: `gcloud`, `kubectl`, `helm`, `terraform`
*   **개발 도구**: Python 3.10+, `uv` (패키지 매니저), `Docker`
*   **GCP 권한**: 프로젝트 생성 권한, IAM 관리자 및 GCS/Artifact Registry 생성 권한

### GCP 로그인 및 프로젝트 설정
```bash
# GCP 로그인
gcloud auth login
gcloud auth application-default login

# 대상 GCP 프로젝트 지정
export GCP_PROJECT_ID="pro-platform-495513-b9"
gcloud config set project $GCP_PROJECT_ID
```

---

## 📂 2. Phase 1 & 2: 로컬 데이터 전처리 및 학습

### ① 가상환경 생성 및 의존성 설치
```bash
# 가상환경 구성
uv venv
source .venv/bin/activate

# 의존성 패키지 설치
uv pip install -r requirements.txt
```

### ② HDFS 로그 파싱 및 학습 데이터셋 가공
[parser.py](file:///Users/hwangbyeonghyeon/Desktop/personal/anoamy_detection_project/src/parser.py)를 실행해 HDFS 원시 로그를 Drain3 템플릿 마이너로 파싱하고 `preprocessed_sample.csv` 및 Drain3 상태 파일(`drain_state.bin`)을 생성합니다.
```bash
python src/parser.py
```

### ③ DeepLog 모델 학습 및 로컬 MLflow 로깅
[train.py](file:///Users/hwangbyeonghyeon/Desktop/personal/anoamy_detection_project/src/train.py)를 실행해 LSTM 모델을 학습시키고 가중치 `deeplog_lstm.pth`와 `vocab.json`을 모델 저장소로 기록합니다.
```bash
python src/train.py --epochs 5 --batch_size 256
```

---

## ☁️ 3. Phase 3: GCP 리소스 및 CI/CD 구축

### ① Terraform 기반 GCP 리소스 프로비저닝
GCS 버킷(모델 레지스트리용) 및 Artifact Registry(컨테이너 레지스트리)를 생성합니다.
```bash
cd terraform/
terraform init
terraform apply -auto-approve
cd ..
```

### ② 모델 가중치 및 단어장(Vocab) GCS 업로드
KServe가 참조할 원격 GCS 경로에 모델 가중치 파일과 Vocabulary 맵을 규격에 맞춰 업로드합니다.
```bash
# GCS 버킷에 가중치 업로드
gsutil cp models/deeplog_lstm.pth gs://deeplog-mlflow-model-registry/deeplog-serving/current/deeplog_lstm.pth
gsutil cp src/vocab.json gs://deeplog-mlflow-model-registry/deeplog-serving/current/vocab.json
```

### ③ 도커 이미지 빌드 및 Artifact Registry 푸시
커스텀 Predictor와 실시간 전처리 서비스 이미지를 빌드하여 Google Artifact Registry(GAR)에 푸시합니다.
```bash
# GCP Artifact Registry 로그인
gcloud auth configure-docker asia-northeast3-docker.pkg.dev

# 1. KServe 커스텀 Predictor 이미지 빌드 및 푸시
docker build -t asia-northeast3-docker.pkg.dev/$GCP_PROJECT_ID/deeplog-repo/deeplog-predictor:latest -f docker/Dockerfile.predictor .
docker push asia-northeast3-docker.pkg.dev/$GCP_PROJECT_ID/deeplog-repo/deeplog-predictor:latest

# 2. 실시간 전처리 및 컨슈머 서비스 이미지 빌드 및 푸시
docker build -t asia-northeast3-docker.pkg.dev/$GCP_PROJECT_ID/deeplog-repo/preprocessing-service:latest -f docker/Dockerfile.preprocess .
docker push asia-northeast3-docker.pkg.dev/$GCP_PROJECT_ID/deeplog-repo/preprocessing-service:latest
```

---

## ☸️ 4. Phase 4 & 5: GKE 클러스터 및 추론 서비스 배포

### ① Workload Identity 및 IAM 설정
GKE 내의 `deeplog-model-reader` 서비스 어카운트가 GCS 버킷의 가중치 파일을 가져올 수 있도록 권한을 설정합니다.
```bash
# GCP 서비스 계정에 Storage Object Viewer 권한 부여
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:deeplog-model-reader@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"

# GCP 서비스 계정과 Kubernetes 서비스 계정 간 Workload Identity 바인딩
gcloud iam service-accounts add-iam-policy-binding \
    deeplog-model-reader@$GCP_PROJECT_ID.iam.gserviceaccount.com \
    --role="roles/iam.workloadIdentityUser" \
    --member="serviceAccount:$GCP_PROJECT_ID.svc.id.goog[deeplog/deeplog-model-reader]"
```

### ② KServe Serverless 스택 설치
KNative Serving, Istio 서비스 메쉬 및 KServe 컨트롤러를 GKE 클러스터에 배포합니다.
```bash
chmod +x k8s/kserve/install.sh
./k8s/kserve/install.sh
```

### ③ KServe InferenceService 배포
LSTM 모델을 서빙할 예측기 Pod를 띄웁니다.
```bash
# 네임스페이스 생성 및 서비스 계정/InferenceService 배포
kubectl create namespace deeplog --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/kserve/serviceaccount.yaml
kubectl apply -f k8s/kserve/inferenceservice.yaml

# 배포 정상 상태 확인 (READY가 True가 될 때까지 확인)
kubectl get inferenceservice -n deeplog
```

### ④ FastAPI 전처리 및 컨슈머 서비스 배포
Kafka로부터 로그를 구독해 정제하고, KServe로 이상 감지를 요청할 컨슈머 서비스를 배포합니다.
```bash
kubectl apply -f k8s/preprocess-deployment.yaml

# 파이프라인 컨슈머 기동 로그 확인
kubectl logs -n deeplog deployment/preprocessing-service -f
```

---

## 📊 5. Phase 6: 관측 가능성 (Observability) 및 이메일 알림 연동

### ① Prometheus & Grafana 모니터링 스택 설치
```bash
# 모니터링 스택 Helm 설치
chmod +x k8s/monitoring/install.sh
./k8s/monitoring/install.sh
```

### ② ServiceMonitor & Prometheus Alerting Rules 적용
```bash
# 전처리 서비스 Scrape 설정 적용
kubectl apply -f k8s/monitoring/servicemonitor.yaml

# 지연 시간, 이상 탐지 비율, Kafka Lag 임계치 모니터링 룰 배포
kubectl apply -f k8s/monitoring/prometheus-rules.yaml
```

### ③ Alertmanager 이메일(SMTP) 알림 활성화
1. [alertmanager-values.yaml](file:///Users/hwangbyeonghyeon/Desktop/personal/anoamy_detection_project/k8s/monitoring/alertmanager-values.yaml) 파일을 열어 본인의 SMTP 서버(예: Gmail) 정보와 수신/발신 이메일 주소를 입력합니다.
2. Helm 업그레이드를 통해 알림 설정을 클러스터에 반영합니다.
```bash
helm upgrade prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  -f k8s/monitoring/alertmanager-values.yaml
```

### ④ Grafana 대시보드 연동
1. Grafana 웹 콘솔로 포트 포워딩을 수행합니다.
   ```bash
   kubectl port-forward svc/prometheus-stack-grafana -n monitoring 3000:80
   ```
2. 웹 브라우저로 `http://localhost:3000`에 접속합니다. (ID: `admin`, PW: `prom-operator`)
3. **Dashboards > Import**에서 [grafana-dashboard.json](file:///Users/hwangbyeonghyeon/Desktop/personal/anoamy_detection_project/k8s/monitoring/grafana-dashboard.json) 파일의 JSON 전체 텍스트를 복사하여 붙여넣고 가져옵니다.

---

## 🎯 6. 엔드투엔드 파이프라인 최종 검증

### ① Mock 로그 생성기 기동
쿠버네티스 혹은 로컬에서 로그 발생기를 가동하여 실시간 트래픽을 모사합니다.
```bash
# 로컬에서 로그 수집/적재 테스트
python src/verify_ingestion.py
```

### ② 대량 이상 모사 및 이메일 수신 확인
1. 인위적으로 이상 템플릿(비정상 Sequence) 로그 데이터를 대량 발생시킵니다.
2. Grafana 대시보드에서 `Anomaly Rate` 그래프가 솟구치는지 실시간으로 모니터링합니다.
3. 5분 평균 이상 비율이 15%를 상회할 시, 사전에 설정해 둔 수신 이메일로 HTML 경보 메일이 정상 도착하는지 최종 검증합니다.
