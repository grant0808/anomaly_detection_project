# DeepLog GKE Deployment Runbook

이 문서는 현재 저장소의 DeepLog 기반 HDFS 로그 이상 탐지 파이프라인을 GCP GKE에서 실제로 실행하는 절차를 정리한다.

대상 아키텍처:

```text
log-generator
  -> Kafka hdfs-raw-logs
  -> preprocessing-service
  -> KServe deeplog-serving
  -> Kafka anomaly-alerts
  -> Prometheus / Grafana / Alertmanager
```

## 1. 사전 준비

로컬에 다음 도구가 필요하다.

```bash
gcloud --version
kubectl version --client
helm version
terraform version
docker version
python --version
```

GCP 프로젝트와 리전을 설정한다.

```bash
export GCP_PROJECT_ID="pro-platform-495513-b9"
export GCP_REGION="asia-northeast3"
export GKE_CLUSTER_NAME="deeplog-k8s-cluster"
export ARTIFACT_REPO="deeplog-repo"
export MODEL_BUCKET="deeplog-mlflow-model-registry-hbh"

gcloud auth login
gcloud auth application-default login
gcloud config set project "$GCP_PROJECT_ID"
```

필요한 GCP API를 활성화한다.

```bash
gcloud services enable \
  compute.googleapis.com \
  container.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com
```

## 2. 인프라 생성

Terraform은 GKE 클러스터, GCS 버킷, Artifact Registry Docker repository를 생성한다.

```bash
cd terraform
terraform init
terraform plan \
  -var="gcp_project_id=$GCP_PROJECT_ID" \
  -var="gcp_region=$GCP_REGION"
terraform apply \
  -var="gcp_project_id=$GCP_PROJECT_ID" \
  -var="gcp_region=$GCP_REGION"
cd ..
```

GKE kubeconfig를 설정한다.

```bash
gcloud container clusters get-credentials "$GKE_CLUSTER_NAME" \
  --region "$GCP_REGION" \
  --project "$GCP_PROJECT_ID"

kubectl get nodes
```

## 3. 모델 아티팩트 준비

KServe predictor는 GCS의 다음 경로에서 모델 파일을 읽는다.

```text
gs://deeplog-mlflow-model-registry-hbh/deeplog-serving/current/vocab.json
gs://deeplog-mlflow-model-registry-hbh/deeplog-serving/current/deeplog_lstm.pth
```

이미 학습된 모델이 있으면 `models/deeplog_lstm.pth`와 `src/vocab.json`이 존재하는지 확인한다.

```bash
test -f src/vocab.json
test -f models/deeplog_lstm.pth
```

모델이 없다면 로컬에서 파싱과 학습을 먼저 수행한다.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python src/parser.py
python src/train.py --epochs 5 --batch_size 256
```

모델과 vocabulary를 GCS에 업로드한다.

```bash
gsutil cp src/vocab.json \
  "gs://$MODEL_BUCKET/deeplog-serving/current/vocab.json"

gsutil cp models/deeplog_lstm.pth \
  "gs://$MODEL_BUCKET/deeplog-serving/current/deeplog_lstm.pth"

gsutil ls "gs://$MODEL_BUCKET/deeplog-serving/current/"
```

## 4. Workload Identity 권한 설정

현재 [k8s/kserve/serviceaccount.yaml](k8s/kserve/serviceaccount.yaml)은 Kubernetes ServiceAccount `deeplog-model-reader`를 다음 GCP Service Account에 매핑한다.

```text
github-action@pro-platform-495513-b9.iam.gserviceaccount.com
```

현재 매니페스트를 그대로 사용할 경우 해당 GSA에 GCS 읽기 권한과 Workload Identity 사용 권한을 부여한다.

```bash
export MODEL_READER_GSA="github-action@$GCP_PROJECT_ID.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:$MODEL_READER_GSA" \
  --role="roles/storage.objectViewer"

gcloud iam service-accounts add-iam-policy-binding "$MODEL_READER_GSA" \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:$GCP_PROJECT_ID.svc.id.goog[deeplog/deeplog-model-reader]"
```

권한 적용 후에는 다음 annotation 값과 GSA가 일치해야 한다.

```bash
grep -n "iam.gke.io/gcp-service-account" k8s/kserve/serviceaccount.yaml
```

## 5. 컨테이너 이미지 빌드와 푸시

Artifact Registry Docker 인증을 설정한다.

```bash
gcloud auth configure-docker "$GCP_REGION-docker.pkg.dev"
```

이미지 태그를 현재 커밋 기준으로 만든다.

```bash
export IMAGE_TAG="$(git rev-parse HEAD)"
export IMAGE_REPO="$GCP_REGION-docker.pkg.dev/$GCP_PROJECT_ID/$ARTIFACT_REPO"
```

세 개 이미지를 빌드하고 푸시한다.

```bash
docker build -t "$IMAGE_REPO/deeplog-predictor:$IMAGE_TAG" -f docker/Dockerfile.predictor .
docker push "$IMAGE_REPO/deeplog-predictor:$IMAGE_TAG"

docker build -t "$IMAGE_REPO/preprocessing-service:$IMAGE_TAG" -f docker/Dockerfile.preprocess .
docker push "$IMAGE_REPO/preprocessing-service:$IMAGE_TAG"

docker build -t "$IMAGE_REPO/log-generator:$IMAGE_TAG" -f docker/Dockerfile.log_generator .
docker push "$IMAGE_REPO/log-generator:$IMAGE_TAG"
```

Kubernetes 매니페스트의 이미지 태그를 방금 푸시한 태그로 맞춘다.

```bash
perl -0pi -e "s#asia-northeast3-docker\.pkg\.dev/[^\\s]+/deeplog-predictor:[^\\s]+#$ENV{IMAGE_REPO}/deeplog-predictor:$ENV{IMAGE_TAG}#g" \
  k8s/kserve/inferenceservice.yaml

perl -0pi -e "s#asia-northeast3-docker\.pkg\.dev/[^\\s]+/preprocessing-service:[^\\s]+#$ENV{IMAGE_REPO}/preprocessing-service:$ENV{IMAGE_TAG}#g" \
  k8s/preprocess-deployment.yaml

perl -0pi -e "s#asia-northeast3-docker\.pkg\.dev/[^\\s]+/log-generator:[^\\s]+#$ENV{IMAGE_REPO}/log-generator:$ENV{IMAGE_TAG}#g" \
  k8s/log-generator-deployment.yaml
```

변경 결과를 확인한다.

```bash
grep -R "image:" -n k8s/kserve/inferenceservice.yaml k8s/preprocess-deployment.yaml k8s/log-generator-deployment.yaml
```

ArgoCD는 [k8s/argocd/argocd-app.yaml](k8s/argocd/argocd-app.yaml)의 `repoURL`에 있는 GitHub 저장소를 읽는다. 따라서 GitOps 방식으로 배포하려면 이미지 태그가 반영된 매니페스트를 먼저 원격 브랜치에 push해야 한다.

```bash
git status --short
git add k8s docker src terraform GKE_DEPLOYMENT_RUNBOOK.md
git commit -m "Prepare GKE deployment manifests"
git push
```

원격에 push하지 않고 현재 로컬 파일로만 테스트하려면 ArgoCD 대신 `kubectl apply -f ...` 방식으로 직접 적용해야 한다. 이 문서의 기본 경로는 ArgoCD/GitOps 배포다.

## 6. Operator와 GitOps 배포

현재 bootstrap 스크립트는 다음 순서로 설치한다.

1. `argocd`, `deeplog` namespace 생성
2. ArgoCD 설치
3. Strimzi Kafka Operator 설치
4. KServe serverless stack 설치
5. Prometheus Operator stack 설치
6. ArgoCD Application 배포

실행한다.

```bash
chmod +x k8s/argocd/install.sh
./k8s/argocd/install.sh
```

설치 상태를 확인한다.

```bash
kubectl get pods -n argocd
kubectl get pods -n deeplog
kubectl get pods -n kserve
kubectl get pods -n knative-serving
kubectl get pods -n istio-system
kubectl get pods -n monitoring
```

ArgoCD Application 상태를 확인한다.

```bash
kubectl get application deeplog-pipeline -n argocd
kubectl describe application deeplog-pipeline -n argocd
```

## 7. Kafka 확인

Kafka cluster와 topic이 준비됐는지 확인한다.

```bash
kubectl get kafka,kafkatopic -n deeplog
kubectl get pods -n deeplog -l strimzi.io/cluster=kafka-cluster
```

Kafka bootstrap service가 있어야 한다.

```bash
kubectl get svc kafka-cluster-kafka-bootstrap -n deeplog
```

## 8. KServe 확인

InferenceService 상태를 확인한다.

```bash
kubectl get inferenceservice deeplog-serving -n deeplog
kubectl describe inferenceservice deeplog-serving -n deeplog
```

predictor pod와 revision 상태를 확인한다.

```bash
kubectl get pods -n deeplog
kubectl get ksvc -n deeplog
```

KServe가 GCS 모델을 못 읽으면 storage initializer 또는 predictor pod 로그에 권한 오류가 나온다.

```bash
kubectl logs -n deeplog -l serving.kserve.io/inferenceservice=deeplog-serving --all-containers=true --tail=200
```

## 9. 전처리 서비스 확인

전처리 서비스가 Kafka와 KServe endpoint를 바라보는지 확인한다.

```bash
kubectl get deploy,svc -n deeplog
kubectl logs -n deeplog deployment/preprocessing-service --tail=200
```

정상 로그의 핵심 문구:

```text
Loaded vocabulary mapping
Loaded Drain3 TemplateMiner
Connected to Kafka. Consuming from 'hdfs-raw-logs'
```

## 10. Mock 로그 생성 시작

로그 생성기 API는 ClusterIP이므로 로컬에서 port-forward로 접근한다.

```bash
kubectl port-forward -n deeplog svc/log-generator-service 8000:8000
```

다른 터미널에서 세 generator를 시작한다.

```bash
curl -X POST http://localhost:8000/api/v1/generator/namesystem/start \
  -H "Content-Type: application/json" \
  -d '{"delay": 0.05}'

curl -X POST http://localhost:8000/api/v1/generator/blockreceiver/start \
  -H "Content-Type: application/json" \
  -d '{"delay": 0.05}'

curl -X POST http://localhost:8000/api/v1/generator/packetresponder/start \
  -H "Content-Type: application/json" \
  -d '{"delay": 0.05}'
```

상태를 확인한다.

```bash
curl http://localhost:8000/api/v1/generator/status
```

전처리 로그에서 처리량과 KServe 호출 오류 여부를 본다.

```bash
kubectl logs -n deeplog deployment/preprocessing-service -f
```

## 11. Prometheus와 Grafana 확인

전처리 서비스의 Prometheus metric endpoint를 직접 확인한다.

```bash
kubectl port-forward -n deeplog svc/preprocessing-service 8080:8080
curl http://localhost:8080/metrics | grep deeplog
```

Grafana에 접속한다.

```bash
kubectl port-forward svc/prometheus-stack-grafana -n monitoring 3000:80
```

브라우저에서 `http://localhost:3000`에 접속한다.

기본 계정:

```text
admin / prom-operator
```

대시보드는 [k8s/monitoring/grafana-dashboard.json](k8s/monitoring/grafana-dashboard.json)을 Import한다.

## 12. Alertmanager 이메일 설정

이메일 알림이 필요하면 [k8s/monitoring/alertmanager-values.yaml](k8s/monitoring/alertmanager-values.yaml)의 SMTP 값을 실제 값으로 수정한 뒤 Helm upgrade를 수행한다.

```bash
helm upgrade prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --wait \
  --timeout 10m \
  -f k8s/monitoring/alertmanager-values.yaml
```

## 13. 빠른 상태 점검 명령

전체 리소스:

```bash
kubectl get all -n deeplog
```

Kafka:

```bash
kubectl get kafka,kafkatopic -n deeplog
```

KServe:

```bash
kubectl get inferenceservice,ksvc -n deeplog
```

ArgoCD:

```bash
kubectl get application -n argocd
```

최근 이벤트:

```bash
kubectl get events -n deeplog --sort-by=.lastTimestamp | tail -50
```

## 14. 자주 막히는 지점

### ImagePullBackOff

증상:

```text
Failed to pull image
```

확인:

```bash
kubectl describe pod -n deeplog <pod-name>
grep -R "image:" -n k8s
```

해결:

```bash
gcloud auth configure-docker "$GCP_REGION-docker.pkg.dev"
docker push "$IMAGE_REPO/<image-name>:$IMAGE_TAG"
```

### KServe가 모델을 못 읽음

증상:

```text
403 Forbidden
storage.objects.get denied
```

확인:

```bash
kubectl logs -n deeplog -l serving.kserve.io/inferenceservice=deeplog-serving --all-containers=true --tail=200
grep -n "iam.gke.io/gcp-service-account" k8s/kserve/serviceaccount.yaml
```

해결:

```bash
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:$MODEL_READER_GSA" \
  --role="roles/storage.objectViewer"
```

### Kafka CRD 또는 ServiceMonitor CRD 오류

증상:

```text
no matches for kind "Kafka"
no matches for kind "ServiceMonitor"
```

해결:

```bash
./k8s/argocd/install.sh
kubectl get crd | grep -E "kafka|servicemonitor|prometheusrule|inferenceservice"
```

### 전처리 서비스가 KServe 호출 실패

확인:

```bash
kubectl logs -n deeplog deployment/preprocessing-service --tail=200
kubectl get svc -n deeplog | grep deeplog-serving
```

현재 전처리 서비스는 다음 endpoint를 사용한다.

```bash
grep -n "KSERVE_ENDPOINT" k8s/preprocess-deployment.yaml
```

실제 생성된 KServe service 이름이 다르면 `KSERVE_ENDPOINT`를 맞춰 수정하고 다시 sync한다.

```bash
kubectl rollout restart deployment/preprocessing-service -n deeplog
```

## 15. 종료와 정리

테스트 후 로그 생성을 멈춘다.

```bash
curl -X POST http://localhost:8000/api/v1/generator/namesystem/stop
curl -X POST http://localhost:8000/api/v1/generator/blockreceiver/stop
curl -X POST http://localhost:8000/api/v1/generator/packetresponder/stop
```

전체 GCP 리소스를 삭제하려면 Terraform으로 삭제한다.

```bash
cd terraform
terraform destroy \
  -var="gcp_project_id=$GCP_PROJECT_ID" \
  -var="gcp_region=$GCP_REGION"
cd ..
```

주의: GCS bucket은 `force_destroy = true`이므로 모델 아티팩트도 함께 삭제된다.
