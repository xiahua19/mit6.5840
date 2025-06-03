curl "http://localhost:8080/get?key=test_key"
curl "http://localhost:8080/put?key=test_key&value=test_value"
curl "http://localhost:8080/delete?key=test_key"

./shard_layer -port=6666 -shard-count=2 -shard-urls="http://server1:8080","http://server2:8080"
./k8s_server -port=8080

# open docker desktop
docker build -t xiahuaforjob/k8s_server:v0.0.1 -f Dockerfile.k8s_server .
docker push xiahuaforjob/k8s_server:v0.0.1
docker build -t xiahuaforjob/shard_layer:v0.0.1 -f Dockerfile.shard_layer .
docker push xiahuaforjob/shard_layer:v0.0.1

# minikube start --vm-driver docker --container-runtime=docker
kubectl apply -f deployment.yaml
kubectl get deployments
kubectl get pods
kubectl get services
kubectl delete deployment --all
kubectl delete pods --all
kubectl delete services --all
kubectl delete statefulset --all
kubectl port-forward service/shard-layer 8080:80
kubectl exec -it shard-layer-75fdddc5b6-2q8wd -- nslookup k8s-server-2