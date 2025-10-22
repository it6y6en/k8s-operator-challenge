## Solution

#### 1. Simple HTTP server runs on Flask (Python). Gunicorn is used to run the instance (workers limited to 1 in our scenario).
- Server responds to /ping, /config (shows config), /reload (can manually trigger config reload, but not used in the current k8s scenario)
- Designed to run as a StatefulSet and communicate with other pods using a headless service
- Reads config from a file mounted to /var/server/config.yaml
- k8s ConfigMap is mounted as a volume to the config.yaml
- Config has 2 parameters: replicas, timer
- Runs a loop every 'timer' seconds, refreshes config each time, and tries to ping (send requests) to other pods
- Can be tested locally with a docker compose file
```
cd pingpong-server
docker compose up
```
- Image is built and pushed to public repo `https://hub.docker.com/r/6y6en/pingpong-server`

#### 2. CRD
- CRD is deployed with the operator in the same helm chart
- Has 3 parameters: replicas, timer, image
- Example of CR instance in `pingpong-crd-example/sample.yaml`

#### 3. Operator
- Simple operator written in Python using kopf.
- It watches create/update/delete of pingpong CR
- Creates/updates ConfigMap with parameters for servers in the cluster (Pods in StatefulSet)
- If changes happen, the operator only updates the StatefulSet definition and ConfigMap data
- Image built and pushed to `https://hub.docker.com/r/6y6en/pingpong-operator`

#### 4. Communication
- Pods communicate using a headless service. Each pod sends ping (request) to other pods based on the replicas number
- Servers write logs to stdout

#### 5. Changes/Updates
- Changes in CR are caught by the operator
- Rollout happens only if the image is updated
- If number of replicas is updated, the operator updates StatefulSet spec and ConfigMap data
- Servers (pods) periodically read config from the file (mounted from ConfigMap)
- Once kubelet updates content in volume inside the pod, the pod gets new config and pings desired number of other servers in the cluster each 'timer' seconds
- Some delay in config syncing may occur (kubelet updates ConfigMap volumes periodically)
- No need to restart pods for scaling

#### 6. Tests
- See instruction to run tests below

### Bonus
- Pods are not restarted. Pods load new config through mounted volume from ConfigMap managed by the operator
- Docker 
  * Slim images 
  * Debugging tools installed on purpose, to look around and curl/ping if needed from inside the pod
- K8s 
  * All resources are namespace-scoped 
  * Operator runs with service account and minimum permissions defined in roles/clusterroles 
  * All pods have resource limits
  * Helm chart to deploy operator

## Run test
clone repo or unzip:

```
git clone https://github.com/it6y6en/k8s-operator-challenge.git
cd k8s-operator-challenge
```

install operator with helm
```
helm install release pingpong-operator-chart --namespace alfa --create-namespace
```

add CR PingPong sample instanse
```
kubectl -n alfa apply -f pingpong-crd-example/sample.yaml
```

check deployed pods
```
kubectl -n alfa get pods
```

check servers communication logs:
```
kubectl -n alfa logs cluster-sample-sts-0 -f
```

### Update / Change
- monitor operator actions: `kubectl -n alfa logs release-pingpong-operator-chart-XXX-XXX -f`
- update sample CR definision in sample.yaml file (change replicas, timer, image) or apply `sample_updated.yaml`
- apply changes `kubectl -n alfa apply -f pingpong-crd-example/sample_updated.yaml`
- if image updated - statefull set rollouts new pods
- replicas changes - statefull set adjusts as per spec
- other parameters (timer) updated - only ConfigMap updates which will be picked up by servers
- check pods: `kubectl -n alfa get pods`
- check CM content: `kubectl -n alfa get cm cluster-sample-cm -o yaml`
- check server's logs: `kubectl -n alfa logs cluster-sample-sts-0 -f`

### CleanUp

```
kubectl -n alfa delete -f pingpong-crd-example/sample.yaml
helm uninstall release -n alfa
```