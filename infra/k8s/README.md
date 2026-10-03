# Kubernetes deployment

These manifests deploy the complete single-node demonstration topology with persistent volumes. Kubernetes is optional. A default StorageClass is required. The backend deliberately runs one replica because the in-process simulator and WebSocket hub have one owner; independent consumer groups already separate persistence from live projection. Do not scale the backend until simulation ownership and cross-replica fanout are implemented.

```sh
docker build -t fleet-operations-backend:local backend
docker build -t fleet-operations-frontend:local frontend
# For kind:
kind load docker-image fleet-operations-backend:local fleet-operations-frontend:local
kubectl apply -f infra/k8s/platform.yaml
kubectl -n fleet-ops rollout status deployment/backend
kubectl -n fleet-ops port-forward service/frontend 5173:80
# In separate terminals for observability:
kubectl -n fleet-ops port-forward service/grafana 3000:3000
kubectl -n fleet-ops port-forward service/prometheus 9090:9090
```

For other clusters, push the two application images to your registry and update their image references. Replace example Secret values before deployment. NetworkPolicy, TLS ingress, identity/RBAC, managed backups, multi-node Redpanda replication and HA database configuration are production deployment work; the shipped manifests are reproducible deployment manifests for a private demonstration environment, not a public multi-tenant production installation.
