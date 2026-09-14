# CPU and memory metrics

`metrics-server.yaml` vendors the official Metrics Server v0.9.0 release manifest:
https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.0/components.yaml

It runs in `kube-system`, collects CPU/memory every 15 seconds, and exposes
`metrics.k8s.io` for `kubectl top` and Lens. It does not store historical metrics.
The upstream resource requests are 100m CPU and 200Mi memory.

Apply and verify independently of the backend release process:

```sh
kubectl --context data-hunt apply -f k8s/metrics-server.yaml
kubectl --context data-hunt -n kube-system rollout status deployment/metrics-server
kubectl --context data-hunt get apiservice v1beta1.metrics.k8s.io
kubectl --context data-hunt top nodes
kubectl --context data-hunt -n data-hunt top pods
```

In Lens, select **Kubernetes Metrics Server** as the cluster's metrics source
and refresh the Pods view. A completed backup pod has no current CPU/memory usage.

The manifest uses the upstream APIService TLS setting for the dynamically
self-signed Metrics Server serving certificate. Kubelet certificate verification
remains enabled; `--kubelet-insecure-tls` is not configured.

To remove this component:

```sh
kubectl --context data-hunt delete -f k8s/metrics-server.yaml
```

Removing it disables consumers of Metrics API, including `kubectl top`, Lens's
Metrics Server source, and any CPU/memory autoscaling added later.
