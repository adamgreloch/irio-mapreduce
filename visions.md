# Visions

## Figuring where to send requests

Suppose you're a batch manager A. How do you know whether any task managers are
online and where should the doTask request be sent?

* Locally, for testing purposes, we can assume static configuration

## Cloud architecture

* GKE
* clusters/services (groups of masters, managers, workers etc.)
* we can use health check service - no need to create hierarchical gRPC HC, just network ping handlers
  https://cloud.google.com/blog/products/containers-kubernetes/kubernetes-best-practices-setting-up-health-checks-with-readiness-and-liveness-probes
* Storage done via Filestore
  https://cloud.google.com/filestore/docs/filestore-for-gke

### k8s details

* we define services (`workers`, `task-managers` etc) for each process type to
  expose them in the internal network
  https://kubernetes.io/docs/concepts/services-networking/service/

  discovery would be via i.e. `WORKERS_SERVICE_HOST` and `WORKERS_SERVICE_PORT` env vars as shown here:
  https://kubernetes.io/docs/concepts/services-networking/service/#discovering-services