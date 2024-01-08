# Visions

## Figuring where to send requests

Suppose you're a batch manager A. How do you know whether any task managers are
online and where should the doTask request be sent?

* Locally, for testing purposes, we can assume static configuration
* On cloud, we can use Envoy Proxy:
  https://cloud.google.com/kubernetes-engine/docs/tutorials/exposing-grpc-services-on-gke-using-envoy-proxy
 
  or do it via Cloud Endpoints (seems even better):
  https://cloud.google.com/endpoints/docs/grpc/get-started-kubernetes-engine

## Cloud architecture

* GKE
* clusters (groups of masters, managers, workers etc.)
* we can use health check service - no need to create hierarchical gRPC HC, just network ping handlers
  https://cloud.google.com/blog/products/containers-kubernetes/kubernetes-best-practices-setting-up-health-checks-with-readiness-and-liveness-probes
* Storage done via Filestore
