JSONNET_CMD = jsonnet -J vendor -m out/base base/main.jsonnet

BASE_FILES = \
	namespace.yaml \
	postgres-deployment.yaml \
	postgres-service.yaml \
	airflow-webserver-deployment.yaml \
	airflow-webserver-service.yaml \
	airflow-scheduler-deployment.yaml \
	airflow-triggerer-deployment.yaml

build-base:
	mkdir -p out/base
	$(JSONNET_CMD)
	@echo "resources:" > out/base/kustomization.yaml
	@for f in $(BASE_FILES); do \
		echo "  - $$f" >> out/base/kustomization.yaml; \
	done
	@echo "✅ Base manifests & kustomization.yaml generated."

build-dev: build-base
	kustomize build overlays/dev > out/dev.yaml
	@echo "✅ Dev manifest built at out/dev.yaml"

build-prod: build-base
	kustomize build overlays/prod > out/prod.yaml
	@echo "✅ Prod manifest built at out/prod.yaml"

clean:
	rm -rf out
