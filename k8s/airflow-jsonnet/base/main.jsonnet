local k = import 'github.com/jsonnet-libs/k8s-libsonnet/1.28/main.libsonnet';
local ns = 'airflow-sandbox';

local labels(app) = { app: app };
local nsObj = k.core.v1.namespace.new(ns);

local env(name, value) = { name: name, value: value };

local dep(name, image, args=[], ports=[], env=[], selector=null) =
  k.apps.v1.deployment.new(name, 1, if selector != null then selector else { app: name }) +
  k.apps.v1.deployment.mixin.metadata.withNamespace(ns) +
  k.apps.v1.deployment.mixin.spec.selector.withMatchLabels(if selector != null then selector else { app: name }) +
  k.apps.v1.deployment.mixin.spec.template.metadata.withLabels(if selector != null then selector else { app: name }) +
  k.apps.v1.deployment.mixin.spec.template.spec.withContainers([{
    name: name, image: image, args: args, ports: ports, env: env
  }]);

// Demo backing stores (swap to managed services in prod)
local postgres = dep('postgres', 'postgres:14',
  [], [{ containerPort: 5432 }],
  [
    env('POSTGRES_USER', 'airflow'),
    env('POSTGRES_PASSWORD', 'airflow'),
    env('POSTGRES_DB', 'airflow'),
  ],
  labels('postgres')
);

// Airflow env: StatsD → observability, OTLP traces → Tempo in observability
local baseEnv = [
  env('AIRFLOW__CORE__EXECUTOR', 'LocalExecutor'),
  env('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'postgresql+psycopg2://airflow:airflow@postgres/airflow'),
  env('AIRFLOW__CELERY__RESULT_BACKEND', 'db+postgresql://airflow:airflow@postgres/airflow'),
  env('AIRFLOW__WEBSERVER__EXPOSE_CONFIG', 'True'),
  env('AIRFLOW__METRICS__STATSD_ON', 'True'),
  env('AIRFLOW__METRICS__STATSD_PREFIX', 'airflow'),
  env('AIRFLOW__METRICS__STATSD_HOST', 'statsd-exporter.observability.svc'),
  env('AIRFLOW__METRICS__STATSD_PORT', '8125'),
  env('OTEL_TRACES_EXPORTER', 'otlp'),
  env('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', 'http://tempo.observability.svc:4317'),
  env('OTEL_SERVICE_NAME', 'airflow'),
];

// NOTE: extend your image to include statsd + OTEL libs; here we assume it's done.
// asia-southeast2-docker.pkg.dev/data-298904/dataeng-images/airflow
local imgAirflow = 'asia-southeast2-docker.pkg.dev/data-298904/dataeng-images/airflow';

local web = dep('airflow-webserver', imgAirflow, ['webserver'], [{ containerPort: 8080 }], baseEnv, labels('airflow'));
local sch = dep('airflow-scheduler', imgAirflow, ['scheduler'], [], baseEnv, labels('airflow'));
local trg = dep('airflow-triggerer', imgAirflow, ['triggerer'], [], baseEnv, labels('airflow'));

local svc(name, ports, selector) =
  k.core.v1.service.new(name, ports[0].port, ports[0].targetPort) +
  k.core.v1.service.mixin.metadata.withNamespace(ns) +
  k.core.v1.service.mixin.spec.withSelector(selector) +
  k.core.v1.service.mixin.spec.withPorts(ports);

{
  "namespace.yaml": nsObj,
  "postgres-deployment.yaml": postgres,
  "postgres-service.yaml": svc('postgres', [{ port: 5432, targetPort: 5432 }], labels('postgres')),
  "airflow-webserver-deployment.yaml": web,
  "airflow-webserver-service.yaml": svc('airflow-web', [{ port: 8080, targetPort: 8080 }], labels('airflow-webserver')),
  "airflow-scheduler-deployment.yaml": sch,
  "airflow-triggerer-deployment.yaml": trg,
}
