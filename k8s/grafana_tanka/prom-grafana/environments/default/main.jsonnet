local grafana(name, port) = {
  deployment: {
    apiVersion: 'apps/v1',
    kind: 'Deployment',
    metadata: {
      name: name,
    },
    spec: {
      selector: {
        matchLabels: {
          name: name,
        },
      },
      template: {
        metadata: {
          labels: {
            name: name,
          },
        },
        spec: {
          containers: [
            {
              image: 'grafana/grafana',
              name: name,
              ports: [{
                  containerPort: port,
                  name: 'ui',
              }],
            },
          ],
        },
      },
    },
  },
  service: {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      labels: {
        name: name,
      },
      name: name,
    },
    spec: {
      ports: [{
          name: 'grafana-ui',
          port: 3000,
          targetPort: port,
      }],
      selector: {
        name: name,
      },
      type: 'NodePort',
    },
  },
};

{
  namespace: {
    apiVersion: "v1",
    kind: "Namespace",
    metadata: {
      name: "monitoring"
    }
  },
  
  grafana: grafana(name='grafana',port=3000),
  prometheus: {
    deployment: {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: {
        name: 'prometheus',
      },
      spec: {
        minReadySeconds: 10,
        replicas: 1,
        revisionHistoryLimit: 10,
        selector: {
          matchLabels: {
            name: 'prometheus',
          },
        },
        template: {
          metadata: {
            labels: {
              name: 'prometheus',
            },
          },
          spec: {
            containers: [
              {
                image: 'prom/prometheus',
                imagePullPolicy: 'IfNotPresent',
                name: 'prometheus',
                ports: [
                  {
                    containerPort: 9090,
                    name: 'api',
                  },
                ],
              },
            ],
          },
        },
      },
    },
    service: {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        labels: {
          name: 'prometheus',
        },
        name: 'prometheus',
      },
      spec: {
        ports: [
          {
            name: 'prometheus-api',
            port: 9090,
            targetPort: 9090,
          },
        ],
        selector: {
          name: 'prometheus',
        },
      },
    },
  },
}
