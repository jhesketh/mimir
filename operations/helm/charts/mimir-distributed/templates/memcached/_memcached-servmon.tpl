{{/*
memcached ServiceMonitor
*/}}
{{- define "mimir.memcached.serviceMonitor" -}}
{{ with (index $.ctx.Values $.component) }}
{{- if .enabled -}}
{{- with $.ctx.Values.serviceMonitor }}
{{- if .enabled }}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "mimir.resourceName" (dict "ctx" $.ctx "component" $.component) }}
  {{- with .namespace }}
  namespace: {{ . }}
  {{- end }}
  labels:
    {{- include "mimir.labels" (dict "ctx" $.ctx "component" $.component) | nindent 4 }}
    {{- with .labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  {{- with .namespaceSelector }}
  namespaceSelector:
  {{- toYaml . | nindent 4 }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "mimir.selectorLabels" (dict "ctx" $.ctx "component" $.component) | nindent 6 }}
    matchExpressions:
      - key: prometheus.io/service-monitor
        operator: NotIn
        values:
          - "false"
  endpoints:
    - port: http-metrics
      {{- with .interval }}
      interval: {{ . }}
      {{- end }}
      {{- with .scrapeTimeout }}
      scrapeTimeout: {{ . }}
      {{- end }}
      relabelings:
        - sourceLabels: [job]
          replacement: "{{ $.Release.Namespace }}/{{ $.component }}"
          targetLabel: job
        - replacement: "{{ include "mimir.clusterName" $.ctx }}"
          targetLabel: cluster
        {{- with .relabelings }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- with .scheme }}
      scheme: {{ . }}
      {{- end }}
      {{- with .tlsConfig }}
      tlsConfig:
        {{- toYaml . | nindent 8 }}
      {{- end }}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
