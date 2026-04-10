{{/*
Expand the name of the chart.
*/}}
{{- define "dispatchhub.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "dispatchhub.labels" -}}
helm.sh/chart: {{ include "dispatchhub.name" . }}-{{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: dispatchhub
{{- end }}

{{/*
Selector labels for a component
*/}}
{{- define "dispatchhub.selectorLabels" -}}
app.kubernetes.io/name: dispatchhub
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Image tag
*/}}
{{- define "dispatchhub.imageTag" -}}
{{- default .Chart.AppVersion .tag }}
{{- end }}
