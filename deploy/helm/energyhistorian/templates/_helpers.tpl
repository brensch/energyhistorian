{{- define "energyhistorian.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "energyhistorian.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "energyhistorian.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "energyhistorian.labels" -}}
app.kubernetes.io/name: {{ include "energyhistorian.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "energyhistorian.selectorLabels" -}}
app.kubernetes.io/name: {{ include "energyhistorian.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "energyhistorian.postgresHost" -}}
{{- printf "%s-postgres" (include "energyhistorian.fullname" .) -}}
{{- end -}}

{{- define "energyhistorian.minioHost" -}}
{{- printf "%s-minio" (include "energyhistorian.fullname" .) -}}
{{- end -}}
