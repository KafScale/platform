{{- define "sql-processor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sql-processor.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "sql-processor.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "sql-processor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "sql-processor.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "sql-processor.configMapName" -}}
{{- if .Values.config.existingConfigMap -}}
{{- .Values.config.existingConfigMap -}}
{{- else -}}
{{- include "sql-processor.fullname" . -}}
{{- end -}}
{{- end -}}

{{- define "sql-processor.executorSelector" -}}
app.kubernetes.io/name: {{ include "sql-processor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: executor
{{- end -}}

{{- define "sql-processor.proxySelector" -}}
app.kubernetes.io/name: {{ include "sql-processor.name" . }}-proxy
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: proxy
{{- end -}}

{{- define "sql-processor.validate" -}}
{{- if and .Values.service.enabled .Values.proxy.enabled -}}
{{- fail "service.enabled and proxy.enabled cannot both be true: the direct executor Service bypasses the proxy ACLs and audit log. Use the proxy Service, or turn the proxy off." -}}
{{- end -}}
{{- end -}}
