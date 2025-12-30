{{- define "iceberg-processor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "iceberg-processor.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "iceberg-processor.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "iceberg-processor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "iceberg-processor.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "iceberg-processor.configMapName" -}}
{{- if .Values.config.existingConfigMap -}}
{{- .Values.config.existingConfigMap -}}
{{- else -}}
{{- include "iceberg-processor.fullname" . -}}
{{- end -}}
{{- end -}}
