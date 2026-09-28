{{- define "glp.labels" -}}
app.kubernetes.io/name: game-log-pipeline
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "glp.image" -}}
image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
imagePullPolicy: {{ .Values.image.pullPolicy }}
{{- end }}

{{- define "glp.securityContext" -}}
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
{{- end }}

{{- define "glp.env" -}}
- name: GLP_KAFKA_BOOTSTRAP
  value: {{ .Values.kafka.bootstrap | quote }}
- name: GLP_TOPIC
  value: {{ .Values.kafka.topic | quote }}
{{- end }}

{{- define "glp.postgresEnv" -}}
- name: GLP_POSTGRES_DSN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.secretName }}
      key: {{ .Values.postgres.secretKey }}
{{- end }}

{{- define "glp.snowflakeEnv" -}}
{{- if .Values.snowflake.secretName }}
{{- range $key := list "account" "user" "password" "warehouse" "database" }}
- name: GLP_SNOWFLAKE_{{ upper $key }}
  valueFrom:
    secretKeyRef:
      name: {{ $.Values.snowflake.secretName }}
      key: {{ $key }}
{{- end }}
- name: GLP_SNOWFLAKE_SCHEMA
  valueFrom:
    secretKeyRef:
      name: {{ .Values.snowflake.secretName }}
      key: schema
      optional: true
{{- end }}
{{- end }}
