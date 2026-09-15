# Observability & Monitoring

This document details the monitoring stack, Prometheus metrics pipeline, Grafana dashboards, and Alertmanager configurations.

![Grafana Dashboard](Grafana%20dashboard.png)

---

## Monitoring Architecture

```
Spark Streaming / Inference Engine
       │
       ▼
MinIO Monitoring Metrics (s3a://monitoring/ml-inference)
       │
       ▼
Metrics Pusher Service (apps/monitoring/pusher.py)
       │
       ▼
Prometheus Pushgateway (port: 9091)
       │
       ▼
Prometheus 3.5.0 (port: 9090) ◄── Kafka Exporter / cAdvisor / Node Exporter
       │
   ┌───┴───────────────────────┐
   ▼                           ▼
Grafana 12.1.1 (port: 3000)  Alertmanager (port: 9093) ──► Slack Webhook
```

---

## 1. Metrics Collected

### ML & Business Metrics (Pushed from Inference)
- `fraud_pipeline_input_records_total`: Total streaming transactions scored.
- `fraud_pipeline_predictions_total`: Total model predictions emitted.
- `fraud_pipeline_fraud_detected_total`: Total positive fraud classifications (`P >= 0.99`).
- `fraud_pipeline_fraud_rate`: Calculated fraud percentage over rolling windows.
- `fraud_pipeline_inference_duration_seconds`: Micro-batch scoring latency.

### Infrastructure & Pipeline Metrics
- `kafka_consumergroup_lag`: Real-time consumer lag across the `transactions` topic.
- `container_cpu_usage_seconds_total`: Resource usage per Docker container (cAdvisor).
- `node_memory_Active_bytes`: System RAM consumption (Node Exporter).

---

## 2. Grafana Dashboards

The platform provisions two pre-built dashboards out of the box:

### 1. Fraud Detection Overview (`fraud_detection_overview.json`)
- **Real-Time Transaction Volume**: Visualizes transactions processed per second.
- **Fraud Rate Meter**: Gauge tracking anomalous spikes above baseline fraud rates (normal ~0.17%).
- **Fraud Alerts Timeline**: Incident markers showing high-dollar transactions flagged as fraud.
- **Inference Latency Heatmap**: Micro-batch processing duration to ensure sub-second performance.

### 2. Infrastructure & Container Health (`infrastructure.json`)
- Spark Master and Worker CPU/Memory utilization.
- Kafka broker message throughput and partition offsets.
- MinIO S3 operations per second and storage volume.

---

## 3. Alertmanager & Slack Notifications

Prometheus evaluates alert rules defined in `monitoring/prometheus/alerts.yml`:
- **HighFraudRateAlert**: Triggers if rolling fraud rate exceeds `2.0%` over 5 minutes (indicating coordinated attacks or data drift).
- **HighConsumerLag**: Triggers if Kafka lag exceeds 10,000 records (Spark streaming backpressure).
- **ModelInferenceFailure**: Triggers if the inference container encounters unhandled exceptions.

When an alert triggers, Alertmanager formats a card with alert severity, summary, and links, sending it to the configured Slack webhook.
