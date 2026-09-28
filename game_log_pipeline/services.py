"""Networked entry points: ingest API (FastAPI -> Kafka), Kafka loader (-> PostgreSQL), rebuild job.

Third-party packages are imported inside each entry point so the reference pipeline and its tests
keep running on the standard library alone. Configuration comes from environment variables that
the Helm chart sets; secrets arrive through Kubernetes Secrets, never through this repository.
"""
import json
import os
import signal
from datetime import datetime, timezone

from .transport import BatchRejected, partition_key, raw_record, split_batch

TOPIC = os.environ.get("GLP_TOPIC", "game.events.raw")


def kafka_config():
    return {"bootstrap.servers": os.environ.get("GLP_KAFKA_BOOTSTRAP", "localhost:9092")}


def postgres_dsn():
    return os.environ["GLP_POSTGRES_DSN"]


def create_app():
    from confluent_kafka import Producer
    from fastapi import FastAPI, HTTPException, Request

    # Idempotent producer: broker-side dedupe of producer retries, acks from all in-sync replicas.
    from prometheus_client import Counter, make_asgi_app

    producer = Producer({**kafka_config(), "enable.idempotence": True, "acks": "all",
                         "linger.ms": 20, "compression.type": "zstd"})
    app = FastAPI(title="game-log-pipeline ingest", version="0.4.0")
    # Scraped by Prometheus (PodMonitor); counts events, not requests, so batches of any size compare.
    events_total = Counter("glp_ingest_events", "Events handled by the ingest API", ["result"])
    app.mount("/metrics", make_asgi_app())

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz():
        try:
            producer.list_topics(topic=TOPIC, timeout=2)
        except Exception as error:  # broker unreachable -> not ready, keep the pod out of the Service
            raise HTTPException(status_code=503, detail="kafka unavailable: %s" % error)
        return {"status": "ready", "topic": TOPIC}

    @app.post("/v1/events", status_code=202)
    async def events(request: Request):
        try:
            lines = split_batch(await request.body())
        except BatchRejected as error:
            events_total.labels("rejected_batch").inc()
            raise HTTPException(status_code=400, detail=str(error))
        failures = []
        for line in lines:
            producer.produce(TOPIC, value=line.encode("utf-8"), key=partition_key(line),
                             on_delivery=lambda err, _msg: err and failures.append(str(err)))
        remaining = producer.flush(10)
        if remaining or failures:
            # The client retries the whole batch; duplicates are absorbed later by event_id rules.
            events_total.labels("unacknowledged").inc(len(lines))
            raise HTTPException(status_code=503, detail="not all events acknowledged")
        events_total.labels("accepted").inc(len(lines))
        return {"accepted": len(lines), "topic": TOPIC}

    return app


def run_loader(batch_size=500):
    """Consume RAW from Kafka into PostgreSQL. Offsets are committed only after the DB commit."""
    import time

    import psycopg
    from confluent_kafka import Consumer
    from prometheus_client import Counter, Gauge, start_http_server

    from .incremental import keys
    from .warehouse import ensure_landing

    # Consumer lag itself comes from the Kafka exporter; these show what the loader committed.
    rows_total = Counter("glp_loader_rows", "RAW rows committed to PostgreSQL")
    last_commit = Gauge("glp_loader_last_commit_unixtime", "Time of the last DB commit followed by an offset commit")
    start_http_server(int(os.environ.get("GLP_METRICS_PORT", "9100")))

    consumer = Consumer({**kafka_config(), "group.id": os.environ.get("GLP_GROUP", "glp-loader"),
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(True))
    with psycopg.connect(postgres_dsn()) as conn:
        ensure_landing(conn)
        while not stop:
            messages = consumer.consume(batch_size, timeout=1.0)
            rows = [raw_record(m.topic(), m.partition(), m.offset(), m.value().decode("utf-8", "replace"),
                               datetime.now(timezone.utc).isoformat())
                    for m in messages if m.error() is None]
            rows = [row + keys(row[3]) for row in rows]
            if not rows:
                continue
            with conn.cursor() as cursor:
                cursor.executemany("""INSERT INTO raw_events(topic, kafka_partition, kafka_offset, payload, ingested_at,
                                                             event_key, txn_key, keyed)
                                      VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                                      ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING""", rows)
            conn.commit()
            consumer.commit(asynchronous=False)
            rows_total.inc(len(rows))
            last_commit.set(time.time())
            print(json.dumps({"loaded": len(rows)}), flush=True)
    consumer.close()


def snowflake_auth(env):
    """Key-pair auth (Snowflake's recommendation for service users) when a PEM key is given, else password."""
    pem = env.get("GLP_SNOWFLAKE_PRIVATE_KEY")
    if not pem:
        return {"password": env["GLP_SNOWFLAKE_PASSWORD"]}
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(pem.encode("ascii"), password=None)
    return {"private_key": key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                                             serialization.NoEncryption())}


def run_rebuild(mode="incremental"):
    """One-shot job (Airflow KubernetesPodOperator / CronJob): landing PG -> marts in PG (+Snowflake)."""
    import psycopg

    from .warehouse import rebuild_warehouse

    targets = []
    with psycopg.connect(postgres_dsn()) as source:
        targets.append(source)
        snowflake = None
        if os.environ.get("GLP_SNOWFLAKE_ACCOUNT"):
            import snowflake.connector
            snowflake = snowflake.connector.connect(
                account=os.environ["GLP_SNOWFLAKE_ACCOUNT"], user=os.environ["GLP_SNOWFLAKE_USER"],
                warehouse=os.environ["GLP_SNOWFLAKE_WAREHOUSE"], database=os.environ["GLP_SNOWFLAKE_DATABASE"],
                schema=os.environ.get("GLP_SNOWFLAKE_SCHEMA", "PUBLIC"), autocommit=False,
                **snowflake_auth(os.environ))
            targets.append(snowflake)
        try:
            result = rebuild_warehouse(source, targets, mode)
        finally:
            if snowflake:
                snowflake.close()
    print(json.dumps(result), flush=True)
    return result
