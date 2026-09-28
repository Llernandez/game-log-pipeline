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
    producer = Producer({**kafka_config(), "enable.idempotence": True, "acks": "all",
                         "linger.ms": 20, "compression.type": "zstd"})
    app = FastAPI(title="game-log-pipeline ingest", version="0.2.0")

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
            raise HTTPException(status_code=400, detail=str(error))
        failures = []
        for line in lines:
            producer.produce(TOPIC, value=line.encode("utf-8"), key=partition_key(line),
                             on_delivery=lambda err, _msg: err and failures.append(str(err)))
        remaining = producer.flush(10)
        if remaining or failures:
            # The client retries the whole batch; duplicates are absorbed later by event_id rules.
            raise HTTPException(status_code=503, detail="not all events acknowledged")
        return {"accepted": len(lines), "topic": TOPIC}

    return app


def run_loader(batch_size=500):
    """Consume RAW from Kafka into PostgreSQL. Offsets are committed only after the DB commit."""
    import psycopg
    from confluent_kafka import Consumer

    from .warehouse import ensure_landing

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
            if not rows:
                continue
            with conn.cursor() as cursor:
                cursor.executemany("""INSERT INTO raw_events(topic, kafka_partition, kafka_offset, payload, ingested_at)
                                      VALUES (%s, %s, %s, %s, %s)
                                      ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING""", rows)
            conn.commit()
            consumer.commit(asynchronous=False)
            print(json.dumps({"loaded": len(rows)}), flush=True)
    consumer.close()


def run_rebuild():
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
                password=os.environ["GLP_SNOWFLAKE_PASSWORD"], warehouse=os.environ["GLP_SNOWFLAKE_WAREHOUSE"],
                database=os.environ["GLP_SNOWFLAKE_DATABASE"], schema=os.environ.get("GLP_SNOWFLAKE_SCHEMA", "PUBLIC"),
                autocommit=False)
            targets.append(snowflake)
        try:
            result = rebuild_warehouse(source, targets)
        finally:
            if snowflake:
                snowflake.close()
    print(json.dumps(result), flush=True)
    return result
