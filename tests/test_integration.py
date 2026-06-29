import subprocess
import time
import json
import urllib.request
import pytest


COMPOSE_FILE = "docker-compose.yml"
COMPOSE_PROJECT = "cdc-test"


@pytest.fixture(scope="module")
def docker_services():
    subprocess.run([
        "docker", "compose", "-p", COMPOSE_PROJECT,
        "-f", COMPOSE_FILE, "up", "-d",
    ], check=True)
    time.sleep(10)
    yield
    subprocess.run([
        "docker", "compose", "-p", COMPOSE_PROJECT,
        "-f", COMPOSE_FILE, "down", "-v",
    ], check=True)


def _wait_for_health(container_name, timeout=120):
    for i in range(timeout):
        result = subprocess.run(
            ["docker", "compose", "-p", COMPOSE_PROJECT,
             "exec", container_name, "sh", "-c", "echo ok"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            return True
        time.sleep(1)
    return False


def test_postgres_healthy(docker_services):
    assert _wait_for_health("postgres-source")


def test_kafka_healthy(docker_services):
    assert _wait_for_health("kafka")


def test_kafka_connect_healthy(docker_services):
    assert _wait_for_health("connect")


def test_minio_healthy(docker_services):
    assert _wait_for_health("minio")


def test_nessie_healthy(docker_services):
    assert _wait_for_health("nessie")


def test_trino_running(docker_services):
    for i in range(30):
        try:
            req = urllib.request.Request("http://localhost:8081/v1/info")
            resp = urllib.request.urlopen(req)
            info = json.loads(resp.read())
            if info.get("starting") is False:
                assert True
                return
        except Exception:
            pass
        time.sleep(2)
    pytest.fail("Trino not ready after 60s")


def test_kafka_connect_has_connector(docker_services):
    time.sleep(5)
    for i in range(30):
        try:
            req = urllib.request.Request("http://localhost:8083/connectors")
            resp = urllib.request.urlopen(req)
            connectors = json.loads(resp.read())
            if "postgres-cdc-connector" in connectors:
                assert True
                return
        except Exception:
            pass
        time.sleep(2)
    pytest.fail("Debezium connector not registered after 60s")


def test_spark_master_running(docker_services):
    for i in range(30):
        try:
            req = urllib.request.Request("http://localhost:4040")
            resp = urllib.request.urlopen(req)
            if resp.status == 200:
                assert True
                return
        except Exception:
            pass
        time.sleep(2)
    pytest.fail("Spark master not accessible after 60s")


def test_grafana_running(docker_services):
    for i in range(30):
        try:
            req = urllib.request.Request("http://localhost:3001/api/health")
            resp = urllib.request.urlopen(req)
            if resp.status == 200:
                assert True
                return
        except Exception:
            pass
        time.sleep(2)
    pytest.fail("Grafana not ready after 60s")
