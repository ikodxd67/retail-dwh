"""Проверка DAG-ов. Запускается там, где установлен Airflow — в CI внутри того
же образа, что и в docker compose (см. .github/workflows/ci.yml)."""

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag

from retail_dwh.ingest.config import load_pipelines
from retail_dwh.settings import PROJECT_DIR


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(PROJECT_DIR / "dags"))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_factory_builds_dag_per_pipeline(dagbag):
    for name in load_pipelines():
        assert f"ingest__{name}" in dagbag.dags


def test_file_pipelines_wait_for_files(dagbag):
    dag = dagbag.dags["ingest__catalog_products"]
    sensor = dag.get_task("wait_for_files")
    assert sensor.mode == "reschedule", "сенсор не должен держать слот исполнителя"
    assert "extract_load_check" in sensor.downstream_task_ids


def test_every_dag_has_owner_and_no_catchup(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert not dag.catchup, dag_id
        for task in dag.tasks:
            assert task.owner and task.owner != "airflow", f"{dag_id}.{task.task_id}"


def test_dwh_build_order(dagbag):
    dag = dagbag.dags["dwh_build"]
    assert dag.get_task("vault_load").downstream_task_ids == {"build_marts"}
    assert dag.get_task("build_marts").downstream_task_ids == {"dq_marts"}
