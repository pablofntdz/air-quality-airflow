from airflow.models import DagBag

def test_dags_load_without_errors():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG errors: {dagbag.import_errors}"

def test_dag_ids_exist():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "download_pipeline" in dagbag.dags
    assert "ingestion_pipeline" in dagbag.dags
    assert "inference_pipeline" in dagbag.dags