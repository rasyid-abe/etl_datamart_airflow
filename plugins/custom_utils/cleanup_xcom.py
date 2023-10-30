from airflow.utils.db import provide_session

@provide_session
def cleanup_xcom(context, session=None):
    from airflow.models.xcom import XCom

    dag = context["dag"]
    dag_run = context["run_id"]
    print("Delete XCom...")
    print(f"dag id : {dag._dag_id}")
    print(f"run id : {dag_run}")    

    session.query(XCom).filter(XCom.dag_id == dag._dag_id,  XCom.run_id == dag_run).delete()