import sys, os, time

try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys._getframe(0).f_code.co_filename))
sys.path.insert(0, os.path.join(_script_dir, ".."))

from config import pipeline_name
from src.session import get_spark_and_client

def main():
    spark, w = get_spark_and_client()
    
    p = next((p for p in w.pipelines.list_pipelines() if p.name == pipeline_name), None)

    if p is None:
        raise RuntimeError(f"Pipeline {pipeline_name} not found")

    update = w.pipelines.start_update(
        pipeline_id=p.pipeline_id
    )

    while True:
        info = w.pipelines.get_update(p.pipeline_id, update.update_id)
        state = info.update.state.value

        print(f" state = {state}")

        if state in ("COMPLETED","FAILED","CANCELED"):
            break

        time.sleep(15)

    if state != "COMPLETED":
        raise RuntimeError(f"Pipeline {pipeline_name} failed with state {state}")

    print(f"Pipeline {pipeline_name} completed with state {state}")

if __name__ == "__main__":
    main()