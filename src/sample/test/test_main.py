from concurrent_error import main
from uuid import uuid4


def test_main_sample_no_data(tmp_path):
    source = "../sample/test/data/sample-USA.json"
    delta_path = str(tmp_path / str(uuid4()))
    delta_table = main.main_sample(source, delta_path)
    assert (delta_table.toDF() is not None)
