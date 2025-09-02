import logging

import pytest

from aqm_eval.aqm_mm_eval.driver.interface import SRWInterface
from aqm_eval.logging_aqm_eval import LOGGER
from test.test_aqm_mm_eval.test_driver.conftest import srw_interface


class TestSRWInterface:
    def test_init_path_happy(self, srw_interface: SRWInterface) -> None:
        LOGGER(srw_interface, level=logging.DEBUG)
        assert srw_interface.date_first_cycle_mm == '2023-06-01-12:00:00'
        assert srw_interface.link_simulation == ('2023*',)

    def test_find_nested_key_happy_second_yaml(self, srw_interface: SRWInterface) -> None:
        actual = srw_interface.find_nested_key(("foo2", "second"))
        assert actual == "baz"

    def test_find_nested_key_sad(self, srw_interface: SRWInterface) -> None:
        with pytest.raises(KeyError):
            srw_interface.find_nested_key(("fail", "badly"))

    def test_find_nested_key_sad_no_child(self, srw_interface: SRWInterface) -> None:
        with pytest.raises(TypeError):
            srw_interface.find_nested_key(("foo", "bar"))


