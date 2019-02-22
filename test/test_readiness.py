import os
from contextlib import suppress
from unittest import TestCase

from app.readiness import Readiness


class ReadinessTestCase(TestCase):

    def setUp(self):
        self.test_readiness_file_path = os.path.join(os.getcwd(), 'test-readiness-file')

        # Ensure existing readiness file won't interfere
        with suppress(FileNotFoundError):
            os.remove(self.test_readiness_file_path)

    def tearDown(self):
        # Ensure readiness file is cleaned up
        with suppress(FileNotFoundError):
            os.remove(self.test_readiness_file_path)

    def test_readiness_file_is_present_while_in_context(self):
        with Readiness(self.test_readiness_file_path):
            assert os.path.isfile(self.test_readiness_file_path), 'Readiness file not found within readiness context'

    def test_readiness_file_is_not_present_after_exiting_context(self):
        with Readiness(self.test_readiness_file_path):
            pass
        assert not os.path.isfile(self.test_readiness_file_path), 'Readiness file was still present after exiting context'

    def test_readiness_file_is_not_present_after_uncaught_exception_in_context(self):
        with self.assertRaises(Exception):
            with Readiness(self.test_readiness_file_path):
                raise Exception
        assert not os.path.isfile(self.test_readiness_file_path), 'Readiness file was still present after uncaught exception in context'
