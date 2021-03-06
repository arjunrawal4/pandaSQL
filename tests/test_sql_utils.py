import unittest
import os
from tempfile import NamedTemporaryFile
import pandasql as ps


class TestSQLUtils(unittest.TestCase):

    def test_get_and_set_database_file(self):
        old_file = ps.get_database_file()
        self.assertTrue(os.path.exists(old_file))

        new_file = NamedTemporaryFile().name
        ps.set_database_file(new_file, delete=True)
        self.assertFalse(os.path.exists(old_file))
        self.assertTrue(os.path.exists(new_file))

        self.assertEqual(os.path.getsize(new_file), 0)
        _ = ps.DataFrame([{'n': i, 's': str(i*2)} for i in range(10)])
        self.assertGreater(os.path.getsize(new_file), 0)
