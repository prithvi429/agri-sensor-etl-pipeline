# test_transformation.py
import unittest
from src.transformation import transform_data

class TestTransformation(unittest.TestCase):
    def test_transform_data(self):
        # TODO: Add test cases for transform_data
        self.assertIsNone(transform_data(None))

if __name__ == "__main__":
    unittest.main()
