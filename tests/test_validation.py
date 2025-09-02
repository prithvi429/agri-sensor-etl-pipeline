# test_validation.py
import unittest
from src.validation import validate_data

class TestValidation(unittest.TestCase):
    def test_validate_data(self):
        # TODO: Add test cases for validate_data
        self.assertIsNone(validate_data(None))

if __name__ == "__main__":
    unittest.main()
