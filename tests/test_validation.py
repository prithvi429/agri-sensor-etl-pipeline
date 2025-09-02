import unittest
import pandas as pd
import os
from validation import validate_data

class TestValidation(unittest.TestCase):
    def setUp(self):
        # Sample data with:
        # - valid and invalid values
        # - missing values
        # - anomalous readings
        self.df = pd.DataFrame({
            "sensor_id": ["s1", "s1", "s2", "s2", "s3"],
            "timestamp": [
                "2023-06-01T00:00:00+0530",
                "2023-06-01T01:00:00+0530",
                "2023-06-01T00:00:00+0530",
                "2023-06-01T02:00:00+0530",
                None  # missing timestamp
            ],
            "reading_type": ["temperature", "temperature", "humidity", "humidity", "temperature"],
            "value": [20.0, 100.0, 50.0, None, -20.0],  # 100.0 and -20.0 are anomalous
            "battery_level": [90, 90, 80, 80, 70],
            "anomalous_reading": [False, True, False, False, True]
        })

        self.report_path = "tests/test_data_quality_report.csv"

    def test_validate_data_creates_report(self):
        # Run validation
        validate_data(self.df, output_path=self.report_path)

        # Check report file created
        self.assertTrue(os.path.exists(self.report_path))

        # Optionally, read report and check contents
        with open(self.report_path, "r") as f:
            content = f.read()
            self.assertIn("Type Check:", content)
            self.assertIn("Range Check:", content)
            self.assertIn("Gaps:", content)
            self.assertIn("Profile:", content)

    def tearDown(self):
        # Clean up report file
        if os.path.exists(self.report_path):
            os.remove(self.report_path)

if __name__ == "__main__":
    unittest.main()