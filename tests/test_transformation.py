import unittest
import pandas as pd
from transformation import clean_data, derive_features

class TestTransformation(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            "sensor_id": ["s1", "s1", "s2", "s2"],
            "timestamp": ["2023-06-01T00:00:00Z", "2023-06-01T01:00:00Z", "2023-06-01T00:00:00Z", "2023-06-01T01:00:00Z"],
            "reading_type": ["temperature", "temperature", "humidity", "humidity"],
            "value": [20.0, 1000.0, 50.0, None],
            "battery_level": [90, 90, 80, 80]
        })

    def test_clean_data(self):
        cleaned = clean_data(self.df)
        self.assertFalse(cleaned['value'].isnull().any())
        self.assertTrue((cleaned['value'] < 1000).all())

    def test_derive_features(self):
        cleaned = clean_data(self.df)
        enriched = derive_features(cleaned)
        self.assertIn('daily_avg_value', enriched.columns)
        self.assertIn('rolling_7d_avg', enriched.columns)
        self.assertIn('anomalous_reading', enriched.columns)
        self.assertIn('calibrated_value', enriched.columns)

if __name__ == '__main__':
    unittest.main()