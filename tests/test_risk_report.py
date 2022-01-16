import os
import sys
from csv import reader

import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metrics_data import risk_report
from metrics_data.csv_extras import UnicodeDictReader

class RiskReportTest(unittest.TestCase):

    @patch('metrics_data.base_report.pd')
    def test_get_score_simple(self, mock_pd):
        with open('active_logo_growth.csv') as fd:
            for row in reader(fd):
                row = [int(risk_report.RiskReport._digitize_digit(i)) for i in row]
                score = risk_report.RiskReport.get_score_simple('active_logo_growth', row[0])
                assert score == row[1]

        with open('active_logos.csv') as fd:
            for row in reader(fd):
                row = [int(risk_report.RiskReport._digitize_digit(i)) for i in row]
                score = risk_report.RiskReport.get_score_simple('active_logos', row[0])
                assert score == row[1]

    @patch('metrics_data.base_report.pd')
    def test_get_score(self, mock_pd):
        with open('active_logos_x_growth.csv') as fd:
            first_row = None
            for row in reader(fd):
                if row[0] == '':
                    first_row = row
                    continue
                value = int(risk_report.RiskReport._digitize_digit(row[0]))
                for i in range(1, len(first_row)):
                    col = int(risk_report.RiskReport._digitize_digit(first_row[i]))
                    score = risk_report.RiskReport.get_score('active_logos_x_growth', value, col)
                    cell = int(row[i])
                    assert score == cell


if __name__ == "__main__":
    unittest.main()
