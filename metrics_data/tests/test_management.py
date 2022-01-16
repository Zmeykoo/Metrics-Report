import unittest
from unittest.mock import patch
import sys
import os

from flask import url_for


def test_arr_x_growth(client, app):
    url = url_for('api.risk_score_rules')
    client.get_json(url, content_type='text/html; charset=utf-8')
