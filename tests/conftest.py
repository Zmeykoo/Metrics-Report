import logging
import json
import sys
import os
from datetime import datetime
from unittest.mock import MagicMock

import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from flask.testing import FlaskClient
from flask_sqlalchemy import SQLAlchemy

import metrics_data
from metrics_data.app import db, engine, Model
from models import RiskScoreRules, User, Project, RiskScoreMultiple

# Side effect: API endpoint registration
import dataapp  # NOQA

from .risk_score_data import RISK_SCORE_MULTIPLIERS

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class TestClient(FlaskClient):
    def get_json(self, url, status=200, content_type='application/json', **kwargs):
        response = self.get(url, **kwargs)
        assert response.status_code == status, response.data
        assert response.content_type == content_type
        if content_type == 'application/json':
            return json.loads(response.data.decode('utf8'))
        return response.data.decode('utf8')

    def post_json(self, url, data, status=200, **kwargs):
        response = self.post(url, data=json.dumps(data),
                             headers={'content-type': 'application/json'})
        assert response.status_code == status, response.data
        assert response.content_type == 'application/json'
        return json.loads(response.data.decode('utf8'))

    def patch_json(self, url, data, status=200, **kwargs):
        response = self.patch(url, data=json.dumps(data),
                              headers={'content-type': 'application/json'})
        assert response.status_code == status, response.data
        assert response.content_type == 'application/json'
        return json.loads(response.data.decode('utf8'))

    def delete_json(self, url, status=200, **kwargs):
        headers = kwargs.get('headers', {})
        headers.update({'content-type': 'application/json'})
        response = self.delete(url, headers=headers)
        assert response.status_code == status, response.data
        assert response.content_type == 'application/json'
        return json.loads(response.data.decode('utf8'))

    def get_specs(self, prefix='', status=200, **kwargs):
        """
        Get a Swagger specification for a RestPlus API
        """
        return self.get_json('{0}/swagger.json'.format(prefix), status=status, **kwargs)


class AuthBackendMock(metrics_data.resource.AuthBackendBase):
    authenticate = MagicMock(return_value={'session_id': 'test_session_id',
                                           'user_id': 'test_user_id',
                                           'user_email': 'test@example.com',
                                           'user_role': 'Staff'})
    check_roles = MagicMock()


def _digitize_digit(value):
    if type(value) == str:
        if ',' in value:
            return value
        return int(value)
    else:
        return value


def create_records():
    Model.metadata.create_all(bind=engine, checkfirst=True)

    try:
        cnt = RiskScoreRules.query.count()
    except:
        cnt = 0
    if cnt > 0:
        # create once
        return

    dir_name = os.path.dirname(os.path.abspath(__file__))
    file_paths = [i for i in os.listdir(dir_name) if i.endswith('.csv')]
    for fp in file_paths:
        file_path = os.path.join(dir_name, fp)

        obj = RiskScoreRules()
        obj.kind = os.path.splitext(os.path.basename(fp))[0]
        obj.json = pd.read_csv(file_path).to_json()
        db.add(obj)

    usr = User()
    usr.email = 'tst@example.com'
    usr.name = 'test user'
    usr.set_password('blah')
    usr.date_joined = datetime.now()
    usr.company = 'Example Company'
    db.add(usr)

    prj = Project()
    prj.name = 'Test Project'
    prj.user_id = usr.id
    prj.company = usr.company
    db.add(prj)

    for k,v in RISK_SCORE_MULTIPLIERS.items():
        obj = RiskScoreMultiple()
        obj.score = k
        obj.multiple = v
        db.add(obj)

    db.commit()
    db.flush()


@pytest.fixture
def app():
    dapp = dataapp.app
    dapp.test_client_class = TestClient
    metrics_data.resource.get_authenticator = lambda: AuthBackendMock()
    create_records()
    return dapp
