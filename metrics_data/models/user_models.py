import uuid
import enum
from pbkdf2 import crypt

from sqlalchemy import ForeignKey
import sqlalchemy.ext.declarative
from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, Enum
from sqlalchemy.orm import backref, relationship

from models.base import Model


def generate_uuid():
    return str(uuid.uuid4())


class User(Model):
    __tablename__ = 'users'

    id = Column(String, primary_key=True, default=generate_uuid)
    email = Column(String)
    name = Column(String)
    password = Column(String)
    tos_accepted = Column(Boolean, default=False)
    picture = Column(String)
    gender = Column(String)
    company = Column(String)
    is_staff = Column(Boolean, default=False)
    is_approved = Column(Boolean, default=False)
    date_joined = Column(DateTime)

    def __repr__(self):
        return "User {}".format(self.email)

    def set_password(self, password):
        self.password = crypt(password)

    @property
    def serial(self):
        return self.id[:6]

    def to_json(self):
        return {
            'serial': self.serial,
            'name': self.name,
            'email': self.email,
            'company': self.company,
            'is_staff': self.is_staff,
            'date_joined': int(self.date_joined.timestamp())
        }


class TokenType(enum.Enum):
    LOCAL = 0
    GOOGLE = 1


class Token(Model):
    __tablename__ = 'tokens'

    id = Column(String, primary_key=True, default=generate_uuid)
    issued_at = Column(DateTime)
    expires = Column(DateTime)
    user_email = Column(String)
    user_id = Column(String)
    user_name = Column(String)
    user_company = Column(String)
    kind = Column(Enum(TokenType))

    def __repr__(self):
        return "Token %s" % (self.email)

    def to_json(self):
        return {
            'id': self.id,
            'issued_at': int(self.issued_at.timestamp()),
            'expires': int(self.expires.timestamp())
        }


class Project(Model):
    """
    User's project
    """
    __tablename__ = 'projects'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    company = Column(String)
    # Companies vary which months they call their "Fiscal Quarters".
    # We can allow them to assign the months of the year to quarters. 
    # By default 1-3 = Q1, 4-6 = Q2, 7-9 = Q3, 10-12 = Q4
    fiscal_quarters = Column(String)
    user_id = Column(String, ForeignKey('users.id'))
    user = relationship("User", backref=backref('projects'))

    def __repr__(self):
        return "Project %s" % (self.name)


class MetricsReportHistory(Model):
    """
    Stores data, that are used for all other types of reports.
    """
    __tablename__ = 'metrics_reports'

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'))
    project = relationship("Project", backref=backref('metrics_reports'))
    json = Column(Text)
    csv = Column(Text)
    score = Column(Integer)
    score_data = Column(Text)  # stores data, used for calculating score
    last_quarter_arr = Column(Integer)
    timestamp = Column(DateTime)

    def __repr__(self):
        return "Metrics Report %s" % (self.project.name)
