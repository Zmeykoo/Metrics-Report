"""
Tables managed from admin interface
"""
from sqlalchemy import Column, String, Integer, Text

from models.base import Model


class RiskScoreRules(Model):
    """
    ARR x Growth 'rainbow' table.
    """
    __tablename__ = 'risk_score_rules'

    id = Column(Integer, primary_key=True)
    kind = Column(String)
    json = Column(Text)

    def __repr__(self):
        return "%s" % (self.kind)


class RiskScoreMultiple(Model):
    __tablename__ = 'risk_score_multiple'

    id = Column(Integer, primary_key=True)
    score = Column(Integer)
    multiple = Column(Integer)

    def __repr__(self):
        return "{}: {}".format(self.score, self.multiple)
