#!/usr/bin/env python
import json
import pandas


def data_frame(query, columns):
    """
    Takes a sqlalchemy query and a list of columns, returns a dataframe.
    """

    def make_row(x):
        return dict([(c, getattr(x, c)) for c in columns])

    return pandas.DataFrame([make_row(x) for x in query])

s = data_frame()
print s.columns.values