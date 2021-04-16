#!/usr/bin/env python

import sqlite3
from icecream import ic
from typing import NamedTuple
from collections import namedtuple
from forbiddenfruit import curse
import inspect

con = sqlite3.connect(':memory:')

q_create_table = """
create table users
( id integer primary key
, name text not null
);
"""

q_insert = """
insert into
users ( name )
values ( ? )
;
"""

q_select = """
select *, 618 as x from users
"""

names = [
    "jdoi 1",
    "jdoi 2",
]

class User(NamedTuple):
    id: int
    name: str

class UserC:
    def __init__(self, id, name):
        self.id = id
        self.name = name

UserNT = namedtuple("UserNT", ["id", "name"])

def create_user(id, name):
    return User(id, name)


def get_params(ctor) -> list[str]:
    sig = inspect.signature(ctor)

    params = [
        # param.name
        # for param in sig.parameters.values()
        param
        for param in sig.parameters
    ]
    ic(params)
    return params

def query(self, ctor, query, params = []):
    cur = con.cursor()
    rows = cur.execute(q_select, params)
    column_names = list(map(lambda tp: tp[0], cur.description))
    ic(column_names)

    var_names = get_params(ctor)
    for row in rows:
        record = dict(zip(column_names, row))
        attrs = {
            key: record.get(key)
            for key in var_names
        }
        yield ctor(**attrs)

def test_query(con, ctor):
    ic(ctor)
    rows = con.query(ctor, q_select)

    for row in rows:
        ic(row)

with con:
    cur = con.cursor()
    cur.execute(q_create_table)

    # cur.execute(q_insert)
    cur.executemany(q_insert, map((lambda name: (name,)), names))

    con.commit()
    curse(con.__class__, "query", query)

    cs = [
        User,
        UserC,
        UserNT,
        create_user,
        lambda id, name: User(id, name),
        lambda name, id: User(id, name),
    ]

    for ctor in cs:
        test_query(con, ctor)
