#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from six import string_types
from enum import Enum
from collections import namedtuple

from pypinot.exceptions import DatabaseError, InternalError, Error


class Type(Enum):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3


def check_closed(function):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise Exception(f'{self.__class__.__name__} already closed')
        return function(self, *args, **kwargs)
    return g


def check_result(function):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):
        if self._results is None:
            raise Error('Called before `execute`')
        return function(self, *args, **kwargs)
    return g


def escape(value):
    if value == '*':
        return value
    elif isinstance(value, string_types):
        return "'{}'".format(value.replace("'", "''"))
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, (list, tuple)):
        return ', '.join(escape(element) for element in value)


def apply_parameters(operation, parameters):
    escaped_parameters = {
        key: escape(value) for key, value in parameters.items()
    }
    return operation % escaped_parameters


def get_group_by_column_names(aggregation_results):
    group_by_cols = []
    for metric in aggregation_results:
        metric_name = metric.get('function', 'noname')
        gby_cols_for_metric = metric.get('groupByColumns', [])
        if group_by_cols and group_by_cols != gby_cols_for_metric:
            raise DatabaseError(f"Cols for metric {metric_name}: {gby_cols_for_metric} differ from other columns {group_by_cols}")
        elif not group_by_cols:
            group_by_cols = gby_cols_for_metric[:]
    return group_by_cols


def get_type(value):
    """Infer type from value."""
    if isinstance(value, string_types):
        return Type.STRING
    elif isinstance(value, (int, float)):
        return Type.NUMBER
    elif isinstance(value, bool):
        return Type.BOOLEAN

    raise Error(f'Value of unknown type: {value}')


def get_types_from_rows(column_names, rows):
    """
    Return description by scraping the rows
    We only return the name and type (inferred from the data).
    """
    if not column_names:
        return []
    if not rows:
        raise InternalError(f'Cannot infer the column types from empty rows')
    types = [None] * len(column_names)
    remaining = len(column_names)
    TypeCodeAndValue = namedtuple('TypeCodeAndValue', ['code', 'value'])
    for row in rows:
        if remaining <= 0:
            break
        if len(row) != len(column_names):
            raise DatabaseError(f'Column names {column_names} does not match row {row}')
        for column_index, value in enumerate(row):
            if value is not None:
                current_type = types[column_index]
                new_type = get_type(value)
                if current_type is None:
                    types[column_index] = TypeCodeAndValue(value=value, code=new_type)
                    remaining -= 1
                elif new_type is not current_type.code:
                    raise DatabaseError(
                            f'Differing column type found for column {name}:'
                            f'{current_type} vs {TypeCodeAndValue(code=new_type, value=value)}')
    if any([t is None for t in types]):
        raise DatabaseError(f'Couldn\'t infer all the types {types}')
    return [t.code for t in types]


def get_types_from_rows(column_names, rows):
    """
    Return description by scraping the rows
    We only return the name and type (inferred from the data).
    """
    if not column_names:
        return []
    if not rows:
        raise InternalError(f'Cannot infer the column types from empty rows')
    types = [None] * len(column_names)
    remaining = len(column_names)
    TypeCodeAndValue = namedtuple('TypeCodeAndValue', ['code', 'value'])
    for row in rows:
        if remaining <= 0:
            break
        if len(row) != len(column_names):
            raise DatabaseError(f'Column names {column_names} does not match row {row}')
        for column_index, value in enumerate(row):
            if value is not None:
                current_type = types[column_index]
                new_type = get_type(value)
                if current_type is None:
                    types[column_index] = TypeCodeAndValue(value=value, code=new_type)
                    remaining -= 1
                elif new_type is not current_type.code:
                    raise DatabaseError(
                            f'Differing column type found for column {name}:'
                            f'{current_type} vs {TypeCodeAndValue(code=new_type, value=value)}')
    if any([t is None for t in types]):
        raise DatabaseError(f'Couldn\'t infer all the types {types}')
    return [t.code for t in types]


def get_description_from_types(column_names, types):
    return [
        (
            name,               # name
            type_code,          # type_code
            None,               # [display_size]
            None,               # [internal_size]
            None,               # [precision]
            None,               # [scale]
            None,               # [null_ok]
        )
        for name, type_code in zip(column_names, types)
    ]
