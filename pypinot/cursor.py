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
import logging
from collections import OrderedDict
from pprint import pformat

import requests
from six.moves.urllib import parse

from pypinot.exceptions import (DatabaseError, NotSupportedError,
                                ProgrammingError)
from pypinot.helper import (apply_parameters, check_closed, check_result,
                            get_description_from_types,
                            get_group_by_column_names, get_types_from_rows)

logger = logging.getLogger(__name__)


class Cursor(object):
    """Connection cursor."""

    def __init__(
        self,
        host,
        port=8099,
        scheme="http",
        path="/query",
        extra_request_headers="",
        debug=False,
    ):
        self.url = parse.urlunparse((scheme, f"{host}:{port}", path, None, None, None))

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.array_size = 1

        self.closed = False

        # these are updated only after a query
        self.description = None
        self.rowcount = -1
        self._results = None
        self._debug = debug
        extra_headers = {}
        if extra_request_headers:
            for header in extra_request_headers.split(","):
                k, v = header.split("=")
                extra_headers[k] = v
        self._extra_request_headers = extra_headers

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None):
        query = apply_parameters(operation, parameters or {})

        headers = {"Content-Type": "application/json"}
        headers.update(self._extra_request_headers)
        payload = {"pql": query}
        if self._debug:
            logger.info(
                f"Submitting the pinot query to {self.url}:\n{query}\n{pformat(payload)}, with {headers}"
            )
        r = requests.post(self.url, headers=headers, json=payload)
        if r.encoding is None:
            r.encoding = "utf-8"

        try:
            payload = r.json()
        except Exception as e:
            raise DatabaseError(
                f"Error when querying {query} from {self.url}, raw response is:\n{r.text}"
            ) from e

        if self._debug:
            logger.info(
                f"Got the payload of type {type(payload)} with the status code {0 if not r else r.status_code}:\n{payload}"
            )

        num_servers_responded = payload.get("numServersResponded", -1)
        num_servers_queried = payload.get("numServersQueried", -1)

        if (
            num_servers_queried > num_servers_responded
            or num_servers_responded == -1
            or num_servers_queried == -1
        ):
            raise DatabaseError(
                f"Query\n\n{query} timed out: Out of {num_servers_queried}, only"
                f" {num_servers_responded} responded"
            )

        # raise any error messages
        if r.status_code != 200:
            msg = f"Query\n\n{query}\n\nreturned an error: {r.status_code}\nFull response is {pformat(payload)}"
            raise ProgrammingError(msg)

        if payload.get("exceptions", []):
            msg = "\n".join(pformat(exception) for exception in payload["exceptions"])
            raise DatabaseError(msg)

        rows = []  # array of array, where inner array is array of column values
        column_names = []  # column names, such that len(column_names) == len(rows[0])

        if "aggregationResults" in payload:
            aggregation_results = payload["aggregationResults"]
            gby_cols = get_group_by_column_names(aggregation_results)
            metric_names = [
                agg_result["function"] for agg_result in aggregation_results
            ]
            gby_rows = OrderedDict()  # Dict of group-by-vals to array of metrics
            total_group_vals_key = ()
            num_metrics = len(metric_names)
            for i, agg_result in enumerate(aggregation_results):
                if "groupByResult" in agg_result:
                    if total_group_vals_key in gby_rows:
                        raise DatabaseError(
                            f"Invalid response {pformat(aggregation_results)} since we have both total and group by results"
                        )
                    for gb_result in agg_result["groupByResult"]:
                        group_values = gb_result["group"]
                        if len(group_values) < len(gby_cols):
                            raise DatabaseError(
                                f"Expected {pformat(agg_result)} to contain {len(gby_cols)}, but got {len(group_values)}"
                            )
                        elif len(group_values) > len(gby_cols):
                            # This can happen because of poor escaping in the results
                            extra = len(group_values) - len(gby_cols)
                            new_group_values = group_values[extra:]
                            new_group_values[0] = (
                                "".join(group_values[0:extra]) + new_group_values[0]
                            )
                            group_values = new_group_values

                        group_values_key = tuple(group_values)
                        if group_values_key not in gby_rows:
                            gby_rows[group_values_key] = [None] * num_metrics
                        gby_rows[group_values_key][i] = gb_result["value"]
                else:  # Global aggregation result
                    if total_group_vals_key not in gby_rows:
                        gby_rows[total_group_vals_key] = [None] * num_metrics
                    if len(gby_rows) != 1:
                        raise DatabaseError(
                            f"Invalid response {pformat(aggregation_results)} since we have both total and group by results"
                        )
                    if len(gby_cols) > 0:
                        raise DatabaseError(
                            f"Invalid response since total aggregation results are present even when non zero gby_cols:{gby_cols}, {pformat(aggregation_results)}"
                        )
                    gby_rows[total_group_vals_key][i] = agg_result["value"]

            rows = []
            column_names = gby_cols + metric_names
            for group_vals, metric_vals in gby_rows.items():
                if len(group_vals) != len(gby_cols):
                    raise DatabaseError(
                        f"Expected {len(gby_cols)} but got {len(group_vals)} for a row"
                    )
                if len(metric_vals) != len(metric_names):
                    raise DatabaseError(
                        f"Expected {len(metric_names)} but got {len(metric_vals)} for a row"
                    )
                rows.append(list(group_vals) + metric_vals)
        elif "selectionResults" in payload:
            results = payload["selectionResults"]
            column_names = results.get("columns")
            values = results.get("results")
            if column_names and values:
                rows = values
            else:
                raise DatabaseError(
                    f"Expected columns and results in selectionResults, but got {pformat(results)} instead"
                )

        logger.debug(f"Got the rows as a type {type(rows)} of size {len(rows)}")
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(pformat(rows))
        self.description = None
        self._results = []
        if rows:
            types = get_types_from_rows(column_names, rows)
            if self._debug:
                logger.info(
                    f"There are {len(rows)} rows and types is {pformat(types)}, column_names are {pformat(column_names)}, first row is like {pformat(rows[0])}, and last row is like {pformat(rows[-1])}"
                )
            self._results = rows
            self.description = get_description_from_types(column_names, types)

        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise NotSupportedError("`executemany` is not supported, use `execute` instead")

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self._results.pop(0)
        except IndexError:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.array_size
        output, self._results = self._results[:size], self._results[size:]
        return output

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        array_size attribute can affect the performance of this operation.
        """
        return list(self)

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def __iter__(self):
        return self

    @check_closed
    def __next__(self):
        output = self.fetchone()
        if output is None:
            raise StopIteration

        return output

    next = __next__
