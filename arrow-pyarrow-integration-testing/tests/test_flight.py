# -*- coding: utf-8 -*-
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

from contextlib import contextmanager

import pyarrow as pa
import arrow_pyarrow_integration_testing as rust
from pyarrow.flight import FlightServerBase, FlightClient, FlightDescriptor, FlightEndpoint, FlightInfo, Location, Ticket, RecordBatchStream
from test_sql import StreamWrapper

class FlightServer(FlightServerBase):
    def do_get(self, context, ticket: Ticket):
        # more than one column are needed to test loading struct data type
        # string column is needed to test particular code path where pointers are dereferenced early
        table = pa.Table.from_pydict({"c1": [1, 2, 3], "c2": ["one", "two", "three"]})
        return RecordBatchStream(table)

def test_flight_data():
    with FlightServer() as server:
        # FlightServer is known to produce mis-aligned memory due to gRPC:
        # https://github.com/apache/arrow/issues/32276
        # This tests that Rust Arrow handles mis-alignment
        client = FlightClient(f"grpc+tcp://localhost:{server.port}")
        batches = client.do_get(Ticket("data")).read_all().to_batches()
        assert len(batches) == 1
        batch = batches[0]

        wrapped = StreamWrapper(batch)
        b = rust.round_trip_record_batch_reader(wrapped)
        new_table = b.read_all()
        new_batches = new_table.to_batches()

        assert len(new_batches) == 1
        new_batch = new_batches[0]

        assert batch == new_batch
        assert batch.schema == new_batch.schema
