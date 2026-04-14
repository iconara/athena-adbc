// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use adbc_core::options::AdbcVersion;
use adbc_core::{Connection, Database, Driver, Statement, error::Error};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use arrow_array::{Array, RecordBatch, StringArray};

fn connect() -> Result<ManagedConnection, Error> {
    let mut driver = ManagedDriver::load_dynamic_from_name(
        "adbc_athena",
        Some(b"AdbcAthenaInit"),
        AdbcVersion::V110,
    )
    .expect("Driver could not be loaded");
    let database = driver.new_database().unwrap();
    database.new_connection()
}

#[test]
fn test_run_query() {
    let mut connection = connect().unwrap();
    let mut statement = connection.new_statement().unwrap();
    let _ = statement.set_sql_query("SELECT 'world' AS hello").unwrap();
    let result_set = statement.execute().unwrap();
    let batches: Vec<RecordBatch> = result_set.map(|b| b.unwrap()).collect();
    let batch = &batches[0];
    let col_index = batch
        .schema()
        .index_of("hello")
        .expect("column 'hello' not found");
    let col = batch
        .column(col_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column 'hello' should be a StringArray");
    assert_eq!(col.value(0), "world");
}

#[test]
fn test_many_result_pages() {
    let mut connection = connect().unwrap();
    let mut statement = connection.new_statement().unwrap();
    let _ = statement
        .set_sql_query(
            "SELECT sequential_number FROM TABLE(sequence(start => 1, stop => 4321, step => 1))",
        )
        .unwrap();
    let result_set = statement.execute().unwrap();
    let mut row_count = 0;
    for batch in result_set {
        row_count += batch.unwrap().num_rows();
    }
    assert_eq!(row_count, 4321);
}
