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
use adbc_core::{Driver, Database, Connection, Statement};
use adbc_driver_manager::ManagedDriver;

#[test]
fn test_create_statement() {
    let mut driver = ManagedDriver::load_dynamic_from_name(
        "adbc_athena",
        Some(b"AdbcAthenaInit"),
        AdbcVersion::V110
    )
    .unwrap();
    let database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    let _ = statement.set_sql_query("SELECT 'world' AS Hello");
}
