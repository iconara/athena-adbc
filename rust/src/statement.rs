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

use std::sync::Arc;

use adbc_core::{
    Optionable, PartitionedResult, Statement,
    error::{Error, Result, Status},
    options::{OptionStatement, OptionValue},
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

use crate::athena::AthenaClient;

pub struct AthenaStatement {
    athena_client: Arc<AthenaClient>,
    sql_query: Option<String>,
}

impl AthenaStatement {
    pub fn new(athena_client: Arc<AthenaClient>) -> Self {
        Self {
            athena_client,
            sql_query: None,
        }
    }
}

impl Statement for AthenaStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        todo!()
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        todo!()
    }

    fn cancel(&mut self) -> Result<()> {
        todo!()
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        if let Some(query_string) = &self.sql_query.clone() {
            self.athena_client.execute(query_string)
        } else {
            Err(Error::with_message_and_status(
                "Can't execute a statement without a SQL query",
                Status::InvalidState,
            ))
        }
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        todo!()
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        todo!()
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        todo!()
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        todo!()
    }

    fn prepare(&mut self) -> Result<()> {
        todo!()
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        todo!()
    }
}

impl Optionable for AthenaStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        todo!()
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        todo!()
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        todo!()
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        todo!()
    }

    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        todo!()
    }
}
