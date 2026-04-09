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

use adbc_core::{Optionable, PartitionedResult, Statement, error::Result, options::{OptionStatement, OptionValue}};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};

#[derive(Debug)]
pub struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SingleBatchReader {
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Default)]
pub struct AthenaStatement {}

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
        todo!()
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

    fn set_sql_query(&mut self, _query: impl AsRef<str>) -> Result<()> {
        todo!()
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
