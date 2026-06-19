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

use std::{collections::HashSet, sync::Arc};

use adbc_core::{
    Connection, Optionable,
    error::Result,
    options::{InfoCode, ObjectDepth, OptionConnection, OptionValue},
};
use arrow_array::RecordBatchReader;
use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
use aws_sdk_athena::Client;
use tokio::runtime::Runtime;

use crate::{athena::AthenaClient, statement::AthenaStatement};

pub struct AthenaConnection {
    athena_client: Arc<AthenaClient>,
}

impl AthenaConnection {
    pub(crate) fn new(runtime: Arc<Runtime>) -> Self {
        let aws_sdk_client = Arc::new(runtime.block_on(create_aws_sdk_client()));
        let athena_client = Arc::new(AthenaClient::new(aws_sdk_client, runtime));
        Self { athena_client }
    }
}

async fn create_aws_sdk_client() -> Client {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    Client::new(&config)
}

impl Connection for AthenaConnection {
    type StatementType = AthenaStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(AthenaStatement::new(self.athena_client.clone()))
    }

    fn cancel(&mut self) -> Result<()> {
        todo!()
    }

    fn commit(&mut self) -> Result<()> {
        todo!()
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<arrow_schema::Schema> {
        todo!()
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        todo!()
    }

    fn rollback(&mut self) -> Result<()> {
        todo!()
    }
}

impl Optionable for AthenaConnection {
    type Option = OptionConnection;

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
