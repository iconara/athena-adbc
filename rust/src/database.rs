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

use adbc_core::{Database, Optionable, error::Result, options::{OptionDatabase, OptionValue}};

use crate::connection::AthenaConnection;

#[derive(Default)]
pub struct AthenaDatabase {}

impl Database for AthenaDatabase {
    type ConnectionType = AthenaConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(Self::ConnectionType::default())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (<Self::ConnectionType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = Self::ConnectionType::default();
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

impl Optionable for AthenaDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        Ok(())
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
