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

use adbc_core::{Driver, Optionable, error::Result, options::OptionValue};
use tokio::runtime::Runtime;

use crate::database::AthenaDatabase;

pub struct AthenaDriver {
    runtime: Arc<Runtime>,
}

impl Default for AthenaDriver {
    fn default() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Tokio runtime initialization");
        Self {
            runtime: Arc::new(runtime),
        }
    }
}

impl Driver for AthenaDriver {
    type DatabaseType = AthenaDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(AthenaDatabase {
            runtime: self.runtime.clone(),
        })
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (<Self::DatabaseType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = self.new_database()?;
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}
