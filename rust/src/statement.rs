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
use arrow_array::{
    RecordBatch, RecordBatchReader,
    builder::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
        Int32Builder, Int64Builder, StringBuilder, StructBuilder,
    },
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaBuilder, SchemaRef};
use aws_sdk_athena::{
    Client,
    error::DisplayErrorContext,
    types::{ColumnInfo, ColumnNullable, QueryExecutionState},
};
use tokio::{
    runtime::Runtime,
    time::{Duration, sleep},
};

enum PageState {
    HasMore(String),
    Done,
}

struct AthenaResultReader {
    client: Arc<Client>,
    runtime: Arc<Runtime>,
    query_execution_id: String,
    schema: SchemaRef,
    athena_types: Vec<AthenaType>,
    pending_batch: Option<RecordBatch>,
    page_state: PageState,
}

impl AthenaResultReader {
    fn new(
        client: Arc<Client>,
        runtime: Arc<Runtime>,
        query_execution_id: String,
        schema: SchemaRef,
        athena_types: Vec<AthenaType>,
        first_batch: RecordBatch,
        next_token: Option<String>,
    ) -> Self {
        let page_state = match next_token {
            Some(token) => PageState::HasMore(token),
            None => PageState::Done,
        };
        Self {
            client,
            runtime,
            query_execution_id,
            schema,
            athena_types,
            pending_batch: Some(first_batch),
            page_state,
        }
    }

    fn fetch_next_page(&mut self) -> Option<std::result::Result<RecordBatch, ArrowError>> {
        let token = match &self.page_state {
            PageState::HasMore(token) => token.clone(),
            PageState::Done => return None,
        };

        let response = self.runtime.block_on(
            self.client
                .get_query_results()
                .query_execution_id(&self.query_execution_id)
                .next_token(&token)
                .send(),
        );

        match response {
            Err(e) => {
                self.page_state = PageState::Done;
                Some(Err(ArrowError::ExternalError(Box::new(
                    std::io::Error::other(e.to_string()),
                ))))
            }
            Ok(output) => {
                self.page_state = match output.next_token() {
                    Some(t) => PageState::HasMore(t.to_string()),
                    None => PageState::Done,
                };
                let result_set = match output.result_set() {
                    Some(rs) => rs,
                    None => {
                        return Some(Err(ArrowError::ExternalError(Box::new(
                            std::io::Error::other("GetQueryResults returned no result set"),
                        ))));
                    }
                };
                Some(
                    rows_to_record_batch(
                        result_set.rows().iter(),
                        &self.schema,
                        &self.athena_types,
                    )
                    .map_err(|e| {
                        ArrowError::ExternalError(Box::new(std::io::Error::other(
                            DisplayErrorContext(e).to_string(),
                        )))
                    }),
                )
            }
        }
    }
}

impl Iterator for AthenaResultReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.pending_batch.take() {
            return Some(Ok(batch));
        }
        self.fetch_next_page()
    }
}

impl RecordBatchReader for AthenaResultReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

enum AthenaType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Boolean,
    Varchar,
}

impl AthenaType {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "tinyint" => Some(Self::TinyInt),
            "smallint" => Some(Self::SmallInt),
            "int" | "integer" => Some(Self::Int),
            "bigint" => Some(Self::BigInt),
            "float" | "real" => Some(Self::Float),
            "double" => Some(Self::Double),
            "boolean" => Some(Self::Boolean),
            "char" | "string" | "varchar" => Some(Self::Varchar),
            _ => None,
        }
    }

    fn to_data_type(&self) -> DataType {
        match self {
            Self::TinyInt => DataType::Int8,
            Self::SmallInt => DataType::Int16,
            Self::Int => DataType::Int32,
            Self::BigInt => DataType::Int64,
            Self::Float => DataType::Float32,
            Self::Double => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Varchar => DataType::Utf8,
        }
    }

    fn append(&self, builder: &mut dyn ArrayBuilder, value: Option<&str>) {
        let any = builder.as_any_mut();
        match self {
            Self::TinyInt => any
                .downcast_mut::<Int8Builder>()
                .unwrap()
                .append_option(value.and_then(|v| v.parse().ok())),
            Self::SmallInt => any
                .downcast_mut::<Int16Builder>()
                .unwrap()
                .append_option(value.and_then(|v| v.parse().ok())),
            Self::Int => any
                .downcast_mut::<Int32Builder>()
                .unwrap()
                .append_option(value.and_then(|v| v.parse().ok())),
            Self::BigInt => any
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_option(value.and_then(|v| v.parse().ok())),
            Self::Float => any
                .downcast_mut::<Float32Builder>()
                .unwrap()
                .append_option(value.and_then(|v| v.parse().ok())),
            Self::Double => any
                .downcast_mut::<Float64Builder>()
                .unwrap()
                .append_option(value.and_then(|v| v.parse().ok())),
            Self::Boolean => any
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_option(value.map(|v| v.eq_ignore_ascii_case("true"))),
            Self::Varchar => any
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_option(value),
        }
    }
}

async fn run_query(client: &Client, query_string: &str) -> Result<String> {
    let start_response = client
        .start_query_execution()
        .query_string(query_string)
        .send()
        .await
        .map_err(|e| {
            Error::with_message_and_status(DisplayErrorContext(e).to_string(), Status::IO)
        })?;
    let query_execution_id = start_response
        .query_execution_id()
        .ok_or_else(|| {
            Error::with_message_and_status(
                "StartQueryExecution returned no query execution ID",
                Status::Internal,
            )
        })?
        .to_string();
    loop {
        let get_response = client
            .get_query_execution()
            .query_execution_id(&query_execution_id)
            .send()
            .await
            .map_err(|e| {
                Error::with_message_and_status(DisplayErrorContext(e).to_string(), Status::IO)
            })?;
        let state = get_response
            .query_execution()
            .and_then(|qe| qe.status())
            .and_then(|s| s.state())
            .ok_or_else(|| {
                Error::with_message_and_status(
                    "GetQueryExecution returned no query state",
                    Status::Internal,
                )
            })?;
        match state {
            QueryExecutionState::Succeeded => {
                return Ok(query_execution_id);
            }
            QueryExecutionState::Failed => {
                let reason = get_response
                    .query_execution()
                    .and_then(|qe| qe.status())
                    .and_then(|s| s.state_change_reason())
                    .unwrap_or("unknown reason");
                return Err(Error::with_message_and_status(
                    format!("Query {query_execution_id} failed: {reason}"),
                    Status::IO,
                ));
            }
            QueryExecutionState::Cancelled => {
                return Err(Error::with_message_and_status(
                    format!("Query {query_execution_id} was cancelled"),
                    Status::Cancelled,
                ));
            }
            _ => {
                sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

fn schema_from_metadata(
    meta_data: &aws_sdk_athena::types::ResultSetMetadata,
) -> (SchemaRef, Vec<AthenaType>) {
    let mut schema_builder = SchemaBuilder::new();
    let mut athena_types = Vec::new();
    for column_info in meta_data.column_info().iter() {
        let (field, athena_type) = column_info_to_field(column_info);
        schema_builder.push(Arc::new(field));
        athena_types.push(athena_type);
    }
    (Arc::new(schema_builder.finish()), athena_types)
}

fn rows_to_record_batch<'a>(
    rows: impl Iterator<Item = &'a aws_sdk_athena::types::Row>,
    schema: &SchemaRef,
    athena_types: &[AthenaType],
) -> Result<RecordBatch> {
    let mut struct_builder = StructBuilder::from_fields(schema.fields().to_owned(), 1000);
    for row in rows {
        let data = row.data();
        for (column_index, column_builder) in
            struct_builder.field_builders_mut().iter_mut().enumerate()
        {
            let value = data
                .get(column_index)
                .and_then(|datum| datum.var_char_value());
            athena_types[column_index].append(column_builder.as_mut(), value);
        }
        struct_builder.append(true);
    }
    Ok(RecordBatch::from(&struct_builder.finish()))
}

async fn fetch_results(
    client: Arc<Client>,
    runtime: Arc<Runtime>,
    query_execution_id: String,
) -> Result<AthenaResultReader> {
    let output = client
        .get_query_results()
        .query_execution_id(&query_execution_id)
        .send()
        .await
        .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;
    let result_set = output.result_set().ok_or_else(|| {
        Error::with_message_and_status("GetQueryResults returned no result set", Status::Internal)
    })?;
    let meta_data = result_set.result_set_metadata().ok_or_else(|| {
        Error::with_message_and_status(
            "GetQueryResults returned no result set metadata",
            Status::Internal,
        )
    })?;
    let (schema, athena_types) = schema_from_metadata(meta_data);
    let next_token = output.next_token().map(str::to_string);
    let first_batch =
        rows_to_record_batch(result_set.rows().iter().skip(1), &schema, &athena_types)?;
    Ok(AthenaResultReader::new(
        client,
        runtime,
        query_execution_id,
        schema,
        athena_types,
        first_batch,
        next_token,
    ))
}

async fn execute_query(
    client: Arc<Client>,
    runtime: Arc<Runtime>,
    query_string: &str,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let query_execution_id = run_query(&client, query_string).await?;
    let reader = fetch_results(client, runtime, query_execution_id).await?;
    Ok(Box::new(reader))
}

fn column_info_to_field(column_info: &ColumnInfo) -> (Field, AthenaType) {
    let athena_type = AthenaType::from_str(column_info.r#type()).unwrap_or_else(|| {
        todo!(
            "type mapping for {} not implemented yet",
            column_info.r#type()
        )
    });
    let is_nullable = !matches!(column_info.nullable(), Some(ColumnNullable::NotNull));
    let field = Field::new(column_info.name(), athena_type.to_data_type(), is_nullable);
    (field, athena_type)
}

pub struct AthenaStatement {
    pub(crate) sql_query: Option<String>,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) client: Arc<Client>,
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
            self.runtime.block_on(execute_query(
                Arc::clone(&self.client),
                Arc::clone(&self.runtime),
                query_string,
            ))
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
