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
    operation::get_query_results::GetQueryResultsOutput,
    types::{ColumnInfo, ColumnNullable, QueryExecutionState},
};
use tokio::{
    runtime::Runtime,
    time::{Duration, sleep},
};

struct AthenaResultReader {
    client: Arc<Client>,
    runtime: Arc<Runtime>,
    query_execution_id: String,
    schema: SchemaRef,
    athena_types: Vec<AthenaType>,
    pending_batch: Option<RecordBatch>,
    next_token: Option<String>,
}

impl AthenaResultReader {
    async fn fetch(
        client: &Client,
        query_execution_id: &str,
        next_token: Option<&str>,
    ) -> Result<GetQueryResultsOutput> {
        let mut request = client
            .get_query_results()
            .query_execution_id(query_execution_id);
        if let Some(token) = next_token {
            request = request.next_token(token);
        }
        request.send().await.map_err(|e| {
            Error::with_message_and_status(DisplayErrorContext(e).to_string(), Status::IO)
        })
    }

    async fn create(
        client: Arc<Client>,
        runtime: Arc<Runtime>,
        query_execution_id: String,
    ) -> Result<Self> {
        let response = Self::fetch(&client, &query_execution_id, None).await?;
        let result_set = response.result_set().ok_or_else(|| {
            Error::with_message_and_status(
                "GetQueryResults returned no result set",
                Status::Internal,
            )
        })?;
        let meta_data = result_set.result_set_metadata().ok_or_else(|| {
            Error::with_message_and_status(
                "GetQueryResults returned no result set metadata",
                Status::Internal,
            )
        })?;
        let next_token = response.next_token().map(str::to_string);
        let (schema, athena_types) = schema_from_metadata(meta_data);
        let pending_batch = Some(rows_to_record_batch(
            result_set.rows().iter().skip(1),
            &schema,
            &athena_types,
        )?);
        Ok(Self {
            client,
            runtime,
            query_execution_id,
            schema,
            athena_types,
            pending_batch,
            next_token,
        })
    }

    fn fetch_next_page_and_token(
        &self,
        next_token: &str,
    ) -> std::result::Result<(RecordBatch, Option<String>), ArrowError> {
        let response = self
            .runtime
            .block_on(Self::fetch(
                &self.client,
                &self.query_execution_id,
                Some(next_token),
            ))
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        let next_token = response.next_token().map(str::to_string);
        let result_set = response.result_set().ok_or_else(|| {
            ArrowError::ExternalError(Box::new(std::io::Error::other(
                "GetQueryResults returned no result set",
            )))
        })?;
        let record_batch =
            rows_to_record_batch(result_set.rows().iter(), &self.schema, &self.athena_types)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        Ok((record_batch, next_token))
    }

    fn fetch_next_page(&mut self) -> Option<std::result::Result<RecordBatch, ArrowError>> {
        self.next_token
            .take()
            .map(|next_token| self.fetch_next_page_and_token(&next_token))
            .map(|page_result| {
                page_result.map(|(record_batch, next_token)| {
                    self.next_token = next_token;
                    record_batch
                })
            })
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

async fn execute_query(
    client: Arc<Client>,
    runtime: Arc<Runtime>,
    query_string: &str,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let query_execution_id = run_query(&client, query_string).await?;
    let reader = AthenaResultReader::create(client, runtime, query_execution_id).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Statement;
    use arrow_array::{Int32Array, StringArray};
    use assertables::assert_all;
    use aws_sdk_athena::{
        operation::{
            get_query_execution::GetQueryExecutionOutput, get_query_results::GetQueryResultsOutput,
            start_query_execution::StartQueryExecutionOutput,
        },
        types::{
            ColumnInfo, ColumnNullable, Datum, QueryExecution, QueryExecutionState,
            QueryExecutionStatus, ResultSet, ResultSetMetadata, Row,
        },
    };
    use aws_smithy_mocks::{Rule, RuleMode, mock, mock_client};

    fn make_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::new().expect("failed to create tokio runtime"))
    }

    fn create_result_set(schema: &[(&str, &str)], data: &[&[&str]]) -> ResultSet {
        let mut columns = Vec::new();
        let mut first_row_data = Vec::new();
        for (name, typ) in schema {
            columns.push(
                ColumnInfo::builder()
                    .name(*name)
                    .r#type(*typ)
                    .nullable(ColumnNullable::Nullable)
                    .build()
                    .unwrap(),
            );
            first_row_data.push(Datum::builder().var_char_value(*name).build());
        }
        let metadata = ResultSetMetadata::builder()
            .set_column_info(Some(columns))
            .build();
        let mut rows = Vec::new();
        rows.push(Row::builder().set_data(Some(first_row_data)).build());
        for r in data {
            let mut row_data = Vec::new();
            for cell in *r {
                row_data.push(Datum::builder().var_char_value(*cell).build());
            }
            rows.push(Row::builder().set_data(Some(row_data)).build());
        }
        ResultSet::builder()
            .result_set_metadata(metadata)
            .set_rows(Some(rows))
            .build()
    }

    fn mock_athena_lifecycle(
        states: Vec<QueryExecutionState>,
        result_set: ResultSet,
    ) -> (Client, Vec<Rule>) {
        let query_execution_id = "test-query-execution-id";
        let mut rules = Vec::new();
        let start_rule =
            mock!(aws_sdk_athena::Client::start_query_execution).then_output(move || {
                StartQueryExecutionOutput::builder()
                    .query_execution_id(query_execution_id)
                    .build()
            });
        rules.push(start_rule);
        for state in states {
            let get_execution_rule = mock!(aws_sdk_athena::Client::get_query_execution)
                .match_requests(|req| req.query_execution_id() == Some(query_execution_id))
                .then_output(move || {
                    let status = QueryExecutionStatus::builder().state(state.clone()).build();
                    GetQueryExecutionOutput::builder()
                        .query_execution(QueryExecution::builder().status(status).build())
                        .build()
                });
            rules.push(get_execution_rule);
        }
        let get_results_rule = mock!(aws_sdk_athena::Client::get_query_results)
            .match_requests(|req| req.query_execution_id() == Some(query_execution_id))
            .then_output(move || {
                GetQueryResultsOutput::builder()
                    .result_set(result_set.clone())
                    .build()
            });
        rules.push(get_results_rule);
        let client = mock_client!(aws_sdk_athena, RuleMode::Sequential, rules.as_slice());
        (client, rules)
    }

    fn athena_happy_path() -> (Client, Vec<Rule>) {
        let states = vec![
            QueryExecutionState::Queued,
            QueryExecutionState::Running,
            QueryExecutionState::Running,
            QueryExecutionState::Succeeded,
        ];
        let result_set = create_result_set(
            &[("col1", "integer"), ("col2", "string")],
            &[&["1", "a"], &["2", "b"]],
        );
        mock_athena_lifecycle(states, result_set)
    }

    #[test]
    fn execute_makes_the_athena_query_lifecycle_api_calls() {
        let (client, rules) = athena_happy_path();
        let runtime = make_runtime();
        let mut statement = AthenaStatement {
            sql_query: None,
            runtime: Arc::clone(&runtime),
            client: Arc::new(client),
        };
        statement.set_sql_query("SELECT 1").unwrap();
        let _ = statement.execute();
        assert_all!(rules.iter(), |r: &Rule| r.num_calls() == 1);
    }

    #[test]
    fn execute_returns_the_query_results_as_a_record_batch() {
        let (client, _) = athena_happy_path();
        let runtime = make_runtime();
        let mut statement = AthenaStatement {
            sql_query: None,
            runtime: Arc::clone(&runtime),
            client: Arc::new(client),
        };
        statement.set_sql_query("SELECT 1").unwrap();
        let batches: Vec<RecordBatch> = statement
            .execute()
            .unwrap()
            .map(|batch| batch.unwrap())
            .collect();
        let col1 = batches[0]
            .column_by_name("col1")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("col1 should be an Int32Array");
        let col2 = batches[0]
            .column_by_name("col2")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("col2 should be a StringArray");
        assert_eq!(col1, &Int32Array::from(vec![1, 2]));
        assert_eq!(col2, &StringArray::from(vec!["a", "b"]));
    }
}
