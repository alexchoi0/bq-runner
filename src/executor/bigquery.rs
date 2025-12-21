use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::get::GetJobRequest;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::job::{
    Job, JobConfiguration, JobConfigurationLoad, JobReference, JobState, JobType, WriteDisposition,
};
use google_cloud_bigquery::http::table::{
    SourceFormat, TableFieldSchema, TableFieldType, TableReference, TableSchema,
};
use google_cloud_bigquery::http::tabledata::list::Value as BqValue;
use tokio::runtime::Handle;
use yachtsql::Value;

use crate::error::{Error, Result};
use crate::rpc::types::ColumnDef;

use super::yachtsql::ColumnInfo;
use super::QueryResult;

pub struct BigQueryExecutor {
    client: Client,
    project_id: String,
    dataset_id: Option<String>,
    query_timeout_ms: Option<i64>,
}

impl BigQueryExecutor {
    pub async fn new() -> Result<Self> {
        let (config, project_id) = ClientConfig::new_with_auth()
            .await
            .map_err(|e| Error::Executor(format!("Failed to authenticate: {}", e)))?;

        let project_id = project_id
            .ok_or_else(|| Error::Executor("No project_id in credentials".into()))?;

        let client = Client::new(config)
            .await
            .map_err(|e| Error::Executor(format!("Failed to create BigQuery client: {}", e)))?;

        let dataset_id = std::env::var("BQ_DATASET").ok();
        let query_timeout_ms = std::env::var("BQ_QUERY_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok());

        Ok(Self {
            client,
            project_id,
            dataset_id,
            query_timeout_ms,
        })
    }

    pub fn load_parquet(
        &self,
        table_name: &str,
        path: &str,
        schema: &[ColumnDef],
    ) -> Result<u64> {
        if !path.starts_with("gs://") {
            return Err(Error::Executor(
                "BigQuery load_parquet requires a GCS path (gs://bucket/path)".to_string(),
            ));
        }

        let dataset_id = self.dataset_id.as_ref().ok_or_else(|| {
            Error::Executor("BQ_DATASET environment variable must be set for load_parquet".into())
        })?;

        let handle = Handle::current();
        handle.block_on(self.load_parquet_async(table_name, path, schema, dataset_id))
    }

    async fn load_parquet_async(
        &self,
        table_name: &str,
        gcs_path: &str,
        schema: &[ColumnDef],
        dataset_id: &str,
    ) -> Result<u64> {
        let table_schema = TableSchema {
            fields: schema
                .iter()
                .map(|col| TableFieldSchema {
                    name: col.name.clone(),
                    data_type: string_to_bq_type(&col.column_type),
                    ..Default::default()
                })
                .collect(),
        };

        let load_config = JobConfigurationLoad {
            source_uris: vec![gcs_path.to_string()],
            destination_table: TableReference {
                project_id: self.project_id.clone(),
                dataset_id: dataset_id.to_string(),
                table_id: table_name.to_string(),
            },
            schema: Some(table_schema),
            source_format: Some(SourceFormat::Parquet),
            write_disposition: Some(WriteDisposition::WriteTruncate),
            ..Default::default()
        };

        let job = Job {
            job_reference: JobReference {
                project_id: self.project_id.clone(),
                job_id: format!("load_parquet_{}", uuid::Uuid::new_v4()),
                location: None,
            },
            configuration: JobConfiguration {
                job_type: "LOAD".to_string(),
                job: JobType::Load(load_config),
                ..Default::default()
            },
            ..Default::default()
        };

        let created_job = self
            .client
            .job()
            .create(&job)
            .await
            .map_err(|e| Error::Executor(format!("Failed to create load job: {}", e)))?;

        let job_id = &created_job.job_reference.job_id;
        if job_id.is_empty() {
            return Err(Error::Executor(
                "Load job created but no job ID returned".into(),
            ));
        }

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let get_request = GetJobRequest { location: None };
            let status = self
                .client
                .job()
                .get(&self.project_id, job_id, &get_request)
                .await
                .map_err(|e| Error::Executor(format!("Failed to get job status: {}", e)))?;

            if status.status.state == JobState::Done {
                if let Some(err) = &status.status.error_result {
                    return Err(Error::Executor(format!(
                        "Load job failed: {:?}",
                        err.message
                    )));
                }

                let rows = status
                    .statistics
                    .and_then(|s| s.load)
                    .and_then(|l| l.output_rows)
                    .unwrap_or(0) as u64;

                return Ok(rows);
            }
        }
    }

    pub fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let handle = Handle::current();
        handle.block_on(self.execute_query_async(sql))
    }

    async fn execute_query_async(&self, sql: &str) -> Result<QueryResult> {
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            timeout_ms: self.query_timeout_ms,
            ..Default::default()
        };

        let response = self
            .client
            .job()
            .query(&self.project_id, &request)
            .await
            .map_err(|e| Error::Executor(format!("BigQuery query failed: {}\n\nSQL: {}", e, sql)))?;

        let field_types: Vec<TableFieldType> = response
            .schema
            .as_ref()
            .map(|s| s.fields.iter().map(|f| f.data_type.clone()).collect())
            .unwrap_or_default();

        let schema: Vec<ColumnInfo> = response
            .schema
            .as_ref()
            .map(|s| {
                s.fields
                    .iter()
                    .map(|field| ColumnInfo {
                        name: field.name.clone(),
                        data_type: bq_type_to_string(&field.data_type),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let rows: Vec<Vec<Value>> = response
            .rows
            .unwrap_or_default()
            .into_iter()
            .map(|tuple| {
                tuple
                    .f
                    .into_iter()
                    .zip(field_types.iter())
                    .map(|(cell, field_type)| bq_value_to_yachtsql(cell.v, field_type))
                    .collect()
            })
            .collect();

        Ok(QueryResult { schema, rows })
    }

    pub fn execute_statement(&self, sql: &str) -> Result<u64> {
        let handle = Handle::current();
        handle.block_on(self.execute_statement_async(sql))
    }

    async fn execute_statement_async(&self, sql: &str) -> Result<u64> {
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            timeout_ms: self.query_timeout_ms,
            ..Default::default()
        };

        let response = self
            .client
            .job()
            .query(&self.project_id, &request)
            .await
            .map_err(|e| Error::Executor(format!("BigQuery statement failed: {}\n\nSQL: {}", e, sql)))?;

        Ok(response.num_dml_affected_rows.unwrap_or(0) as u64)
    }
}

fn bq_type_to_string(field_type: &TableFieldType) -> String {
    match field_type {
        TableFieldType::String => "STRING".to_string(),
        TableFieldType::Bytes => "BYTES".to_string(),
        TableFieldType::Integer | TableFieldType::Int64 => "INT64".to_string(),
        TableFieldType::Float | TableFieldType::Float64 => "FLOAT64".to_string(),
        TableFieldType::Boolean | TableFieldType::Bool => "BOOLEAN".to_string(),
        TableFieldType::Timestamp => "TIMESTAMP".to_string(),
        TableFieldType::Record | TableFieldType::Struct => "STRUCT".to_string(),
        TableFieldType::Date => "DATE".to_string(),
        TableFieldType::Time => "TIME".to_string(),
        TableFieldType::Datetime => "DATETIME".to_string(),
        TableFieldType::Numeric | TableFieldType::Decimal => "NUMERIC".to_string(),
        TableFieldType::Bignumeric | TableFieldType::Bigdecimal => "BIGNUMERIC".to_string(),
        TableFieldType::Interval => "INTERVAL".to_string(),
        TableFieldType::Json => "JSON".to_string(),
    }
}

fn string_to_bq_type(type_str: &str) -> TableFieldType {
    match type_str.to_uppercase().as_str() {
        "STRING" => TableFieldType::String,
        "BYTES" => TableFieldType::Bytes,
        "INT64" | "INTEGER" => TableFieldType::Int64,
        "FLOAT64" | "FLOAT" => TableFieldType::Float64,
        "BOOLEAN" | "BOOL" => TableFieldType::Boolean,
        "TIMESTAMP" => TableFieldType::Timestamp,
        "DATE" => TableFieldType::Date,
        "TIME" => TableFieldType::Time,
        "DATETIME" => TableFieldType::Datetime,
        "NUMERIC" | "DECIMAL" => TableFieldType::Numeric,
        "BIGNUMERIC" | "BIGDECIMAL" => TableFieldType::Bignumeric,
        "INTERVAL" => TableFieldType::Interval,
        "JSON" => TableFieldType::Json,
        "STRUCT" | "RECORD" => TableFieldType::Struct,
        _ => TableFieldType::String,
    }
}

fn bq_value_to_yachtsql(value: BqValue, field_type: &TableFieldType) -> Value {
    match value {
        BqValue::Null => Value::Null,
        BqValue::String(s) => parse_bq_string(&s, field_type),
        BqValue::Array(cells) => {
            let inner_type = match field_type {
                TableFieldType::Record | TableFieldType::Struct => field_type.clone(),
                _ => field_type.clone(),
            };
            Value::Array(
                cells
                    .into_iter()
                    .map(|c| bq_value_to_yachtsql(c.v, &inner_type))
                    .collect(),
            )
        }
        BqValue::Struct(tuple) => Value::Struct(
            tuple
                .f
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    let val = bq_value_to_yachtsql(c.v, &TableFieldType::String);
                    (format!("f{}", i), val)
                })
                .collect(),
        ),
    }
}

fn parse_bq_string(s: &str, field_type: &TableFieldType) -> Value {
    match field_type {
        TableFieldType::String => Value::String(s.to_string()),
        TableFieldType::Bytes => {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                .map(Value::Bytes)
                .unwrap_or_else(|_| Value::String(s.to_string()))
        }
        TableFieldType::Integer | TableFieldType::Int64 => s
            .parse::<i64>()
            .map(Value::Int64)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        TableFieldType::Float | TableFieldType::Float64 => s
            .parse::<f64>()
            .map(Value::float64)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        TableFieldType::Boolean | TableFieldType::Bool => match s.to_lowercase().as_str() {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => Value::String(s.to_string()),
        },
        TableFieldType::Timestamp => {
            if let Ok(ts) = s.parse::<f64>() {
                let secs = ts as i64;
                let nanos = ((ts - secs as f64) * 1_000_000_000.0) as u32;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                    return Value::Timestamp(dt);
                }
            }
            chrono::DateTime::parse_from_rfc3339(s)
                .map(|dt| Value::Timestamp(dt.with_timezone(&chrono::Utc)))
                .unwrap_or_else(|_| Value::String(s.to_string()))
        }
        TableFieldType::Date => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map(Value::Date)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        TableFieldType::Time => chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
            .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
            .map(Value::Time)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        TableFieldType::Datetime => chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
            .map(Value::DateTime)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        TableFieldType::Numeric | TableFieldType::Decimal => s
            .parse::<rust_decimal::Decimal>()
            .map(Value::Numeric)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        TableFieldType::Bignumeric | TableFieldType::Bigdecimal => s
            .parse::<rust_decimal::Decimal>()
            .map(Value::Numeric)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        TableFieldType::Json => serde_json::from_str(s)
            .map(Value::Json)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        TableFieldType::Record | TableFieldType::Struct => Value::String(s.to_string()),
        TableFieldType::Interval => Value::String(s.to_string()),
    }
}
