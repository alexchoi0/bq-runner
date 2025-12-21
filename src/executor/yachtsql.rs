use std::fs::File;

use arrow::array::Array;
use arrow::datatypes::DataType as ArrowDataType;
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use yachtsql::QueryExecutor as YachtExecutor;

use crate::error::{Error, Result};

pub use yachtsql::{ColumnInfo, QueryResult};

pub struct YachtSqlExecutor {
    executor: Mutex<YachtExecutor>,
}

impl YachtSqlExecutor {
    pub fn new() -> Result<Self> {
        let executor = YachtExecutor::new();

        Ok(Self {
            executor: Mutex::new(executor),
        })
    }

    pub fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let mut executor = self.executor.lock();

        let result = executor
            .execute_sql(sql)
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        result
            .to_query_result()
            .map_err(|e| Error::Executor(e.to_string()))
    }

    pub fn execute_statement(&self, sql: &str) -> Result<u64> {
        let mut executor = self.executor.lock();

        let result = executor
            .execute_sql(sql)
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        Ok(result.row_count() as u64)
    }

    pub fn load_parquet(
        &self,
        table_name: &str,
        path: &str,
        schema: &[crate::rpc::types::ColumnDef],
    ) -> Result<u64> {
        let file = File::open(path)
            .map_err(|e| Error::Executor(format!("Failed to open parquet file: {}", e)))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::Executor(format!("Failed to read parquet: {}", e)))?;

        let reader = builder
            .build()
            .map_err(|e| Error::Executor(format!("Failed to build parquet reader: {}", e)))?;

        let columns: Vec<String> = schema
            .iter()
            .map(|col| format!("{} {}", col.name, col.column_type))
            .collect();

        let create_sql = format!(
            "CREATE OR REPLACE TABLE {} ({})",
            table_name,
            columns.join(", ")
        );

        let mut executor = self.executor.lock();
        executor
            .execute_sql(&create_sql)
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, create_sql)))?;

        let mut total_rows = 0u64;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| Error::Executor(format!("Failed to read batch: {}", e)))?;

            if batch.num_rows() == 0 {
                continue;
            }

            let mut all_values: Vec<Vec<String>> = Vec::new();
            for row_idx in 0..batch.num_rows() {
                let mut row_values = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let bq_type = &schema[col_idx].column_type;
                    let value = arrow_value_to_sql(col.as_ref(), row_idx, bq_type);
                    row_values.push(value);
                }
                all_values.push(row_values);
            }

            let values_str: Vec<String> = all_values
                .iter()
                .map(|row| format!("({})", row.join(", ")))
                .collect();

            let insert_sql = format!(
                "INSERT INTO {} VALUES {}",
                table_name,
                values_str.join(", ")
            );

            executor
                .execute_sql(&insert_sql)
                .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, insert_sql)))?;

            total_rows += batch.num_rows() as u64;
        }

        Ok(total_rows)
    }

    pub fn list_tables(&self) -> Result<Vec<(String, u64)>> {
        let result = self.execute_query(
            "SELECT table_name, table_rows FROM information_schema.tables WHERE table_schema = 'public'"
        )?;

        let tables: Vec<(String, u64)> = result
            .rows
            .into_iter()
            .map(|row| {
                let name = row[0].as_str().unwrap_or("").to_string();
                let row_count = row[1].as_u64().unwrap_or(0);
                (name, row_count)
            })
            .collect();

        Ok(tables)
    }

    pub fn describe_table(&self, table_name: &str) -> Result<(Vec<(String, String)>, u64)> {
        let schema_sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
            table_name
        );
        let schema_result = self.execute_query(&schema_sql)?;

        let schema: Vec<(String, String)> = schema_result
            .rows
            .into_iter()
            .map(|row| {
                let name = row[0].as_str().unwrap_or("").to_string();
                let col_type = row[1].as_str().unwrap_or("STRING").to_string();
                (name, col_type)
            })
            .collect();

        let count_sql = format!("SELECT COUNT(*) FROM {}", table_name);
        let count_result = self.execute_query(&count_sql)?;
        let row_count = count_result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok((schema, row_count))
    }
}

impl Default for YachtSqlExecutor {
    fn default() -> Self {
        Self::new().expect("Failed to create YachtSQL executor")
    }
}

fn arrow_value_to_sql(array: &dyn Array, row: usize, bq_type: &str) -> String {
    use arrow::array::*;

    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let val = arr.value(row);
            match bq_type.to_uppercase().as_str() {
                "DATE" => format!("DATE_FROM_UNIX_DATE({})", val),
                "TIMESTAMP" => format!("TIMESTAMP_MICROS({})", val),
                _ => val.to_string(),
            }
        }
        ArrowDataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            format!("'{}'", arr.value(row).replace('\'', "''"))
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            format!("'{}'", arr.value(row).replace('\'', "''"))
        }
        ArrowDataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        ArrowDataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = arr.value(row);
            let days = ms / (24 * 60 * 60 * 1000);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        ArrowDataType::Timestamp(unit, _) => {
            let micros = match unit {
                arrow::datatypes::TimeUnit::Second => {
                    let arr = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                    arr.value(row) * 1_000_000
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                    arr.value(row) * 1_000
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    arr.value(row)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                    arr.value(row) / 1_000
                }
            };
            format!("TIMESTAMP_MICROS({})", micros)
        }
        _ => "NULL".to_string(),
    }
}
