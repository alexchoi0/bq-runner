use serde_json::Value as JsonValue;
use yachtsql::Value;

pub fn json_to_sql_value(val: &JsonValue) -> String {
    match val {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
        JsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_sql_value).collect();
            format!("[{}]", items.join(", "))
        }
        JsonValue::Object(obj) => {
            let fields: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("'{}': {}", k, json_to_sql_value(v)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
    }
}

pub fn value_to_sql(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Numeric(d) => d.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Bytes(b) => format!("b'{}'", hex::encode(b)),
        Value::Date(d) => format!("DATE '{}'", d),
        Value::Time(t) => format!("TIME '{}'", t),
        Value::DateTime(dt) => format!("DATETIME '{}'", dt),
        Value::Timestamp(ts) => format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f UTC")),
        Value::Json(j) => format!("'{}'", j.to_string().replace('\'', "''")),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(value_to_sql).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Struct(fields) => {
            let items: Vec<String> = fields.iter().map(|(_, v)| value_to_sql(v)).collect();
            format!("({})", items.join(", "))
        }
        Value::Geography(g) => format!("'{}'", g.replace('\'', "''")),
        Value::Interval(i) => format!("INTERVAL {} {} {}", i.months, i.days, i.nanos),
        Value::Range(r) => {
            let start = r.start.as_ref().map(|v| value_to_sql(v)).unwrap_or_else(|| "UNBOUNDED".to_string());
            let end = r.end.as_ref().map(|v| value_to_sql(v)).unwrap_or_else(|| "UNBOUNDED".to_string());
            format!("[{}, {})", start, end)
        }
        Value::Default => "DEFAULT".to_string(),
    }
}
