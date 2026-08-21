use super::ValueType;

pub(crate) fn name_rule(column_name: &str) -> Option<ValueType> {
    if column_name.eq_ignore_ascii_case("wafer_id") {
        return Some(ValueType::Long);
    }
    if column_name.to_ascii_lowercase().contains("time") {
        return Some(ValueType::DateTime);
    }
    None
}

pub(crate) fn dataframe_dtype(column_name: &str, dtype_name: &str) -> Option<ValueType> {
    if let Some(value_type) = name_rule(column_name) {
        return Some(value_type);
    }

    let dtype = dtype_name.trim().to_ascii_lowercase();
    if dtype == "bool" || dtype == "boolean" || dtype == "python:bool" {
        Some(ValueType::Bool)
    } else if dtype.contains("datetime")
        || dtype.starts_with("timestamp")
        || dtype == "python:datetime"
    {
        Some(ValueType::DateTime)
    } else if dtype == "date" || dtype == "python:date" {
        Some(ValueType::Date)
    } else if dtype == "time" || dtype == "python:time" {
        Some(ValueType::Time)
    } else if dtype.contains("timedelta")
        || dtype.starts_with("duration")
        || dtype == "python:timedelta"
    {
        Some(ValueType::TimeSpan)
    } else if matches!(
        dtype.as_str(),
        "int8" | "int16" | "int32" | "uint8" | "uint16"
    ) {
        Some(ValueType::Int)
    } else if dtype.starts_with("int") || dtype.starts_with("uint") || dtype == "python:int" {
        Some(ValueType::Long)
    } else if dtype == "float32" {
        Some(ValueType::Float)
    } else if dtype.starts_with("float") || dtype == "python:float" {
        Some(ValueType::Double)
    } else if dtype.contains("binary") || dtype == "bytes" || dtype == "python:bytes" {
        Some(ValueType::Binary)
    } else if dtype == "object"
        || dtype == "string"
        || dtype == "str"
        || dtype == "utf8"
        || dtype == "category"
        || dtype == "categorical"
        || dtype == "enum"
        || dtype == "null"
        || dtype == "python:str"
    {
        Some(ValueType::String)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{dataframe_dtype, name_rule};
    use crate::ValueType;

    #[test]
    fn column_name_rules_are_case_insensitive() {
        assert_eq!(name_rule("wafer_id"), Some(ValueType::Long));
        assert_eq!(name_rule("WAFER_ID"), Some(ValueType::Long));
        assert_eq!(name_rule("event_TIME_utc"), Some(ValueType::DateTime));
    }

    #[test]
    fn column_name_rules_override_dataframe_dtype() {
        assert_eq!(dataframe_dtype("wafer_id", "string"), Some(ValueType::Long));
        assert_eq!(
            dataframe_dtype("test_time", "int64"),
            Some(ValueType::DateTime)
        );
    }

    #[test]
    fn common_dataframe_dtypes_map_to_sbdf_types() {
        assert_eq!(dataframe_dtype("flag", "boolean"), Some(ValueType::Bool));
        assert_eq!(dataframe_dtype("count", "Int32"), Some(ValueType::Int));
        assert_eq!(dataframe_dtype("count", "Int64"), Some(ValueType::Long));
        assert_eq!(dataframe_dtype("value", "float64"), Some(ValueType::Double));
        assert_eq!(
            dataframe_dtype("created", "datetime64[ns]"),
            Some(ValueType::DateTime)
        );
        assert_eq!(dataframe_dtype("label", "object"), Some(ValueType::String));
    }
}
