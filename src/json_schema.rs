use serde_json::Value;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonSchemaError(String);

impl JsonSchemaError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl Display for JsonSchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for JsonSchemaError {}

pub fn validate_json_schema(
    value: &Value,
    schema: &Value,
    root: &str,
    schema_label: &str,
) -> Result<(), JsonSchemaError> {
    validate_json_schema_value(value, schema, root, schema_label)
}

fn validate_json_schema_value(
    value: &Value,
    schema: &Value,
    path: &str,
    schema_label: &str,
) -> Result<(), JsonSchemaError> {
    let Some(schema) = schema.as_object() else {
        return Err(JsonSchemaError::new(format!(
            "{schema_label} must be an object"
        )));
    };
    if let Some(types) = schema.get("type") {
        validate_json_schema_type(value, types, path, schema_label)?;
    }
    if let Some(enum_values) = schema.get("enum") {
        let Some(enum_values) = enum_values.as_array() else {
            return Err(JsonSchemaError::new(format!(
                "{schema_label}.enum must be an array"
            )));
        };
        if !enum_values.iter().any(|expected| expected == value) {
            return Err(JsonSchemaError::new(format!(
                "{path} does not match any {schema_label}.enum value"
            )));
        }
    }
    if let Some(required) = schema.get("required") {
        let Some(required) = required.as_array() else {
            return Err(JsonSchemaError::new(format!(
                "{schema_label}.required must be an array"
            )));
        };
        for key in required {
            let Some(key) = key.as_str() else {
                return Err(JsonSchemaError::new(format!(
                    "{schema_label}.required entries must be strings"
                )));
            };
            if value.get(key).is_none() {
                return Err(JsonSchemaError::new(format!(
                    "{path} missing schema-required key `{key}`"
                )));
            }
        }
    }
    if let Some(properties) = schema.get("properties") {
        let Some(properties) = properties.as_object() else {
            return Err(JsonSchemaError::new(format!(
                "{schema_label}.properties must be an object"
            )));
        };
        let Some(object) = value.as_object() else {
            return Err(JsonSchemaError::new(format!("{path} must be an object")));
        };
        for (key, property_schema) in properties {
            if let Some(property_value) = object.get(key) {
                validate_json_schema_value(
                    property_value,
                    property_schema,
                    &format!("{path}.{key}"),
                    schema_label,
                )?;
            }
        }
        if schema.get("additionalProperties").and_then(Value::as_bool) == Some(false) {
            for key in object.keys() {
                if !properties.contains_key(key) {
                    return Err(JsonSchemaError::new(format!(
                        "{path}.{key} is not allowed by {schema_label}.additionalProperties=false"
                    )));
                }
            }
        }
    }
    if let Some(items) = schema.get("items") {
        let Some(array) = value.as_array() else {
            return Err(JsonSchemaError::new(format!("{path} must be an array")));
        };
        for (index, item) in array.iter().enumerate() {
            validate_json_schema_value(item, items, &format!("{path}[{index}]"), schema_label)?;
        }
    }
    Ok(())
}

fn validate_json_schema_type(
    value: &Value,
    types: &Value,
    path: &str,
    schema_label: &str,
) -> Result<(), JsonSchemaError> {
    let allowed = if let Some(kind) = types.as_str() {
        vec![kind]
    } else if let Some(kinds) = types.as_array() {
        let mut parsed = Vec::new();
        for kind in kinds {
            let Some(kind) = kind.as_str() else {
                return Err(JsonSchemaError::new(format!(
                    "{schema_label}.type array entries must be strings"
                )));
            };
            parsed.push(kind);
        }
        parsed
    } else {
        return Err(JsonSchemaError::new(format!(
            "{schema_label}.type must be a string or array"
        )));
    };
    if allowed
        .iter()
        .any(|kind| json_value_matches_type(value, kind))
    {
        return Ok(());
    }
    Err(JsonSchemaError::new(format!(
        "{path} must match {schema_label}.type {}",
        allowed.join("|")
    )))
}

fn json_value_matches_type(value: &Value, kind: &str) -> bool {
    match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    }
}
