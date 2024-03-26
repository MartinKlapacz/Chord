use std::collections::HashMap;

use tera::{Filter, Result, Value};

pub struct WebUrlFromGrpcUrl {}

impl Filter for WebUrlFromGrpcUrl {
    fn filter(&self, value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
        let value = value.to_string();
        let value = &value[1..value.len() - 1];
        match value {
            "" => { Ok(Value::String(String::default())) }
            value => {
                let last_port_digit = value.as_bytes()[value.len() - 1] - 48;
                Ok(Value::String(format!("http://chord.martinklapacz.org:571{}", last_port_digit)))
            }
        }
    }

    fn is_safe(&self) -> bool {
        true
    }
}
