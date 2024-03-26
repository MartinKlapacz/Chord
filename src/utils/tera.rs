use std::collections::HashMap;

use tera::{Result, Value};


pub fn foo(value: Value, _: HashMap<String, Value>) -> Result<Value> {
    let s = String::from("hello");
    Ok(Value::String(s))
}
