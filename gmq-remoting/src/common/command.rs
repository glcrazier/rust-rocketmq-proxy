use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use serde::{Deserialize, Serialize};

use crate::util::{read_u32, Error};

static REQUEST_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Header {
    code: u8,
    flag: u8,
    language: String,
    opaque: usize,
    remark: Option<String>,
    #[serde(default)]
    ext_fields: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct Command {
    header: Header,
    body: Option<Vec<u8>>,
}

impl Command {
    pub fn new(code: u8) -> Self {
        Self::new_with_header(code, HashMap::new())
    }

    pub fn new_with_header(code: u8, custom_header: impl Into<HashMap<String, String>>) -> Self {
        Self {
            body: None,
            header: Header {
                code,
                flag: 0,
                language: "RUST".to_string(),
                opaque: REQUEST_ID.fetch_add(1, Ordering::Relaxed),
                remark: None,
                ext_fields: custom_header.into(),
            },
        }
    }

    pub fn code(&self) -> u8 {
        self.header.code
    }

    pub fn opaque(&self) -> usize {
        self.header.opaque
    }

    pub fn add_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.header.ext_fields.insert(key.into(), value.into());
    }

    pub fn get_property(&self, key: &str) -> Option<&String> {
        self.header.ext_fields.get(key)
    }

    pub fn set_body(&mut self, body: Vec<u8>) {
        self.body = Some(body);
    }

    pub fn body(&self) -> Option<&[u8]> {
        self.body.as_ref().map(|v| v.as_ref())
    }

    pub fn encode(self) -> Vec<u8> {
        let mut length: u32 = 4;

        let header_data = serde_json::to_vec(&self.header).unwrap();
        length = length + header_data.len() as u32;

        let ref_body = self.body.as_ref();
        if let Some(body) = ref_body {
            length = length + body.len() as u32;
        }
        let header_len = (0 << 24) | (header_data.len() & 0x00FFFFFF) as u32;
        let mut result = Vec::with_capacity(4 + length as usize);
        result.extend(length.to_be_bytes());
        result.extend(header_len.to_be_bytes());
        result.extend(header_data);
        if let Some(body) = self.body {
            result.extend(body);
        }

        result
    }

    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        let length = read_u32(data);
        let header_length = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        let header: Header = serde_json::from_slice(&data[8..8 + header_length as usize])
            .map_err(|e| Error::DecodeCommandError(e.into()))?;
        let total_length = 4 + length;
        Ok(Self {
            header,
            body: Some(data[8 + header_length as usize..total_length as usize].to_vec()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let mut command = Command::new(1);
        command.add_property("test-key", "value");
        command.set_body(vec![1, 2, 3]);

        let encoded = command.encode();
        let decoded = Command::decode(&encoded).unwrap();
        assert_eq!(1, decoded.code());
        assert_eq!(0, decoded.opaque());
        assert_eq!("value", decoded.get_property("test-key").unwrap());
        assert_eq!(vec![1, 2, 3], decoded.body().unwrap());
    }
}
