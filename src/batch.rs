/// A sequence of writes that RustKV commits atomically.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BatchOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WriteBatch {
    // 声明和实现是分离的, 实现是implict
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.operations.push(BatchOperation::Put {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        });
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        self.operations.push(BatchOperation::Delete {
            key: key.as_ref().to_vec(),
        });
    }

    pub fn clear(&mut self) {
        self.operations.clear();
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.operations().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.operations().is_empty()
    }

    pub(crate) fn operations(&self) -> &[BatchOperation] {
        &self.operations
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchOperation, WriteBatch};

    #[test]
    fn new_and_default_batches_are_empty() {
        for batch in [WriteBatch::new(), WriteBatch::default()] {
            assert!(batch.is_empty());
            assert_eq!(batch.len(), 0);
            assert!(batch.operations().is_empty());
        }
    }

    #[test]
    fn operations_preserve_insertion_order_and_owned_bytes() {
        let mut key = b"key".to_vec();
        let mut value = b"value".to_vec();
        let mut batch = WriteBatch::new();

        batch.put(&key, &value);
        batch.delete(b"other");
        batch.put(b"key", b"last");
        key.fill(b'x');
        value.fill(b'y');

        assert_eq!(
            batch.operations(),
            [
                BatchOperation::Put {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                },
                BatchOperation::Delete {
                    key: b"other".to_vec(),
                },
                BatchOperation::Put {
                    key: b"key".to_vec(),
                    value: b"last".to_vec(),
                },
            ]
        );

        assert_eq!(b"xxx".to_vec(), key);
        assert_eq!(b"yyyyy".to_vec(), value);
    }

    #[test]
    fn put_accepts_an_empty_value() {
        let mut batch = WriteBatch::new();
        batch.put(b"empty-value", []);

        assert_eq!(
            batch.operations(),
            [BatchOperation::Put {
                key: b"empty-value".to_vec(),
                value: Vec::new(),
            }]
        );
    }

    #[test]
    fn clear_removes_all_operations_and_batch_can_be_reused() {
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"1");
        batch.delete(b"b");
        assert_eq!(batch.len(), 2);

        batch.clear();
        assert!(batch.is_empty());

        batch.put(b"c", b"3");
        assert_eq!(
            batch.operations(),
            [BatchOperation::Put {
                key: b"c".to_vec(),
                value: b"3".to_vec(),
            }]
        );
    }
}
