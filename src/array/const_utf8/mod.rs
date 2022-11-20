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

use crate::array::Array;
use crate::bitmap::Bitmap;
use crate::datatypes::DataType;
use std::any::Any;
use std::iter;

pub(super) mod fmt;

/// An equivalent to [`Utf8Array`] that stores only one unique string.
#[derive(Clone)]
pub struct ConstUtf8Array {
    value: String,
    len: usize,
}

impl ConstUtf8Array {
    /// Returns a new [`ConstUtf8Array`] with the given `value` repeated `len` times.
    pub fn new(value: String, len: usize) -> Self {
        Self { value, len }
    }

    /// Returns the unique value in this array.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the length of this array.
    fn len(&self) -> usize {
        self.len
    }

    /// Returns whether this array is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns `self.len()` copies of the value in this array.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        iter::repeat(&*self.value).take(self.len)
    }
}

impl Array for ConstUtf8Array {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.len
    }

    fn data_type(&self) -> &DataType {
        &DataType::ConstUtf8
    }

    fn validity(&self) -> Option<&Bitmap> {
        None
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        assert!(offset.saturating_add(length) <= self.len);
        unsafe { self.slice_unchecked(offset, length) }
    }

    unsafe fn slice_unchecked(&self, _offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(Self {
            value: self.value.clone(),
            len: length,
        })
    }

    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        assert!(validity.is_none());
        self.to_boxed()
    }

    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}
