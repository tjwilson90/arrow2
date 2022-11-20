use std::sync::Arc;

use crate::array::ConstUtf8Array;
use crate::datatypes::DataType;
use crate::{
    array::{Array, Utf8Array},
    bitmap::MutableBitmap,
};

use super::Growable;

/// Concrete [`Growable`] for the [`ConstUtf8Array`].
pub struct GrowableConstUtf8<'a> {
    arrays: Vec<&'a ConstUtf8Array>,
    validity: MutableBitmap,
    values: Vec<u8>,
    offsets: Vec<i32>,
    length: i32, // always equal to the last offset at `offsets`.
}

impl<'a> GrowableConstUtf8<'a> {
    /// Creates a new [`GrowableUtf8`] bound to `arrays` with a pre-allocated `capacity`.
    /// # Panics
    /// If `arrays` is empty.
    pub fn new(arrays: Vec<&'a ConstUtf8Array>, _use_validity: bool, capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        let length = 0;
        offsets.push(length);

        Self {
            arrays: arrays.to_vec(),
            values: Vec::with_capacity(0),
            offsets,
            length,
            validity: MutableBitmap::with_capacity(capacity),
        }
    }

    fn to(&mut self) -> Utf8Array<i32> {
        let validity = std::mem::take(&mut self.validity);
        let offsets = std::mem::take(&mut self.offsets);
        let values = std::mem::take(&mut self.values);

        #[cfg(debug_assertions)]
        {
            crate::array::specification::try_check_offsets_and_utf8(&offsets, &values).unwrap();
        }

        unsafe {
            Utf8Array::<i32>::try_new_unchecked(
                DataType::Utf8,
                offsets.into(),
                values.into(),
                validity.into(),
            )
            .unwrap()
        }
    }
}

impl<'a> Growable<'a> for GrowableConstUtf8<'a> {
    fn extend(&mut self, index: usize, _start: usize, len: usize) {
        let array = self.arrays[index];
        let value = array.value().as_bytes();
        self.values.reserve(len * value.len());
        self.offsets.reserve(len);
        for _ in 0..len {
            self.values.extend_from_slice(value);
            self.offsets.push(self.values.len().try_into().unwrap());
        }
        self.length = *self.offsets.last().unwrap();
    }

    fn extend_validity(&mut self, additional: usize) {
        self.offsets
            .resize(self.offsets.len() + additional, self.length);
        self.validity.extend_constant(additional, false);
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a> From<GrowableConstUtf8<'a>> for Utf8Array<i32> {
    fn from(mut val: GrowableConstUtf8<'a>) -> Self {
        val.to()
    }
}
