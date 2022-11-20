use std::sync::Arc;

use crate::{
    array::{Array, ConstUtf8Array, Utf8Array},
    bitmap::MutableBitmap,
    datatypes::DataType,
    offset::Offsets,
};

use super::Growable;

/// Concrete [`Growable`] for the [`ConstUtf8Array`].
pub struct GrowableConstUtf8<'a> {
    arrays: Vec<&'a ConstUtf8Array>,
    validity: MutableBitmap,
    values: Vec<u8>,
    offsets: Offsets<i32>,
}

impl<'a> GrowableConstUtf8<'a> {
    /// Creates a new [`GrowableUtf8`] bound to `arrays` with a pre-allocated `capacity`.
    /// # Panics
    /// If `arrays` is empty.
    pub fn new(arrays: Vec<&'a ConstUtf8Array>, _use_validity: bool, capacity: usize) -> Self {
        Self {
            arrays: arrays.to_vec(),
            values: Vec::with_capacity(0),
            offsets: Offsets::with_capacity(capacity),
            validity: MutableBitmap::with_capacity(capacity),
        }
    }

    fn to(&mut self) -> Utf8Array<i32> {
        let validity = std::mem::take(&mut self.validity);
        let offsets = std::mem::take(&mut self.offsets);
        let values = std::mem::take(&mut self.values);

        #[cfg(debug_assertions)]
        {
            crate::array::specification::try_check_utf8(&offsets, &values).unwrap();
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
            self.offsets.try_push_usize(value.len()).unwrap();
        }
    }

    fn extend_validity(&mut self, additional: usize) {
        self.offsets.extend_constant(additional);
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
