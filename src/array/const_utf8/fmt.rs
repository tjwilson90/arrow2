use super::ConstUtf8Array;
use std::fmt::{Debug, Formatter, Result, Write};

use super::super::fmt::write_vec;

pub fn write_value<W: Write>(array: &ConstUtf8Array, f: &mut W) -> Result {
    write!(f, "{}", array.value())
}

impl Debug for ConstUtf8Array {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let writer = |f: &mut Formatter, _index| write_value(self, f);

        write!(f, "ConstUtf8Array")?;
        write_vec(f, writer, None, self.len(), "None", false)
    }
}
