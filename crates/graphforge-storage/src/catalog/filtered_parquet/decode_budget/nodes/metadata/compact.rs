//! Bounded compact-footer inspection before the pinned decoder allocates.
use super::{add, mul, refused, slot_bytes};
use datafusion::error::DataFusionError;
use std::io::{Seek, SeekFrom};
use thrift::protocol::{TCompactInputProtocol, TInputProtocol, TType};
type Protocol<'a> = TCompactInputProtocol<std::io::Cursor<&'a [u8]>>;

pub(super) struct Facts {
    pub(super) objects: usize,
    pub(super) slots: usize,
    pub(super) strings: usize,
    pub(super) arrow_schema: Option<(usize, usize)>,
}
pub(super) struct Scan<'a> {
    protocol: Protocol<'a>,
    bytes: &'a [u8],
    pub(super) objects: usize,
    pub(super) slots: usize,
    pub(super) strings: usize,
}
impl<'a> Scan<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self {
            protocol: TCompactInputProtocol::new(std::io::Cursor::new(bytes)),
            bytes,
            objects: 0,
            slots: 0,
            strings: 0,
        }
    }
    fn position(&mut self) -> Result<usize, DataFusionError> {
        let position = self
            .protocol
            .stream_position()
            .map_err(DataFusionError::IoError)?;
        usize::try_from(position).map_err(|_| refused())
    }
    fn field(
        &mut self,
        last: &mut i16,
    ) -> Result<thrift::protocol::TFieldIdentifier, DataFusionError> {
        let position = self.position()?;
        let byte = *self.bytes.get(position).ok_or_else(refused)?;
        if byte >> 4 != 0 && last.checked_add(i16::from(byte >> 4)).is_none() {
            return Err(refused());
        }
        let field = self
            .protocol
            .read_field_begin()
            .map_err(|error| DataFusionError::Context(
                format!("node metadata field at byte {position}, header {byte:#04x}, previous {last}: {error:?}"),
                Box::new(crate::catalog::parquet_err(error)),
            ))?;
        if let Some(id) = field.id {
            *last = id;
        }
        Ok(field)
    }
    fn string(&mut self) -> Result<(usize, usize), DataFusionError> {
        let mut length = 0usize;
        let mut ended = false;
        for shift in (0..35).step_by(7) {
            let byte = self
                .protocol
                .read_i8()
                .map_err(crate::catalog::parquet_err)?
                .cast_unsigned();
            if shift == 28 && byte > 15 {
                return Err(refused());
            }
            length |= usize::from(byte & 127) << shift;
            if byte & 128 == 0 {
                ended = true;
                break;
            }
        }
        if !ended {
            return Err(refused());
        }
        let start = self.position()?;
        let end = add(start, length)?;
        if end > self.bytes.len() {
            return Err(refused());
        }
        self.strings = add(self.strings, length)?;
        self.protocol
            .seek(SeekFrom::Start(end as u64))
            .map_err(DataFusionError::IoError)?;
        Ok((start, end))
    }
    fn unsigned(&mut self) -> Result<i32, DataFusionError> {
        let mut value = 0u32;
        for shift in (0..35).step_by(7) {
            let byte = self
                .protocol
                .read_i8()
                .map_err(crate::catalog::parquet_err)?
                .cast_unsigned();
            if shift == 28 && byte > 7 {
                return Err(refused());
            }
            value |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return i32::try_from(value).map_err(|_| refused());
            }
        }
        Err(refused())
    }
    fn element_type(byte: u8) -> Result<TType, DataFusionError> {
        Ok(match byte {
            // Parquet 58.4 accepts both historical compact Bool tags;
            // thrift 0.17's collection decoder accepts only tag 1.
            1 | 2 => TType::Bool,
            3 => TType::I08,
            4 => TType::I16,
            5 => TType::I32,
            6 => TType::I64,
            7 => TType::Double,
            8 => TType::String,
            9 => TType::List,
            10 => TType::Set,
            11 => TType::Map,
            12 => TType::Struct,
            _ => return Err(refused()),
        })
    }
    fn collection(&mut self) -> Result<(TType, i32), DataFusionError> {
        let byte = self
            .protocol
            .read_i8()
            .map_err(crate::catalog::parquet_err)?
            .cast_unsigned();
        let count = if byte >> 4 == 15 {
            self.unsigned()?
        } else {
            i32::from(byte >> 4)
        };
        // The pinned Parquet reader also permits the zero-byte empty list.
        let kind = if byte == 0 {
            TType::I08
        } else {
            Self::element_type(byte & 15)?
        };
        Ok((kind, count))
    }
    fn element(&mut self, kind: TType, depth: usize) -> Result<(), DataFusionError> {
        if kind == TType::Bool {
            // Collection booleans carry their own byte. Struct booleans use
            // read_bool's pending value from the field header instead.
            let byte = self
                .protocol
                .read_i8()
                .map_err(crate::catalog::parquet_err)?;
            if !matches!(byte, 0..=2) {
                return Err(refused());
            }
            Ok(())
        } else {
            self.value(kind, depth)
        }
    }
    fn count(&mut self, count: i32, kind: TType) -> Result<usize, DataFusionError> {
        let count = usize::try_from(count).map_err(|_| refused())?;
        if count > self.bytes.len().saturating_sub(self.position()?) {
            return Err(refused());
        }
        self.slots = add(self.slots, mul(count, slot_bytes(kind)?)?)?;
        Ok(count)
    }
    pub(super) fn structure(
        &mut self,
        depth: usize,
        schema: bool,
    ) -> Result<Option<i32>, DataFusionError> {
        if depth >= 32 {
            return Err(refused());
        }
        self.objects = add(self.objects, 1)?;
        self.protocol
            .read_struct_begin()
            .map_err(crate::catalog::parquet_err)?;
        let mut children = None;
        let mut last = 0;
        loop {
            let field = self.field(&mut last)?;
            if field.field_type == TType::Stop {
                break;
            }
            if schema && field.id == Some(5) && field.field_type == TType::I32 {
                children = Some(
                    self.protocol
                        .read_i32()
                        .map_err(crate::catalog::parquet_err)?,
                );
            } else {
                self.value(field.field_type, depth + 1)?;
            }
            self.protocol
                .read_field_end()
                .map_err(crate::catalog::parquet_err)?;
        }
        self.protocol
            .read_struct_end()
            .map_err(crate::catalog::parquet_err)?;
        Ok(children)
    }
    fn value(&mut self, kind: TType, depth: usize) -> Result<(), DataFusionError> {
        if depth >= 32 {
            return Err(refused());
        }
        let position = self.position()?;
        let p = &mut self.protocol;
        match kind {
            TType::Bool => {
                p.read_bool().map_err(|error| {
                    DataFusionError::Context(
                        format!("node metadata bool at byte {position}, depth {depth}: {error:?}"),
                        Box::new(crate::catalog::parquet_err(error)),
                    )
                })?;
            }
            TType::I08 => {
                p.read_i8().map_err(crate::catalog::parquet_err)?;
            }
            TType::I16 => {
                p.read_i16().map_err(crate::catalog::parquet_err)?;
            }
            TType::I32 => {
                p.read_i32().map_err(crate::catalog::parquet_err)?;
            }
            TType::I64 => {
                p.read_i64().map_err(crate::catalog::parquet_err)?;
            }
            TType::Double => {
                p.read_double().map_err(crate::catalog::parquet_err)?;
            }
            TType::String => {
                self.string()?;
            }
            TType::Struct => {
                self.structure(depth, false)?;
            }
            TType::List | TType::Set => {
                let (element, count) = self.collection()?;
                for _ in 0..self.count(count, element)? {
                    self.element(element, depth + 1)?;
                }
            }
            TType::Map => {
                let count = self.unsigned()?;
                if count > 0 {
                    let types = self
                        .protocol
                        .read_i8()
                        .map_err(crate::catalog::parquet_err)?
                        .cast_unsigned();
                    let key = Self::element_type(types >> 4)?;
                    let value = Self::element_type(types & 15)?;
                    let count = self.count(count, key)?;
                    self.slots = add(self.slots, mul(count, slot_bytes(value)?)?)?;
                    for _ in 0..count {
                        self.element(key, depth + 1)?;
                        self.element(value, depth + 1)?;
                    }
                }
            }
            _ => return Err(refused()),
        }
        Ok(())
    }
}

fn schema_elements(scan: &mut Scan<'_>, flat_fields: Option<usize>) -> Result<(), DataFusionError> {
    let list = scan
        .protocol
        .read_list_begin()
        .map_err(crate::catalog::parquet_err)?;
    if list.element_type != TType::Struct
        || !flat_fields.map_or(matches!(list.size, 6 | 9), |fields| {
            usize::try_from(list.size).ok() == fields.checked_add(1)
        })
    {
        return Err(refused());
    }
    let count = scan.count(list.size, TType::Struct)?;
    let mut children = [0; 9];
    for slot in &mut children[..count] {
        *slot = scan.structure(1, true)?.unwrap_or(0);
    }
    // from_thrift reserves num_children before validating remaining
    // elements. These are the actual legacy/canonical node shapes.
    let mut flat = [0; 9];
    let expected: &[i32] = if let Some(fields) = flat_fields {
        flat[0] = i32::try_from(fields).map_err(|_| refused())?;
        &flat[..count]
    } else if count == 6 {
        &[5, 0, 0, 0, 0, 0]
    } else {
        &[6, 0, 0, 0, 1, 1, 0, 0, 0]
    };
    if &children[..count] != expected {
        return Err(refused());
    }
    scan.protocol
        .read_list_end()
        .map_err(crate::catalog::parquet_err)?;
    Ok(())
}

pub(super) fn footer(bytes: &[u8], flat_fields: Option<usize>) -> Result<Facts, DataFusionError> {
    let mut scan = Scan::new(bytes);
    scan.objects = 1;
    let mut arrow_schema = None;
    let mut saw_schema = false;
    scan.protocol
        .read_struct_begin()
        .map_err(crate::catalog::parquet_err)?;
    let mut previous_field = 0;
    loop {
        let field = scan.field(&mut previous_field)?;
        if field.field_type == TType::Stop {
            break;
        }
        if field.id == Some(2) && field.field_type == TType::List {
            schema_elements(&mut scan, flat_fields)?;
            saw_schema = true;
        } else if field.id == Some(5) && field.field_type == TType::List {
            let (element, count) = scan.collection()?;
            // Parquet accepts a zero-byte empty list without an element type.
            // Nonempty metadata still has to contain KeyValue structs.
            if count != 0 && element != TType::Struct {
                return Err(refused());
            }
            for _ in 0..scan.count(count, TType::Struct)? {
                scan.objects = add(scan.objects, 1)?;
                scan.protocol
                    .read_struct_begin()
                    .map_err(crate::catalog::parquet_err)?;
                let mut key = None;
                let mut value = None;
                let mut previous_field = 0;
                loop {
                    let kv = scan.field(&mut previous_field)?;
                    if kv.field_type == TType::Stop {
                        break;
                    }
                    match (kv.id, kv.field_type) {
                        (Some(1), TType::String) => key = Some(scan.string()?),
                        (Some(2), TType::String) => value = Some(scan.string()?),
                        _ => scan.value(kv.field_type, 1)?,
                    }
                    scan.protocol
                        .read_field_end()
                        .map_err(crate::catalog::parquet_err)?;
                }
                scan.protocol
                    .read_struct_end()
                    .map_err(crate::catalog::parquet_err)?;
                if key.is_some_and(|(start, end)| &bytes[start..end] == b"ARROW:schema") {
                    arrow_schema = value;
                }
            }
            scan.protocol
                .read_list_end()
                .map_err(crate::catalog::parquet_err)?;
        } else {
            scan.value(field.field_type, 0)?;
        }
        scan.protocol
            .read_field_end()
            .map_err(crate::catalog::parquet_err)?;
    }
    scan.protocol
        .read_struct_end()
        .map_err(crate::catalog::parquet_err)?;
    if !saw_schema {
        return Err(refused());
    }
    Ok(Facts {
        objects: scan.objects,
        slots: scan.slots,
        strings: scan.strings,
        arrow_schema,
    })
}
