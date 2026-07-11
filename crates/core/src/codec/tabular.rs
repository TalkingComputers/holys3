use super::{DecodeWriter, DocumentBody};
use anyhow::{Context, Result as AnyhowResult};

pub(super) fn parquet_to_json_lines(
    key: &str,
    bytes: bytes::Bytes,
    limit: Option<u64>,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let context = || format!("parquet decode failed for {key}");
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .with_context(context)?
        .build()
        .with_context(context)?;
    let mut writer = arrow_json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json::writer::LineDelimited>(DecodeWriter::new(
            key,
            limit,
            0,
            memory_limit,
        ));
    for batch in reader {
        writer
            .write(&batch.with_context(context)?)
            .with_context(context)?;
    }
    writer.finish().with_context(context)?;
    writer.into_inner().finish()
}

fn finite_floats(value: apache_avro::types::Value) -> apache_avro::types::Value {
    use apache_avro::types::Value;
    match value {
        Value::Float(f) if !f.is_finite() => Value::Null,
        Value::Double(d) if !d.is_finite() => Value::Null,
        Value::Union(branch, inner) => Value::Union(branch, Box::new(finite_floats(*inner))),
        Value::Array(items) => Value::Array(items.into_iter().map(finite_floats).collect()),
        Value::Map(entries) => Value::Map(
            entries
                .into_iter()
                .map(|(k, v)| (k, finite_floats(v)))
                .collect(),
        ),
        Value::Record(fields) => Value::Record(
            fields
                .into_iter()
                .map(|(name, v)| (name, finite_floats(v)))
                .collect(),
        ),
        other => other,
    }
}

fn decimal_text(unscaled: &num_bigint::BigInt, scale: usize) -> String {
    let raw = unscaled.to_string();
    let (sign, digits) = match raw.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", raw.as_str()),
    };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = if digits.len() <= scale {
        format!("{}{digits}", "0".repeat(scale + 1 - digits.len()))
    } else {
        digits.to_owned()
    };
    let point = padded.len() - scale;
    format!("{sign}{}.{}", &padded[..point], &padded[point..])
}

fn scaled_decimals(
    value: apache_avro::types::Value,
    schema: &apache_avro::Schema,
    names: &std::collections::HashMap<apache_avro::schema::Name, &apache_avro::Schema>,
) -> AnyhowResult<apache_avro::types::Value> {
    use apache_avro::types::Value;
    use apache_avro::Schema;
    Ok(match (value, schema) {
        (value, Schema::Ref { name }) => {
            let schema = names
                .get(name)
                .with_context(|| format!("avro schema reference {name} unresolved"))?;
            scaled_decimals(value, schema, names)?
        }
        (Value::Decimal(decimal), Schema::Decimal(spec)) => {
            let unscaled: num_bigint::BigInt = decimal.into();
            Value::String(decimal_text(&unscaled, spec.scale))
        }
        (Value::Record(fields), Schema::Record(spec)) => Value::Record(
            fields
                .into_iter()
                .zip(&spec.fields)
                .map(|((name, value), field)| {
                    anyhow::ensure!(
                        name == field.name,
                        "avro record field order diverged from schema"
                    );
                    Ok((name, scaled_decimals(value, &field.schema, names)?))
                })
                .collect::<AnyhowResult<_>>()?,
        ),
        (Value::Array(items), Schema::Array(spec)) => Value::Array(
            items
                .into_iter()
                .map(|item| scaled_decimals(item, &spec.items, names))
                .collect::<AnyhowResult<_>>()?,
        ),
        (Value::Map(entries), Schema::Map(spec)) => Value::Map(
            entries
                .into_iter()
                .map(|(key, value)| Ok((key, scaled_decimals(value, &spec.types, names)?)))
                .collect::<AnyhowResult<_>>()?,
        ),
        (Value::Union(branch, inner), Schema::Union(spec)) => {
            let variant = spec
                .variants()
                .get(branch as usize)
                .with_context(|| format!("avro union branch {branch} out of range"))?;
            Value::Union(branch, Box::new(scaled_decimals(*inner, variant, names)?))
        }
        (value, _) => value,
    })
}

pub(super) fn avro_to_json_lines(
    key: &str,
    bytes: &[u8],
    limit: Option<u64>,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    let context = || format!("avro decode failed for {key}");
    let reader = apache_avro::Reader::new(bytes).with_context(context)?;
    let schema = reader.writer_schema().clone();
    let resolved = apache_avro::schema::ResolvedSchema::try_from(&schema).with_context(context)?;
    let mut out = DecodeWriter::new(key, limit, 0, memory_limit);
    for (records, value) in reader.enumerate() {
        let value = match value {
            Ok(value) => value,
            Err(err) if !out.is_empty() => {
                eprintln!(
                    "warning: {key}: avro stream ends in garbage ({err}); \
                     searching the {} records that decoded",
                    records
                );
                return out.finish();
            }
            Err(err) => return Err(anyhow::Error::new(err)).with_context(context),
        };
        let value = scaled_decimals(value, &schema, resolved.get_names()).with_context(context)?;
        let json = serde_json::Value::try_from(finite_floats(value)).with_context(context)?;
        let json = json.to_string();
        out.append(json.as_bytes())?;
        out.append(b"\n")?;
    }
    out.finish()
}

fn arrow_batches_to_json_lines(
    key: &str,
    limit: Option<u64>,
    memory_limit: usize,
    batches: impl Iterator<Item = Result<arrow_array::RecordBatch, arrow_schema::ArrowError>>,
) -> AnyhowResult<DocumentBody> {
    let context = || format!("Arrow IPC decode failed for {key}");
    let mut writer = arrow_json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json::writer::LineDelimited>(DecodeWriter::new(
            key,
            limit,
            0,
            memory_limit,
        ));
    for batch in batches {
        writer
            .write(&batch.with_context(context)?)
            .with_context(context)?;
    }
    writer.finish().with_context(context)?;
    writer.into_inner().finish()
}

pub(super) fn arrow_ipc_to_json_lines(
    key: &str,
    bytes: bytes::Bytes,
    limit: Option<u64>,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    let reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)
        .with_context(|| format!("Arrow IPC decode failed for {key}"))?;
    arrow_batches_to_json_lines(key, limit, memory_limit, reader)
}

pub(super) fn arrow_ipc_stream_to_json_lines(
    key: &str,
    bytes: bytes::Bytes,
    limit: Option<u64>,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .with_context(|| format!("Arrow IPC stream decode failed for {key}"))?;
    arrow_batches_to_json_lines(key, limit, memory_limit, reader)
}

pub(super) fn orc_to_json_lines(
    key: &str,
    bytes: bytes::Bytes,
    limit: Option<u64>,
    memory_limit: usize,
) -> AnyhowResult<DocumentBody> {
    let context = || format!("ORC decode failed for {key}");
    let reader = orc_rust::ArrowReaderBuilder::try_new(bytes)
        .with_context(context)?
        .build();
    let mut writer = arrow_json_58::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json_58::writer::LineDelimited>(DecodeWriter::new(
            key,
            limit,
            0,
            memory_limit,
        ));
    for batch in reader {
        writer
            .write(&batch.with_context(context)?)
            .with_context(context)?;
    }
    writer.finish().with_context(context)?;
    writer.into_inner().finish()
}
