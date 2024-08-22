use crate::error::Error;
use crate::Variant;
use std::fs::File;

pub fn read_parquet(path: File) -> Result<Variant<'static>, Error> {
    Ok(Variant::default())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::json::to_json;
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    pub fn test_read() -> Result<(), Error> {
        let path = File::open("...").expect("Bad File");

        let builder = ParquetRecordBatchReaderBuilder::try_new(path)?;
        let mut reader = builder.build()?;

        while let Some(Ok(row)) = reader.next() {
            let vd = row.column_by_name("value").expect("No Struct").as_struct();
            let metadata = vd
                .column_by_name("metadata")
                .expect("No Struct")
                .as_binary::<i32>();
            let values = vd
                .column_by_name("value")
                .expect("No Struct")
                .as_binary::<i32>();

            for i in 0..row.num_rows() {
                let variant = Variant {
                    metadata: metadata.value(i),
                    value: values.value(i),
                    pos: 0,
                };
                println!("{}", to_json(variant)?);
            }
        }
        Ok(())
    }
}
