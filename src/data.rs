use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{RecordBatch, UInt16Builder};
use crate::reader;

pub fn create_record_batch(dicom_reader: reader::DicomReader) -> RecordBatch {
    let mut columns_builder = UInt16Builder::new();
    let mut rows_builder = UInt16Builder::new();
    let mut frames_builder = UInt16Builder::new();

    for dicom_image in dicom_reader.take(3) {
        columns_builder.append_value(dicom_image.columns.try_into().unwrap());
        rows_builder.append_value(dicom_image.rows.try_into().unwrap());
        frames_builder.append_value(dicom_image.frames.try_into().unwrap());
        println!("{:?}", dicom_image);
    }
    let schema = Schema::new(vec![
        Field::new("columns", DataType::UInt16, false),
        Field::new("rows", DataType::UInt16, false),
        Field::new("frames", DataType::UInt16, false),
    ]);
    let record_batch = RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(columns_builder.finish()),
            std::sync::Arc::new(rows_builder.finish()),
            std::sync::Arc::new(frames_builder.finish()),
        ],
    ).unwrap();

    record_batch
}
