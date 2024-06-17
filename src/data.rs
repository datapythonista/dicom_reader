use arrow::datatypes::{Schema, Field, DataType, Int16Type};
use arrow::array::{RecordBatch, UInt16Builder, StringBuilder, StringDictionaryBuilder, LargeBinaryBuilder};
use crate::reader;

pub fn create_record_batch(dicom_reader: reader::DicomReader) -> RecordBatch {
    let mut path_builder = StringBuilder::new();
    let mut modality_builder = StringDictionaryBuilder::<Int16Type>::new();
    let mut columns_builder = UInt16Builder::new();
    let mut rows_builder = UInt16Builder::new();
    let mut frames_builder = UInt16Builder::new();
    // let mut voxels_builder = LargeBinaryBuilder::new();

    for dicom_image in dicom_reader.take(10) {
        path_builder.append_value(dicom_image.path);
        modality_builder.append_value(dicom_image.modality);
        columns_builder.append_value(dicom_image.columns.try_into().unwrap());
        rows_builder.append_value(dicom_image.rows.try_into().unwrap());
        frames_builder.append_value(dicom_image.frames.try_into().unwrap());
        // voxels_builder.append_value(dicom_image.voxels.as_bytes());  // `.as_bytes()` is nightly
    }
    let schema = Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("modality", DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)), false),
        Field::new("columns", DataType::UInt16, false),
        Field::new("rows", DataType::UInt16, false),
        Field::new("frames", DataType::UInt16, false),
        // Field::new("voxels", DataType::Binary, false),
    ]);
    let record_batch = RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(path_builder.finish()),
            std::sync::Arc::new(modality_builder.finish()),
            std::sync::Arc::new(columns_builder.finish()),
            std::sync::Arc::new(rows_builder.finish()),
            std::sync::Arc::new(frames_builder.finish()),
            // std::sync::Arc::new(voxels_builder.finish()),
        ],
    ).unwrap();

    record_batch
}
