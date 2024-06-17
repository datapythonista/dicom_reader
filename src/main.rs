use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
mod reader;
mod data;

fn main() {
    //let data_dir = "/home/mgarcia/src/dicom_reader/data/manifest-1684259732535";
    let data_dir = "/home/mgarcia/src/dicom_reader/data/manifest-1677266205028";
    //let data_dir = "/home/mgarcia/src/dicom_reader/data/tciaDownload";
    let dicom_reader = reader::DicomReader::new(data_dir);

    let record_batch = data::create_record_batch(dicom_reader);
    println!("{record_batch:?}");

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let pyarrow_record_batch = record_batch.to_pyarrow(py).unwrap();
        println!("{pyarrow_record_batch:?}");
    });
}
