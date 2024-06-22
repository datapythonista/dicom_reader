//use arrow::pyarrow::ToPyArrow;
//use pyo3::prelude::*;
use polars::prelude::{LazyFrame, AnonymousScan, AnonymousScanArgs, Schema,
                      ScanArgsAnonymous, col, lit};
mod reader;
mod scan;

fn main() {
    //let data_dir = "/home/mgarcia/src/dicom_reader/data/manifest-1684259732535";
    let data_dir = "/home/mgarcia/src/dicom_reader/data/manifest-1677266205028";
    //let data_dir = "/home/mgarcia/src/dicom_reader/data/tciaDownload";

    /*
    let record_batch = reader::DicomReader::new(data_dir).to_record_batch();
    println!("{record_batch:?}");

    let record_batch_opts = reader::DicomReader::new(data_dir).to_record_batch_with_options(None, None);
    println!("{record_batch_opts:?}");
    */


    /*
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let pyarrow_record_batch = record_batch.to_pyarrow(py).unwrap();
        println!("{pyarrow_record_batch:?}");
    });
    */

    let scan_opts = AnonymousScanArgs {
        n_rows: Some(2),
        with_columns: Some(std::sync::Arc::new(vec!["path".to_string(),
                                                    "modality".to_string()])),
        schema: std::sync::Arc::new(Schema::new()),
        output_schema: None,
        predicate: None,
    };
    let dataframe = scan::DicomScan::new(data_dir).scan(scan_opts);
    println!("dataframe={dataframe:?}");

    let ldf = LazyFrame::anonymous_scan(
        std::sync::Arc::new(scan::DicomScan::new(&data_dir)),
        ScanArgsAnonymous::default(),
    ).unwrap();

    let df = ldf.select([col("path"), col("modality"), col("frames")])
                //.filter(col("frames").gt(30))
                .filter(col("modality").eq(lit("PT")))
                .limit(5)
                .collect();

    println!("result={df:?}");
}
