//use arrow::pyarrow::ToPyArrow;
//use pyo3::prelude::*;
// use polars::prelude::{AnonymousScan, AnonymousScanArgs, Schema};
use polars::prelude::{LazyFrame, ScanArgsAnonymous, col, lit};
use polars::datatypes::DataType;
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

    /*
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
    */

    let ldf = LazyFrame::anonymous_scan(
        std::sync::Arc::new(scan::DicomScan::new(&data_dir)),
        ScanArgsAnonymous::default(),
    ).unwrap();

    let df = ldf.filter(col("modality").eq(lit("CT")))
                .with_column((col("rows").cast(DataType::UInt64)
                              * col("columns")
                              * col("frames")
                             ).alias("total_voxels"))
                .filter(col("total_voxels").lt(500 * 500 * 20))
                .select([
                    col("path"),
                    col("modality"),
                    col("total_voxels"),
                    col("rows"),
                    col("columns"),
                    col("frames"),
                ])
                .fetch(30)
                .unwrap();

    println!("result={df:?}");
}
