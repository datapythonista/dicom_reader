use polars::prelude::{LazyFrame, col, lit};
use polars::prelude::{ParquetWriteOptions, ParquetCompression};
use polars::datatypes::DataType;
use crate::scan::DicomScanner;
mod reader;
mod scan;

fn main() {
    let data_dir = "/home/mgarcia/src/dicom_reader/data/manifest-1677266205028";

    let q = LazyFrame::scan_dicom(&data_dir).unwrap()
                .with_streaming(true)
                .filter(col("modality").eq(lit("CT")))
                .with_column((col("rows").cast(DataType::UInt64)
                              * col("columns")
                              * col("frames")
                             ).alias("total_voxels"))
                .filter(col("total_voxels").lt(500 * 500 * 50))
                .select([
                    col("path"),
                    col("modality"),
                    col("total_voxels"),
                    col("rows"),
                    col("columns"),
                    col("frames"),
                ]);

    let plan = q.explain(false).unwrap();
    println!("{}", plan);

    /*
    let df = q.fetch(20)
              .unwrap();

    println!("result={df:?}");
    */

    let path = std::path::Path::new("dicom_manifest_1677266205028.parquet").to_path_buf();
    let options = ParquetWriteOptions {
        compression: ParquetCompression::Uncompressed,
        statistics: false,
        row_group_size: None,
        data_pagesize_limit: None,
        maintain_order: false,
    };
    q.sink_parquet(path, options).unwrap();
}
