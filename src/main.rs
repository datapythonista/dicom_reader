use futures::stream::StreamExt;
use polars::prelude::{LazyFrame, col, lit};
//use polars::prelude::{ParquetWriteOptions, ParquetCompression};
use polars::datatypes::DataType;
use datafusion::prelude::SessionConfig;
use datafusion::execution::context::SessionContext;
use crate::polars_reader::DicomScanner;
mod reader;
mod polars_reader;
mod datafusion_reader;

fn exec_polars_pipeline(path: impl AsRef<std::path::Path>) {
    let q = LazyFrame::scan_dicom(path).unwrap()
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
    let df = q.fetch(2).unwrap();
    println!("polars df: {:?}", df);

    let options = ParquetWriteOptions {
        compression: ParquetCompression::Uncompressed,
        statistics: false,
        row_group_size: None,
        data_pagesize_limit: None,
        maintain_order: false,
    };
    q.sink_parquet("out_dicom.parquet", options).unwrap();
    */
}

async fn exec_datafusion_pipeline(path: impl AsRef<std::path::Path>) {
    let config = SessionConfig::new().with_batch_size(5);
    let ctx = SessionContext::new_with_config(config);
    let dicom_table = datafusion_reader::DicomTableProvider::new(&path);
    ctx.register_table("dicom_table", std::sync::Arc::new(dicom_table))
        .unwrap();

    let plan = ctx.sql("
        SELECT SPLIT_PART(path, '/', -1),
               columns,
               rows,
               frames,
               rows * columns * frames AS total_voxels
        FROM dicom_table
        WHERE modality = 'CT'
        ORDER BY total_voxels DESC
        LIMIT 30;
        "
    ).await.unwrap();

    let mut stream = plan.clone().execute_stream().await.unwrap();
    println!("Stream schema: {}", stream.schema());

    while let Some(batch) = stream.next().await {
        println!("num_rows: {:?}", batch.unwrap().num_rows());
    }

    let result = plan.collect().await.unwrap();
    let pretty_result = arrow::util::pretty::pretty_format_batches(&result)
        .unwrap()
        .to_string();
    println!("{pretty_result}");
}

#[tokio::main]
async fn main() {
    let data_dir = "/home/mgarcia/src/dicom_reader/data/manifest-1677266205028";
    exec_polars_pipeline(&data_dir);
    exec_datafusion_pipeline(&data_dir).await;
}
