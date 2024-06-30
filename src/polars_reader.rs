use std::any::Any;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use polars::prelude::{AnonymousScan,
                      AnonymousScanArgs,
                      LazyFrame,
                      DataFrame,
                      Series,
                      PolarsResult,
                      Schema,
                      ArrowSchema,
                      ArrowField,
                      ArrowDataType,
                      ScanArgsAnonymous};
use polars_arrow::datatypes::IntegerType::Int16;
use crate::reader;

pub struct DicomScan {
    path: String,
}

impl DicomScan {
    pub fn new(path: impl AsRef<std::path::Path>) -> Self {
        DicomScan { path: path.as_ref().to_str().unwrap().to_string() }
    }
}

impl AnonymousScan for DicomScan {
    fn as_any(&self) -> &(dyn Any + 'static) {
        self
    }
    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let mut projection: Option<Vec<&str>> = None;

        if let Some(ref columns) = scan_opts.with_columns {
            projection = Some(columns.iter().map(|string| { string.as_str() }).collect());
        }

        let record_batch = reader::DicomStreamer::new(&self.path)
            .with_limit(scan_opts.n_rows)
            .with_projection(projection)
            .to_record_batch(false)
            .unwrap();
        recordbatch_to_polars_dataframe(record_batch)
    }
    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<Arc<Schema>> {
        let schema = Schema::from(ArrowSchema::from(vec![
            ArrowField::new("path", ArrowDataType::Utf8, false),
            ArrowField::new("modality", ArrowDataType::Dictionary(
                                            Int16,
                                            Box::new(ArrowDataType::Utf8),
                                            false), false),
            ArrowField::new("columns", ArrowDataType::UInt16, false),
            ArrowField::new("rows", ArrowDataType::UInt16, false),
            ArrowField::new("frames", ArrowDataType::UInt16, false),
            ArrowField::new("voxels", ArrowDataType::Binary, false),
        ]));
        Ok(Arc::new(schema))
    }
    fn allows_projection_pushdown(&self) -> bool {
        true
    }
}

fn recordbatch_to_polars_dataframe(record_batch: RecordBatch) -> PolarsResult<DataFrame> {
    DataFrame::new(record_batch.columns()
                               .iter()
                               .zip(record_batch.schema().all_fields().iter().map(|field| { field.name().as_str() }))
                               .map(|(arc_dyn_array, col_name)| { (arc_dyn_array.to_data(), col_name) })
                               .map(|(array_data, col_name)| { (polars_arrow::array::from_data(&array_data), col_name) })
                               .map(|(box_dyn_array, col_name)| { Series::try_from((col_name, box_dyn_array)).unwrap() })
                               .collect())

}

pub trait DicomScanner {
    fn scan_dicom(path: impl AsRef<std::path::Path>) -> PolarsResult<LazyFrame> {
        let function = DicomScan::new(path);
        let args = ScanArgsAnonymous::default();

        LazyFrame::anonymous_scan(Arc::new(function), args)
    }
}

impl DicomScanner for LazyFrame {}
