use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties, Partitioning, ExecutionMode,
                                RecordBatchStream, DisplayAs, DisplayFormatType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_expr::Expr;
use datafusion::error::DataFusionError;
use crate::reader;

#[derive(Debug)]
struct DicomExecutionPlan {
    path: PathBuf,
    properties: PlanProperties,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
}

impl DicomExecutionPlan {
    fn new(path: impl AsRef<Path>,
           schema: Arc<Schema>,
           projection: Option<Vec<usize>>,
           limit: Option<usize>) -> Self {
        let eq_properties = EquivalenceProperties::new(schema);
        let partitioning = Partitioning::UnknownPartitioning(1);
        let execution_mode = ExecutionMode::Bounded;
        let properties = PlanProperties::new(eq_properties, partitioning, execution_mode);

        DicomExecutionPlan {
            path: path.as_ref().to_path_buf(),
            properties,
            projection,
            limit,
        }
    }
}

impl DisplayAs for DicomExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DicomExecutionPlan")
    }
}

type ResultExecute = Result<Pin<Box<dyn RecordBatchStream<Item = Result<RecordBatch,
                                                                        DataFusionError>> + Send>>,
                            DataFusionError>;

impl ExecutionPlan for DicomExecutionPlan {
    fn execute(&self,
               _partition: usize,
               _context: Arc<TaskContext>) -> ResultExecute {

        let proj: Option<Vec<String>> = match self.projection {
            Some(ref proj_indices) => {
                Some(self.schema()
                         .project(proj_indices)?
                         .fields
                         .iter()
                         .map(|f| f.name().to_string())
                         .collect())
            }
            None => None
        };
        let proj_str: Option<Vec<&str>> = if let Some(ref proj_vec) = proj {
            Some(proj_vec.iter().map(|item| item.as_str()).collect())
        } else {
            None
        };

        let record_batch = reader::DicomReader::new(&self.path)
            .to_record_batch_with_options(self.limit, proj_str);

        let record_batch_streamer = MemoryStream::try_new(
            vec![record_batch],
            self.properties.equivalence_properties().schema().clone(),
            self.projection.clone(),
        )?;
        Ok(Box::pin(record_batch_streamer))
    }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }
    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }
}

pub struct DicomTableProvider {
    path: PathBuf,
}

impl DicomTableProvider {
    pub fn new(path: impl AsRef<Path>) -> Self {
        DicomTableProvider { path: path.as_ref().to_path_buf() }
    }
}

#[async_trait]
impl TableProvider for DicomTableProvider {
    async fn scan(&self,
                  _state: &SessionState,
                  projection: Option<&Vec<usize>>,
                  _filters: &[Expr],
                  limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        println!("limit: {:?}", limit);
        println!("projection: {:?}", projection);

        Ok(Arc::new(DicomExecutionPlan::new(&self.path,
                                            self.schema(),
                                            projection.cloned(),
                                            limit)))
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }
    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("modality", DataType::Dictionary(
                                        Box::new(DataType::Int16),
                                        Box::new(DataType::Utf8)),
                       false),
            Field::new("columns", DataType::UInt16, false),
            Field::new("rows", DataType::UInt16, false),
            Field::new("frames", DataType::UInt16, false),
            Field::new("voxels", DataType::LargeBinary, false),
        ]))
    }
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }
}
