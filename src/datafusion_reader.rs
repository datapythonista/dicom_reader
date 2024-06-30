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
                                RecordBatchStream, DisplayAs, DisplayFormatType, project_schema};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_expr::Expr;
use datafusion::error::DataFusionError;
use crate::reader;

type ResultExecute = Result<Pin<Box<dyn RecordBatchStream<Item = Result<RecordBatch,
                                                                        DataFusionError>> + Send>>,
                            DataFusionError>;

#[derive(Debug)]
struct DicomExecutionPlan {
    path: PathBuf,
    properties: PlanProperties,
    limit: Option<usize>,
}

impl DicomExecutionPlan {
    fn new(path: impl AsRef<Path>,
           schema: Arc<Schema>,
           projection: Option<&Vec<usize>>,
           limit: Option<usize>) -> Self {

        let projected_schema = project_schema(&schema, projection).unwrap();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        DicomExecutionPlan {
            path: path.as_ref().to_path_buf(),
            properties,
            limit,
        }
    }
}

impl DisplayAs for DicomExecutionPlan {
    fn fmt_as(&self,
              _t: DisplayFormatType,
              f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DicomExecutionPlan")
    }
}


impl ExecutionPlan for DicomExecutionPlan {
    fn execute(&self,
               _partition: usize,
               context: Arc<TaskContext>) -> ResultExecute {

        let columns = self.properties
                          .equivalence_properties()
                          .schema()
                          .fields
                          .into_iter()
                          .map(|f| f.name().to_string())
                          .collect::<Vec<_>>();

        let columns_str = columns.iter()
                                 .map(|c| c.as_str())
                                 .collect::<Vec<_>>();

        let batch_size = context.session_config().batch_size();

        let record_batch_streamer = RecordBatchStreamAdapter::new(
            self.properties.equivalence_properties().schema().clone(),
            reader::DicomStreamer::new(&self.path)
                .with_projection(Some(columns_str))
                .with_limit(self.limit)
                .with_batch_size(Some(batch_size)),
        );
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
        Ok(Arc::new(DicomExecutionPlan::new(&self.path,
                                            self.schema(),
                                            projection,
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
