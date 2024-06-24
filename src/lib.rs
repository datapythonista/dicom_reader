mod reader;
mod polars_reader;
#[cfg(feature = "python")]
mod pyarrow_reader;

#[cfg(feature = "python")]
pub use pyarrow_reader::{read_dicom_to_pandas, read_dicom_to_polars, dicom_reader};

pub use polars_reader::DicomScanner;
