use pyo3::prelude::*;
use arrow::pyarrow::ToPyArrow;
use crate::reader;

#[pyfunction]
#[pyo3(name="read_dicom")]
pub fn read_dicom_to_pandas(path: &str) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| {
        Ok(reader::DicomReader::new(path).to_record_batch()
                                         .to_pyarrow(py)
                                         .unwrap()
                                         .call_method0(py, "to_pandas")
                                         .unwrap())
    })
}

#[pyfunction]
#[pyo3(name="read_dicom")]
pub fn read_dicom_to_polars(path: &str) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| {
        let pyarrow_record_batch = reader::DicomReader::new(path).to_record_batch()
                                                                 .to_pyarrow(py)
                                                                 .unwrap();
        Ok(Python::import_bound(py, "polars").unwrap()
                                             .getattr("from_arrow")
                                             .unwrap()
                                             .call1((pyarrow_record_batch,))
                                             .unwrap()
                                             .into())
    })
}

#[pymodule]
pub fn dicom_reader(_module: &Bound<'_, PyModule>) -> PyResult<()> {
    Python::with_gil(|py| {
        if let Ok(pandas) = Python::import_bound(py, "pandas") {
            pandas.add_function(wrap_pyfunction!(read_dicom_to_pandas, &pandas).unwrap()).unwrap();
        }

        if let Ok(polars) = Python::import_bound(py, "polars") {
            polars.add_function(wrap_pyfunction!(read_dicom_to_polars, &polars).unwrap()).unwrap();
        }
    });
    Ok(())
}
