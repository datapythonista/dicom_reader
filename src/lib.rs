use pyo3::prelude::*;
use arrow::pyarrow::ToPyArrow;
mod reader;

fn read_dicom(path: &str, with_opts: bool) -> Py<PyAny> {
    Python::with_gil(|py| {
        if with_opts {
            reader::DicomReader::new(path).to_record_batch_with_options(None, None)
                                          .to_pyarrow(py)
                                          .unwrap()
        } else {
            reader::DicomReader::new(path).to_record_batch()
                                          .to_pyarrow(py)
                                          .unwrap()
        }
    })
}

#[pyfunction]
#[pyo3(name="read_dicom")]
fn read_dicom_to_pandas(path: &str) -> PyResult<Py<PyAny>> {
    let pyarrow_record_batch = read_dicom(path, false);
    Python::with_gil(|py| {
        let pandas_dataframe = pyarrow_record_batch.call_method0(py, "to_pandas").unwrap();
        Ok(pandas_dataframe)
    })
}

#[pyfunction]
#[pyo3(name="read_dicom")]
fn read_dicom_to_polars(path: &str) -> PyResult<Py<PyAny>> {
    let pyarrow_record_batch = read_dicom(path, true);
    Python::with_gil(|py| {
        let polars = Python::import_bound(py, "polars").unwrap();
        let from_arrow = polars.getattr("from_arrow").unwrap();
        let args = (pyarrow_record_batch,);
        let polars_dataframe = from_arrow.call1(args).unwrap().into();
        Ok(polars_dataframe)
    })
}

#[pymodule]
fn dicom_reader(_module: &Bound<'_, PyModule>) -> PyResult<()> {
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
