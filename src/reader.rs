use std::collections::HashSet;
use std::sync::Arc;
use dicom::pixeldata::PixelDecoder;
use dicom::dictionary_std::tags;
use arrow::datatypes::{Schema, Field, DataType, Int16Type};
use arrow::array::{RecordBatch, ArrayRef, UInt16Builder, StringBuilder, StringDictionaryBuilder, LargeBinaryBuilder};

/// A standard representation of a Dicom image
///
/// This is not standard in the dimensions, but in the bits used to represent the data.
/// All the voxels are represented in 16 bits HU.
pub struct DicomImage {
    pub path: String,
    pub modality: String,
    pub columns: usize,
    pub rows: usize,
    pub frames: usize,
    pub voxels: Vec<i16>,
}
impl DicomImage {
    fn new(files: Vec<&std::path::Path>) -> Self {
        let options = dicom::pixeldata::ConvertOptions::new()
            .with_modality_lut(dicom::pixeldata::ModalityLutOption::None)
            .with_voi_lut(dicom::pixeldata::VoiLutOption::Default)
            .with_bit_depth(dicom::pixeldata::BitDepthOption::Auto);

        let path = files[0].parent().unwrap().to_str().unwrap();
        let first_dicom_file = dicom::object::open_file(files[0]).unwrap();
        let pixel_data = first_dicom_file.decode_pixel_data().unwrap();
        let columns = pixel_data.columns() as usize;
        let rows = pixel_data.rows() as usize;
        let frames = files.len();

        let modality = first_dicom_file.element(tags::MODALITY).unwrap().to_str().unwrap();

        if pixel_data.bits_allocated() != 16 {
            panic!("Only 16 bits pixels are supported. Found bits_allocated={}", pixel_data.bits_allocated());
        }
        if pixel_data.samples_per_pixel() != 1 {
            panic!("Only monochrome files are supported. Found samples_per_pixel={}", pixel_data.samples_per_pixel());
        }

        let mut ct_scan = DicomImage {
            path: path.to_string(),
            modality: modality.to_string(),
            columns,
            rows,
            frames,
            voxels: Vec::with_capacity(columns * rows * frames)
        };

        for current_file in files {
            if let Ok(dicom_file) = dicom::object::open_file(current_file) {
                ct_scan.voxels.extend_from_slice(&dicom_file.decode_pixel_data().unwrap().to_vec_with_options(&options).unwrap());
            }
        }
        ct_scan
    }
}
impl std::fmt::Debug for DicomImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CTScan({}x{}x{})", self.columns, self.rows, self.frames)
    }
}

pub struct DicomReader {
    dicom_directories: Vec<String>,
    index: usize,
}
impl DicomReader {
    pub fn new(path: &str) -> Self {
        let mut dicom_directories = HashSet::new();

        for entry in walkdir::WalkDir::new(path).sort_by_file_name().into_iter().filter_map(|x| x.ok()) {
            if entry.path().extension().is_some() && entry.path().extension().unwrap() == "dcm" {
                dicom_directories.insert(entry.path().parent().unwrap().to_str().unwrap().to_owned());
            }
        }
        DicomReader {
            dicom_directories: dicom_directories.into_iter().collect::<Vec<_>>(),
            index: 0,
        }
    }

    #[allow(dead_code)]
    pub fn to_record_batch(self) -> RecordBatch {
        let mut path_builder = StringBuilder::new();
        let mut modality_builder = StringDictionaryBuilder::<Int16Type>::new();
        let mut columns_builder = UInt16Builder::new();
        let mut rows_builder = UInt16Builder::new();
        let mut frames_builder = UInt16Builder::new();
        let mut voxels_builder = LargeBinaryBuilder::new();

        for dicom_image in self.take(3) {
            path_builder.append_value(dicom_image.path);
            modality_builder.append_value(dicom_image.modality);
            columns_builder.append_value(dicom_image.columns.try_into().unwrap());
            rows_builder.append_value(dicom_image.rows.try_into().unwrap());
            frames_builder.append_value(dicom_image.frames.try_into().unwrap());
            let voxels_bytes: &[u8] = unsafe { std::slice::from_raw_parts(
                dicom_image.voxels.as_ptr() as *const u8,
                dicom_image.voxels.len() * 2,
            ) };
            voxels_builder.append_value(voxels_bytes);
        }
        let schema = Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("modality", DataType::Dictionary(
                                        Box::new(DataType::Int16),
                                        Box::new(DataType::Utf8)),
                       false),
            Field::new("columns", DataType::UInt16, false),
            Field::new("rows", DataType::UInt16, false),
            Field::new("frames", DataType::UInt16, false),
            Field::new("voxels", DataType::LargeBinary, false),
        ]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(path_builder.finish()),
                Arc::new(modality_builder.finish()),
                Arc::new(columns_builder.finish()),
                Arc::new(rows_builder.finish()),
                Arc::new(frames_builder.finish()),
                Arc::new(voxels_builder.finish()),
            ],
        ).unwrap();

        record_batch
    }

    pub fn to_record_batch_with_options(self,
                                        n_rows: Option<usize>,
                                        columns: Option<Vec<&str>>) -> RecordBatch {

        let iterator: Box<dyn Iterator<Item=DicomImage>> = match n_rows {
            Some(num_rows) => { Box::new(self.take(num_rows)) }
            None => { Box::new(self) }
        };


        let (fetch_path,
             fetch_modality,
             fetch_columns,
             fetch_rows,
             fetch_frames,
             fetch_voxels) = if let Some(cols_vec) = columns {
            let known_columns: HashSet<&str> = vec![
                "path",
                "modality",
                "columns",
                "rows",
                "frames",
                "voxels",
            ].into_iter().collect();
            let cols_set: HashSet<&str> = cols_vec.into_iter().collect();
            let diff: HashSet<&str> = cols_set.difference(&known_columns).cloned().collect();
            if !diff.is_empty() {
                panic!("Unknown columns: {:?}", diff);
            }
            (
                cols_set.contains("path"),
                cols_set.contains("modality"),
                cols_set.contains("columns"),
                cols_set.contains("rows"),
                cols_set.contains("frames"),
                cols_set.contains("voxels"),
            )
        } else {
            (true, true, true, true, true, true)
        };

        // Can we avoid creating the builders?
        let mut path_builder = StringBuilder::new();
        let mut modality_builder = StringDictionaryBuilder::<Int16Type>::new();
        let mut columns_builder = UInt16Builder::new();
        let mut rows_builder = UInt16Builder::new();
        let mut frames_builder = UInt16Builder::new();
        let mut voxels_builder = LargeBinaryBuilder::new();

        for dicom_image in iterator {
            if fetch_path {
                path_builder.append_value(dicom_image.path);
            }
            if fetch_modality {
                modality_builder.append_value(dicom_image.modality);
            }
            if fetch_columns {
                columns_builder.append_value(dicom_image.columns.try_into().unwrap());
            }
            if fetch_rows {
                rows_builder.append_value(dicom_image.rows.try_into().unwrap());
            }
            if fetch_frames {
                frames_builder.append_value(dicom_image.frames.try_into().unwrap());
            }
            if fetch_voxels {
                let voxels_bytes: &[u8] = unsafe { std::slice::from_raw_parts(
                    dicom_image.voxels.as_ptr() as *const u8,
                    dicom_image.voxels.len() * 2,
                ) };
                voxels_builder.append_value(voxels_bytes);
            }
        }

        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        if fetch_path {
            fields.push(Field::new("path", DataType::Utf8, false));
            arrays.push(Arc::new(path_builder.finish()));
        }
        if fetch_modality {
            fields.push(Field::new("modality", DataType::Dictionary(
                                                    Box::new(DataType::Int16),
                                                    Box::new(DataType::Utf8)),
                                   false));
            arrays.push(Arc::new(modality_builder.finish()));
        }
        if fetch_columns {
            fields.push(Field::new("columns", DataType::UInt16, false));
            arrays.push(Arc::new(columns_builder.finish()));
        }
        if fetch_rows {
            fields.push(Field::new("rows", DataType::UInt16, false));
            arrays.push(Arc::new(rows_builder.finish()));
        }
        if fetch_frames {
            fields.push(Field::new("frames", DataType::UInt16, false));
            arrays.push(Arc::new(frames_builder.finish()));
        }
        if fetch_voxels {
            fields.push(Field::new("voxels", DataType::LargeBinary, false));
            arrays.push(Arc::new(voxels_builder.finish()));
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

}
impl Iterator for DicomReader {
    type Item = DicomImage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.dicom_directories.len() {
            None
        } else {

            let files = std::fs::read_dir(&self.dicom_directories[self.index]).unwrap();
            let dicom_files = files.filter(|x| x.as_ref().unwrap().path().extension().unwrap() == "dcm")
                                   .map(|x| x.unwrap().path())
                                   .collect::<Vec<_>>();

            self.index += 1;
            Some(DicomImage::new(dicom_files.iter().map(|x| x.as_path()).collect()))
        }
    }
}
