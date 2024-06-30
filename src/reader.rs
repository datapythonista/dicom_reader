use std::collections::HashSet;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::Stream;
use dicom::pixeldata::PixelDecoder;
use dicom::dictionary_std::tags;
use arrow::datatypes::{Schema, Field, DataType, Int16Type};
use arrow::array::{RecordBatch, ArrayRef, UInt16Builder, StringBuilder, StringDictionaryBuilder, LargeBinaryBuilder};
use datafusion::error::DataFusionError;

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
    files: Vec<std::path::PathBuf>,
}
impl DicomImage {
    fn new(files: Vec<impl AsRef<std::path::Path>>) -> Self {
        let first_file = files[0].as_ref();

        let path = first_file.parent().unwrap().to_str().unwrap();
        let first_dicom_file = dicom::object::open_file(first_file).unwrap();
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

        DicomImage {
            path: path.to_string(),
            modality: modality.to_string(),
            columns,
            rows,
            frames,
            files: files.iter().map(|x| x.as_ref().to_path_buf()).collect(),
        }
    }
    fn voxels(&self) -> Vec<i16> {
        let mut result = Vec::with_capacity(self.columns * self.rows * self.frames);

        let options = dicom::pixeldata::ConvertOptions::new()
            .with_modality_lut(dicom::pixeldata::ModalityLutOption::None)
            .with_voi_lut(dicom::pixeldata::VoiLutOption::Default)
            .with_bit_depth(dicom::pixeldata::BitDepthOption::Auto);

        for current_file in self.files.iter() {
            if let Ok(dicom_file) = dicom::object::open_file(current_file) {
                result.extend_from_slice(&dicom_file.decode_pixel_data().unwrap().to_vec_with_options(&options).unwrap());
            }
        }
        result
    }
}
impl std::fmt::Debug for DicomImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CTScan({}x{}x{})", self.columns, self.rows, self.frames)
    }
}

pub struct DicomReader {
    dicom_directories: Vec<String>,
}
impl DicomReader {
    pub fn new(path: impl AsRef<std::path::Path>) -> Self {
        let mut dicom_directories = HashSet::new();

        for entry in walkdir::WalkDir::new(path).sort_by_file_name().into_iter().filter_map(|x| x.ok()) {
            if entry.path().extension().is_some() && entry.path().extension().unwrap() == "dcm" {
                dicom_directories.insert(entry.path().parent().unwrap().to_str().unwrap().to_owned());
            }
        }
        DicomReader {
            dicom_directories: dicom_directories.into_iter().collect::<Vec<_>>(),
        }
    }

    pub fn iter(&self) -> DicomIter {
        DicomIter {
            dicom_reader: self,
            index: 0,
        }
    }

}

impl<'a> IntoIterator for DicomReader {
    type Item = DicomImage;
    type IntoIter = DicomReaderIterator;

    fn into_iter(self) -> Self::IntoIter {
        DicomReaderIterator {
            dicom_reader: self,
            index: 0,
        }
    }
}

pub struct DicomIter<'a> {
    dicom_reader: &'a DicomReader,
    index: usize,
}

impl Iterator for DicomIter<'_> {
    type Item = DicomImage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.dicom_reader.dicom_directories.len() {
            None
        } else {

            let files = std::fs::read_dir(&self.dicom_reader.dicom_directories[self.index]).unwrap();
            let dicom_files = files.filter(|x| x.as_ref().unwrap().path().extension().unwrap() == "dcm")
                                   .map(|x| x.unwrap().path())
                                   .collect::<Vec<_>>();

            self.index += 1;
            Some(DicomImage::new(dicom_files.iter().map(|x| x.as_path()).collect()))
        }
    }
}

pub struct DicomReaderIterator {
    dicom_reader: DicomReader,
    index: usize,
}

impl Iterator for DicomReaderIterator {
    type Item = DicomImage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.dicom_reader.dicom_directories.len() {
            None
        } else {

            let files = std::fs::read_dir(&self.dicom_reader.dicom_directories[self.index]).unwrap();
            let dicom_files = files.filter(|x| x.as_ref().unwrap().path().extension().unwrap() == "dcm")
                                   .map(|x| x.unwrap().path())
                                   .collect::<Vec<_>>();

            self.index += 1;
            Some(DicomImage::new(dicom_files.iter().map(|x| x.as_path()).collect()))
        }
    }
}

pub struct DicomStreamer {
    row_iterator: DicomReaderIterator,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    remaining_limit: Option<usize>,
    batch_size: usize,
}

impl DicomStreamer {
    pub fn new(path: impl AsRef<std::path::Path>) -> Self {
        let reader = DicomReader::new(path);
        DicomStreamer {
            row_iterator: reader.into_iter(),
            projection: None,
            limit: None,
            remaining_limit: None,
            batch_size: 3,
        }
    }

    pub fn with_projection(mut self, projection: Option<Vec<&str>>) -> Self {
        self.projection = projection.map(|vec| {
            vec.into_iter()
               .map(|x| x.to_string())
               .collect()
        });
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self.remaining_limit = limit;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn to_record_batch(&mut self, streaming: bool) -> Option<RecordBatch> {
        let mut batch_iterator: Box<dyn Iterator<Item=DicomImage>> = Box::new(&mut self.row_iterator);

        let rows_to_fetch = if let Some(limit) = self.remaining_limit {
            if limit < self.batch_size {
                limit
            } else {
                (*self).remaining_limit = Some(limit - self.batch_size);
                self.batch_size
            }
        } else {
            self.batch_size
        };

        if streaming {
            batch_iterator = Box::new(batch_iterator.take(rows_to_fetch));
        };

        let (mut fetch_path,
             mut fetch_modality,
             mut fetch_columns,
             mut fetch_rows,
             mut fetch_frames,
             mut fetch_voxels) = (true, true, true, true, true, true);

        if let Some(ref columns) = self.projection {
            let columns_set: HashSet<&str> = columns.into_iter()
                                                    .map(|x| x.as_str())
                                                    .collect();

            let known_columns = vec!["path", "modality", "columns", "rows", "frames", "voxels"]
                .into_iter()
                .collect::<HashSet<_>>();
            let unknown_columns = columns_set.difference(&known_columns).collect::<Vec<_>>();
            if !unknown_columns.is_empty() {
                panic!("Unknown columns: {:?}", unknown_columns);
            }
            fetch_path = columns_set.contains("path");
            fetch_modality = columns_set.contains("modality");
            fetch_columns = columns_set.contains("columns");
            fetch_rows = columns_set.contains("rows");
            fetch_frames = columns_set.contains("frames");
            fetch_voxels = columns_set.contains("voxels");
        }

        // Can we avoid creating the builders?
        let mut path_builder = StringBuilder::new();
        let mut modality_builder = StringDictionaryBuilder::<Int16Type>::new();
        let mut columns_builder = UInt16Builder::new();
        let mut rows_builder = UInt16Builder::new();
        let mut frames_builder = UInt16Builder::new();
        let mut voxels_builder = LargeBinaryBuilder::new();

        let mut empty_iterator = true;

        for dicom_image in batch_iterator {
            empty_iterator = false;

            if fetch_path {
                path_builder.append_value(dicom_image.path.clone());
            }
            if fetch_modality {
                modality_builder.append_value(dicom_image.modality.clone());
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
                let voxels = dicom_image.voxels();
                let voxels_bytes: &[u8] = unsafe { std::slice::from_raw_parts(
                    voxels.as_ptr() as *const u8,
                    voxels.len() * 2,
                ) };
                voxels_builder.append_value(voxels_bytes);
            }
        }

        if empty_iterator {
            return None;
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

        Some(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap())
    }
}

impl Stream for DicomStreamer {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        let self_unpinned = Pin::get_mut(self);
        let result = self_unpinned.to_record_batch(true);

        if let Some(record_batch) = result {
            Poll::Ready(Some(Ok(record_batch)))
        } else {
            Poll::Ready(None)
        }
    }
}
