use std::fs;
use std::path::PathBuf;
use std::ffi::{OsStr, OsString};


pub fn iter_directory(directory_path: &str, file_extensions: Vec<OsString>) -> Vec<PathBuf> {
    let mut result = vec![];
    for fname in fs::read_dir(&directory_path).unwrap() {  // TODO unwrap
        let path = fname.unwrap().path();  // TODO unwrap
        let fname_extension = path.extension().unwrap_or(OsStr::new(""));
        if file_extensions.contains(&fname_extension.to_os_string()) {
            result.push(path);
        };
    }
    result
}
