
import ctypes
import json
import pathlib


def call_dicom_reader(data_directory, lib_name):
    args = {'directory_path': str(data_directory)}
    args_str = json.dumps(args)
    args_buffer = args_str.encode('utf-8')
    args_ptr = ctypes.c_char_p(args_buffer)

    lib = ctypes.CDLL(lib_name)  ## changed from ctypes.dll.LoadLibrary --> ctypes.CDLL for MacOS
    lib.read_dicom(args_ptr)


if __name__ == '__main__':
    data_directory = pathlib.Path(__file__).resolve().parent.parent / 'data'
    call_dicom_reader(data_directory, lib_name='libdicom_reader.dylib')   ## changed from libdicom_reader.so --> libdicom_reader.dylib for MacOS
