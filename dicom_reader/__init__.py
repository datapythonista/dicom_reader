import ctypes
import json
import pathlib


def call_dicom_reader(data_directory, lib_name):
    args = {'directory_path': str(data_directory)}
    args_str = json.dumps(args)
    args_buffer = args_str.encode('utf-8')
    args_ptr = ctypes.c_char_p(args_buffer)

    lib = ctypes.cdll.LoadLibrary(lib_name)
    lib.read_dicom(args_ptr)


if __name__ == '__main__':
    data_directory = pathlib.Path(__file__).resolve().parent.parent / 'data'
    call_dicom_reader(data_directory, lib_name='libdicom_reader.so')
