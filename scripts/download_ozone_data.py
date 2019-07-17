#!/Users/Lucien/anaconda/envs/tdi/bin/python

import os
import zipfile
import requests

from global_vbls import data_path, db_file

def unzip_file(path_to_file, destination, new_name):
    """
    Function for unzipping a zip file.

    PARAMETERS:
    ***********

    INPUTS:
    path_to_file = file_path/file_name to zip file.
    destionation = destination directory for zip file's contents.

    OUTPUTS:
    None
    """

    files_in_dir_before = os.listdir(destination)
    zip_ref = zipfile.ZipFile(path_to_file, 'r')
    zip_ref.extractall(destination)
    zip_ref.close()
    files_in_dir_after = os.listdir(destination)

    unzipped_dir = [di for di in files_in_dir_after if di not in files_in_dir_before]
    if len(unzipped_dir) == 1:
        os.rename(os.path.join(destination, unzipped_dir[0]), os.path.join(destination, new_name))

    return


def make_directory(dir_path):
    try:
        os.mkdir(dir_path)
    except FileExistsError:
        print('directory already exists: ' + dir_path)
    return None


def download_file_http(url, final_dest, final_name):
    r = requests.get(url)
    with open(os.path.join(final_dest, final_name), 'wb') as f:
        f.write(r.content)
    return final_dest, final_name


def download_list_of_zipfiles(url_list):
    final_filenames = []
    for url in url_list:
        final_dest, zip_name = download_file_http(url, data_path, url.split('/')[-1])
        final_filenames.append(zip_name.split('.')[0] + '.csv')
        unzip_file(os.path.join(final_dest, zip_name), final_dest, final_filenames[-1])
        os.remove(os.path.join(final_dest, zip_name))
    return final_filenames


if __name__ == '__main__':
    st_year, ed_year = 2003, 2019

    make_directory(data_path)

    o3_urls = ['https://aqs.epa.gov/aqsweb/airdata/hourly_44201_' + str(year) + '.zip'
               for year in range(st_year, ed_year + 1)]
    o3_files = download_list_of_zipfiles(o3_urls)
