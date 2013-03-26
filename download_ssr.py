import urllib
import zipfile
import os
import config
import tempfile


def get_ssr_data():

    saved_filename, headers = urllib.urlretrieve(config.base_url + "stedsnavn.geojson.zip")

    zfile = zipfile.ZipFile(saved_filename)

    handle, filename = tempfile.mkstemp(suffix='.geojson')

    file = open(filename, "w")
    file.write(zfile.read("stedsnavn.geojson"))
    file.close()
    os.remove(saved_filename)
    return filename