import urllib2
import re

from fabric.api import run
from fabric.contrib import files

from buildvm.utils import fail_gracefully, cmd

BASE_URL = 'http://files.innogames.de/'

run = fail_gracefully(run)

def get_images():
    try:
        image_html = urllib2.urlopen(BASE_URL, timeout=2).read()
    except urllib2.URLError:
        return None

    return re.findall(r'<a\s+href="(.+?)\.tar\.gz"', image_html)

def download_image(image):
    url = BASE_URL + image
    if not files.exists(image):
        run(cmd('wget {0}', url))

def extract_image(image, target_dir):
    run(cmd('tar xfz {0} -C {1}', image, target_dir))
