import logging
import urllib2

from fabric.api import run
from fabric.contrib import files

from igvm.settings import (
    FOREMAN_IMAGE_URL,
    FOREMAN_IMAGE_MD5_URL,
)
from igvm.utils import cmd


log = logging.getLogger(__name__)


def validate_image_checksum(image):
    """Compares the local image checksum against the checksum returned by
    foreman."""
    local_hash = run(cmd('md5sum {0}', image)).split()[0]

    url = FOREMAN_IMAGE_MD5_URL.format(image=image)
    try:
        remote_hash = urllib2.urlopen(url, timeout=2).read().split()[0]
    except urllib2.URLError as e:
        log.warning('Failed to fetch image checksum at {}: {}'.format(url, e))
        return False

    return local_hash == remote_hash


def download_image(image):
    if files.exists(image) and not validate_image_checksum(image):
        log.warning('Image validation failed, downloading latest version')
        run(cmd('rm -f {0}', image))

    if not files.exists(image):
        url = FOREMAN_IMAGE_URL.format(image=image)
        run(cmd('wget -nv {0}', url))


def extract_image(image, target_dir, hw_os):
    if hw_os == 'squeeze':
        run(cmd('tar xfz {0} -C {1}', image, target_dir))
    else:
        run(cmd("tar --xattrs --xattrs-include='*' -xzf {0} -C {1}", image, target_dir))
