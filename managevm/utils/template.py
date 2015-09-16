import os
from fabric.contrib.files import upload_template as _upload_template

from managevm.utils import get_installdir, fail_gracefully

_template_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'templates')

print "Loading templates from: %s" % _template_dir

def upload_template(filename, destination, context=None):
    _upload_template(
            filename, destination, context, backup=False, use_jinja=True,
            template_dir=_template_dir)

upload_template = fail_gracefully(upload_template)
