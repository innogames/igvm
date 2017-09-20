import os
from fabric.contrib.files import upload_template as _upload_template


def upload_template(filename, destination, context=None):
    template_dir = os.path.join(os.path.dirname(__file__), '..', 'templates')

    _upload_template(
        filename,
        destination,
        context,
        backup=False,
        use_jinja=True,
        template_dir=template_dir,
        use_sudo=True,
    )
