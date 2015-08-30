from os import listdir
from os.path import isfile, join
from importlib.machinery import SourceFileLoader


def get_revision_from_name(filename):
    try:
        rev, info = filename.split('_', 1)
        info = info.split('.')[:-1][0]
        revision = int(rev)
        description = ' '.join(info.split('_'))
        return revision, description
    except:
        raise Exception("%s is not a valid migration file" % filename)


class Revision:
    def __init__(self, filename):
        self.filename = filename
        rev, description = get_revision_from_name(filename)
        self.version = rev
        self.description = description
        module = SourceFileLoader(filename, filename).load_module()
        self.upgrade = module.upgrade
        self.downgrade = module.downgrade

    def __repr__(self):
        return '<Revision: version=%s, desc=%s>' % (
            self.version,
            self.description
        )

    def __eq__(self, other):
        # TODO: Maybe be smarter about this?
        r1 = repr(self)
        r2 = repr(other)

        return r1 == r2


def get_files_in_directory(directory):
    """
    Get all file in a directory, exclude any directories. This will sort by
    revision number.
    """
    files = [
        Revision(f) for f in listdir(directory) if isfile(join(directory, f))
    ]

    revisions = sorted(files, key=lambda r: r.version)
    return revisions


def get_upgrade_path(directory, version=None):
    """
    Loads all the files in the order necessary to upgrade.

    Optionals `revision` argument if you want to start from
    a certain location.
    """
    revisions = get_files_in_directory(directory)
    if version:
        revisions_to_run = [r for r in revisions if r.version >= version]
    else:
        revisions_to_run = revisions

    return revisions_to_run
