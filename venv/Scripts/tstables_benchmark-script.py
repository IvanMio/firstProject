#!c:\users\ivanm\pycharmprojects\firstproject\venv\scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'tstables==0.0.15','console_scripts','tstables_benchmark'
import re
import sys

# for compatibility with easy_install; see #2198
__requires__ = 'tstables==0.0.15'

try:
    from importlib.metadata import distribution
except ImportError:
    try:
        from importlib_metadata import distribution
    except ImportError:
        from pkg_resources import load_entry_point


def importlib_load_entry_point(spec, group, name):
    dist_name, _, _ = spec.partition('==')
    matches = (
        entry_point
        for entry_point in distribution(dist_name).entry_points
        if entry_point.group == group and entry_point.name == name
    )
    return next(matches).load()


globals().setdefault('load_entry_point', importlib_load_entry_point)


if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(load_entry_point('tstables==0.0.15', 'console_scripts', 'tstables_benchmark')())
