import pathlib
import tempfile
import unittest
from unittest import mock

import migration_watchdog as watchdog
from migration_io import read_json, write_json


class DisabledWatchdogTests(unittest.TestCase):
    def test_explicit_disable_never_inspects_training_or_changes_stop(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            c=pathlib.Path(tmp)
            write_json(c/'config.json',{'training_watchdog_enabled':False})
            (c/'STOP').write_text('operator stop')
            with mock.patch('sys.argv',['watchdog','--control',str(c)]), \
                    mock.patch.object(watchdog,'elastic_processes',side_effect=AssertionError('must not inspect training')):
                watchdog.main()
            self.assertEqual((c/'STOP').read_text(),'operator stop')
            self.assertEqual(read_json(c/'training-health.json')['state'],'disabled_by_user')


if __name__=='__main__':unittest.main()
