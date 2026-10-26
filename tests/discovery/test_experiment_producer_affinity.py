from pathlib import Path
import unittest

from experiment_producer_affinity import validate_scope


class ProducerAffinityScope(unittest.TestCase):
    def test_only_exact_p4_producer_is_accepted_never_copy_or_p3_roles(self):
        control = Path('/share-mixed/project/.hcr-copy-p4-fixture')
        config = dict(source='/share/project', destination='/share-mixed/project',
                      migration_scope='project_priority4', exclude=['eai_pwm'], control=str(control))
        expected = dict(pid=123, start_ticks='4', argv=['/usr/bin/python3', '-u',
                        str(control / 'migration_priority4_run.py'), '--control', str(control)])
        validate_scope(control, expected, config)
        for name in ('migration_cached_worker.py', 'migration_priority4_worker.py', 'migration_fast_cold_resume.py'):
            with self.assertRaises(ValueError):
                validate_scope(control, dict(expected, argv=expected['argv'][:2] +
                               [str(control / name)] + expected['argv'][3:]), config)
        for change in (dict(exclude=[]), dict(source='/share/project/eai_pwm'),
                       dict(control='/other'), dict(destination='/share/project')):
            with self.assertRaises(ValueError):
                validate_scope(control, expected, dict(config, **change))


if __name__ == '__main__':
    unittest.main()
