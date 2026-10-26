import os
from pathlib import Path
import tempfile
import types
import unittest

import migration_reaudit_accelerate as accelerate


class ExpansionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='reaudit-expand-test-');self.addCleanup(self.temp.cleanup)
        self.source=Path(self.temp.name)
        self.scan=types.SimpleNamespace(directory=lambda root,rel:os.open(self.source/rel,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW),
            excluded=lambda rel:rel=='eai_pwm/datasets/raw' or rel.startswith('eai_pwm/datasets/raw/'))

    def test_shallow_task_does_not_steal_another_original_owner(self):
        unit=dict(rel='a',leaves_only=True)
        self.assertEqual(accelerate.expand(self.scan,unit),[unit])

    def test_expanded_recursive_task_has_one_owner_per_entry(self):
        for path in ['a/x/xx','a/x/xy','a/y','a/z/deep']:(self.source/path).mkdir(parents=True,exist_ok=True)
        for path in ['a/f','a/x/f','a/x/xx/f','a/y/f','a/z/deep/f']:(self.source/path).write_text('fixture')
        units=accelerate.expand(self.scan,dict(rel='a',leaves_only=False))
        self.assertEqual(len(units),len({u['rel'] for u in units}))
        for path in (self.source/'a').rglob('*'):
            rel=str(path.relative_to(self.source));owners=[]
            for unit in units:
                root=unit['rel']
                if path.is_dir():
                    owns=(rel==root or (not unit['leaves_only'] and rel.startswith(root+'/')))
                else:
                    owns=(str(path.parent.relative_to(self.source))==root if unit['leaves_only'] else rel.startswith(root+'/'))
                if owns:owners.append(unit)
            self.assertEqual(len(owners),1,(rel,owners))

    def test_broad_parent_is_split_instead_of_becoming_one_hot_queue(self):
        for i in range(80):(self.source/'b'/str(i)).mkdir(parents=True,exist_ok=True)
        units=accelerate.expand(self.scan,dict(rel='b',leaves_only=False))
        self.assertEqual(len(units),81)
        self.assertIn(dict(rel='b',leaves_only=True),units)
        self.assertEqual(sum(not u['leaves_only'] for u in units),80)

    def test_unreadable_or_disappeared_preview_keeps_original_obligation(self):
        unit=dict(rel='missing',leaves_only=False)
        self.assertEqual(accelerate.expand(self.scan,unit),[unit])

    def test_raw_scope_is_never_previewed(self):
        with self.assertRaises(ValueError):accelerate.expand(self.scan,dict(rel='eai_pwm/datasets/raw',leaves_only=False))


if __name__=='__main__':unittest.main()
