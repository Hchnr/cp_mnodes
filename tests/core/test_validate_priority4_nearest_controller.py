import ast
from pathlib import Path
import unittest

import validate_priority4_nearest_controller as validation


class ControllerFixtureClosureTests(unittest.TestCase):
    def test_all_direct_and_transitive_test_fixture_imports_are_frozen(self):
        names=set(validation.fixture_names())
        repository=Path(validation.__file__).resolve().parent
        self.assertTrue(set(validation.TESTS)<=names)
        for name in names:
            tree=ast.parse((repository/(name+'.py')).read_text())
            imports=[]
            for node in ast.walk(tree):
                if isinstance(node,ast.Import):imports.extend(item.name for item in node.names)
                elif isinstance(node,ast.ImportFrom) and node.module:imports.append(node.module)
            for module in imports:
                if module.startswith('test_'):
                    self.assertIn(module.split('.')[0],names,(name,module))


if __name__=='__main__':unittest.main()
