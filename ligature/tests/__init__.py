import unittest
import ligature


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.discover(ligature.__path__)
    unittest.TextTestRunner(verbosity=2).run(suite)