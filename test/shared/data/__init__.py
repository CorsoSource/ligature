import test.shared.data

def run_all_tests(module=None):
    """
    Run all test.
    >>> shared.test.start.run_all_tests(shared.tests)
    """
#   if repr(module).startswith('<app package '):
#       for name,item in module.getDict().items():
#           run_all_tests(item)
#   elif repr(module).startswith('<module '):
#       if 'runTests' in dir(module):
#           print '\n\n%s\nRunning tests: %r' % ('='*80, module)
#           module.runTests()
#   else:
#       pass
    
    test.shared.data.record.runTests()
    test.shared.data.expression.runTests()
    
    test.shared.data.scanners.chunk.runTests()
    test.shared.data.scanners.element.runTests()
    test.shared.data.scanners.group.runTests()
    test.shared.data.scanners.record.runTests()
    test.shared.data.scanners.replaying.runTests()
    
    test.shared.data.calculations.aggregate.runTests()
    test.shared.data.calculations.cluster.runTests()
    test.shared.data.calculations.sweep.runTests()
    test.shared.data.calculations.window.runTests()
    
    test.shared.data.transforms.merge.runTests()
    test.shared.data.transforms.pivot.runTests()
    test.shared.data.transforms.regroup.runTests()

    print '\n%s\nTests complete!' % ('='*80,)