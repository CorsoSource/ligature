def run_all_tests(module):
    """
    Run all tests.
    >>> shared.tests.start.run_all_tests(shared.tests)
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
    
    shared.tests.data.record.runTests()
    shared.tests.data.expression.runTests()
    
    shared.tests.data.scanners.chunk.runTests()
    shared.tests.data.scanners.element.runTests()
    shared.tests.data.scanners.group.runTests()
    shared.tests.data.scanners.record.runTests()
    shared.tests.data.scanners.replaying.runTests()
    
    shared.tests.data.calculations.aggregate.runTests()
    shared.tests.data.calculations.cluster.runTests()
    shared.tests.data.calculations.sweep.runTests()
    shared.tests.data.calculations.window.runTests()
    
    shared.tests.data.transforms.merge.runTests()
    shared.tests.data.transforms.pivot.runTests()
    shared.tests.data.transforms.regroup.runTests()

    print '\n%s\nTests complete!' % ('='*80,)