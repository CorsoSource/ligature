import unittest


from ligature.recordset import RecordSet
from ligature.examples import simpleRecordSet, simpleAddition

from ligature.calculations.aggregate import Aggregate


class AggregateTestCase(unittest.TestCase):

	def test_basic(self):

		function = lambda a,b: sum(a) - sum(b)

		srs = RecordSet(simpleRecordSet)
		
		c = Aggregate([srs], function, 'c')

		# Calculations are lazily evaluated
		self.assertEqual(c._resultset._groups, [])

		# When evaluated, we get the following
		self.assertEqual(
			[[v.c for v in group] for group in c.results.groups],
			[[41]]
			)

		srs.extend(simpleAddition)

		# adding data from a source doesn't immediately update
		self.assertEqual(len(c._resultset._groups), 1)

		# but upon evaluation we see an update has been applied
		# note that results are always one group
		self.assertEqual(
			[[v.c for v in group] for group in c.results.groups],
			[[119]]
			)

		# Demonstrate slicing for columns works as expected
		self.assertEqual(
			[tuple(group) for group in c.results['c',:]],
			[(119,)]
			)

		

def runTests():
	suite = unittest.TestLoader().loadTestsFromTestCase(AggregateTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)



if __name__ == '__main__':
    unittest.main()