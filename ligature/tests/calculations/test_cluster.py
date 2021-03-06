import unittest


from ligature.recordset import RecordSet
from ligature.examples import simpleRecordSet, simpleAddition

from ligature.calculations.cluster import Cluster


class ClusterTestCase(unittest.TestCase):

	def test_basic(self):

		function = lambda a,b: a+b

		srs = RecordSet(simpleRecordSet)
		
		c = Cluster([srs], function, 'c')

		# Calculations are lazily evaluated
		self.assertEqual(c._resultset._groups, [])

		# When evaluated, we get the following
		self.assertEqual(
			[[v.c for v in group] for group in c.results.groups],
			[[1, 3, 3, 5], [5, 7], [7, 9, 9]]
			)

		srs.extend(simpleAddition)

		# adding data from a source doesn't immediately update
		self.assertEqual(len(c._resultset._groups), 3)

		# but upon evaluation we see an update has been applied
		# note that this maintains groups
		self.assertEqual(
			[[v.c for v in group] for group in c.results.groups],
			[[1, 3, 3, 5], [5, 7], [7, 9, 9], [12, 12, 14], [14, 16, 16]]
			)

		# Demonstrate slicing for columns works as expected
		self.assertEqual(
			[tuple(group) for group in c.results['c',:]],
			[(1, 3, 3, 5), (5, 7), (7, 9, 9), (12, 12, 14), (14, 16, 16)]
			)



def runTests():
	suite = unittest.TestLoader().loadTestsFromTestCase(ClusterTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)



if __name__ == '__main__':
    unittest.main()