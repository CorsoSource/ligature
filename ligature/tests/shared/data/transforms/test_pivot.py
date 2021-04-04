import unittest


from shared.data.recordset import RecordSet
from shared.data.examples import simpleRecordSet, simpleAddition

from shared.data.transforms.pivot import Pivot


class PivotTestCase(unittest.TestCase):

	def test_basic(self):

		srs = RecordSet(simpleRecordSet)
		
		pivot = Pivot(srs)

		# Pivot converts a group or records into one record per group
		self.assertEqual(
			[[record._tuple for record in group] for group in pivot], 
			[[((1, 2, 3, 4), (0, 1, 0, 1))], 
			 [((5, 6), (0, 1))], 
			 [((7, 8, 9), (0, 1, 0))]]
			)

		srs.extend(simpleAddition)

		# adding data means the transform consumes the new data when checked
		self.assertEqual(
			[[record._tuple for record in group] for group in pivot],
			[[((1, 2, 3, 4), (0, 1, 0, 1))],
			 [((5, 6), (0, 1))],
			 [((7, 8, 9), (0, 1, 0))],
			 [((11, 12, 13), (1, 0, 1))],
			 [((14, 15, 16), (0, 1, 0))]]
			)



def runTests():
	suite = unittest.TestLoader().loadTestsFromTestCase(PivotTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)
