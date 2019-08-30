import unittest

from shared.corso.meta import sentinel

from shared.data.recordset import RecordSet
from shared.data.examples import simpleRecordSet, simpleAddition

from shared.data.scanners.replaying import ReplayingElementScanner, ReplayingChunkScanner, ReplayingRecordScanner, ReplayingGroupScanner


class ReplayingElementScannerTestCase(unittest.TestCase):

	def test_basic(self):

		srs = RecordSet(simpleRecordSet)
		
		scanner = ReplayingElementScanner(srs,'a')

		# Replaying scanners are like generators...
		self.assertEqual(
			[v for v in scanner], 
			[1, 2, 3, 4, 5, 6, 7, 8, 9]
			)

		# ... but will NOT exhaust when fully consumed
		self.assertEqual(
			[v for v in scanner],
			[1, 2, 3, 4, 5, 6, 7, 8, 9]
			)

		# partial iteration...
		self.assertEqual(
			[v for v in sentinel(scanner, 4)],
			[1, 2, 3]
			)
		# ... and an anchor...
		scanner.anchor()
		# ... resumes iteration from the anchor
		self.assertEqual(
			[v for v in scanner],
			[5, 6, 7, 8, 9]
			)

		# Anchoring after iteration stops emitting
		scanner.anchor()
		self.assertEqual(
			[v for v in scanner],
			[]
			)		

		# But if the source adds more data...
		srs.extend(simpleAddition)

		# ... means the scanner consumes the new data
		self.assertEqual(
			[v for v in scanner],
			[11, 12, 13, 14, 15, 16]
			)

		# resetting the scanner means it will replay the whole dataset
		scanner.reset()

		self.assertEqual(
			[v for v in scanner],
			[1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16]
			)


class ReplayingChunkScannerTestCase(unittest.TestCase):

	def test_basic(self):

		srs = RecordSet(simpleRecordSet)
		
		scanner = ReplayingChunkScanner(srs,'a')

		# Replaying scanners are like generators...
		self.assertEqual(
			[v for v in scanner], 
			[(1, 2, 3, 4), (5, 6), (7, 8, 9)]
			)

		# ... but will NOT exhaust when fully consumed
		self.assertEqual(
			[v for v in scanner],
			[(1, 2, 3, 4), (5, 6), (7, 8, 9)]
			)

		# Anchor after next(iter)
		self.assertEqual(
			next(scanner),
			(1,2,3,4)
			)
		scanner.anchor()

		self.assertEqual(
			[v for v in scanner],
			[(5, 6), (7, 8, 9)]
			)

		scanner.reset()

		# partial iteration with an anchor
		for i,v in enumerate(scanner):
		    if i < 2: 
		        scanner.anchor()
		        break

		self.assertEqual(
			[v for v in scanner],
			[(5, 6), (7, 8, 9)]
			)

		# Anchoring after iteration stops emitting
		scanner.anchor()
		self.assertEqual(
			[v for v in scanner],
			[]
			)		

		# But if the source adds more data...
		srs.extend(simpleAddition)

		# ... means the scanner consumes the new data
		self.assertEqual(
			[v for v in scanner],
			[(11, 12, 13), (14, 15, 16)]
			)

		# resetting the scanner means it will replay the whole dataset
		scanner.reset()

		self.assertEqual(
			[v for v in scanner],
			[(1, 2, 3, 4), (5, 6), (7, 8, 9), (11, 12, 13), (14, 15, 16)]
			)


class ReplayingRecordScannerTestCase(unittest.TestCase):

	def test_basic(self):

		srs = RecordSet(simpleRecordSet)
		
		scanner = ReplayingRecordScanner(srs)

		# Replaying scanners are like generators...
		self.assertEqual(
			[r._tuple for r in scanner], 
			[(1, 0), (2, 1), (3, 0), (4, 1), (5, 0), (6, 1), (7, 0), (8, 1), (9, 0)]
			)

		# ... but will NOT exhaust when fully consumed
		self.assertEqual(
			[r._tuple for r in scanner], 
			[(1, 0), (2, 1), (3, 0), (4, 1), (5, 0), (6, 1), (7, 0), (8, 1), (9, 0)]
			)

		# partial iteration...
		for i,r in enumerate(scanner):
		    if i >= 3:
		        scanner.anchor() # anchor in the iteration
		        break
		# ... and an anchor...
		scanner.anchor()
		# ... resumes iteration from the anchor
		self.assertEqual(
			[r._tuple for r in scanner],
			[(5, 0), (6, 1), (7, 0), (8, 1), (9, 0)]
			)

		# Anchoring after iteration stops emitting
		scanner.anchor()
		self.assertEqual(
			[r._tuple for r in scanner],
			[]
			)		

		# But if the source adds more data...
		srs.extend(simpleAddition)

		# ... means the scanner consumes the new data
		self.assertEqual(
			[r._tuple for r in scanner],
			[(11, 1), (12, 0), (13, 1), (14, 0), (15, 1), (16, 0)]
			)

		# resetting the scanner means it will replay the whole dataset
		scanner.reset()

		self.assertEqual(
			[r._tuple for r in scanner],
			[(1, 0),
			 (2, 1),
			 (3, 0),
			 (4, 1),
			 (5, 0),
			 (6, 1),
			 (7, 0),
			 (8, 1),
			 (9, 0),
			 (11, 1),
			 (12, 0),
			 (13, 1),
			 (14, 0),
			 (15, 1),
			 (16, 0)]
			)


class ReplayingGroupScannerTestCase(unittest.TestCase):

	def test_basic(self):

		srs = RecordSet(simpleRecordSet)
		
		scanner = ReplayingGroupScanner(srs)

		# Replaying scanners are like generators...
		self.assertEqual(
			[[r._tuple for r in g] for g in scanner], 
			[[(1, 0), (2, 1), (3, 0), (4, 1)], 
			 [(5, 0), (6, 1)], 
			 [(7, 0), (8, 1), (9, 0)]]
			)

		# ... but will NOT exhaust when fully consumed
		self.assertEqual(
			[[r._tuple for r in g] for g in scanner], 
			[[(1, 0), (2, 1), (3, 0), (4, 1)], 
			 [(5, 0), (6, 1)], 
			 [(7, 0), (8, 1), (9, 0)]]
			)

		# Anchor after next(iter)
		self.assertEqual(
			[r._tuple for r in next(scanner)],
			[(1, 0), (2, 1), (3, 0), (4, 1)]
			)
		scanner.anchor()

		self.assertEqual(
			[[r._tuple for r in g] for g in scanner],
			[[(5, 0), (6, 1)], 
			 [(7, 0), (8, 1), (9, 0)]]
			)

		scanner.reset()

		# partial iteration with an anchor
		for i,r in enumerate(scanner):
		    if i < 2: 
		        scanner.anchor()
		        break

		self.assertEqual(
			[[r._tuple for r in g] for g in scanner],
			[[(5, 0), (6, 1)], 
			 [(7, 0), (8, 1), (9, 0)]]
			)

		# Anchoring after iteration stops emitting
		scanner.anchor()
		self.assertEqual(
			[v for v in scanner],
			[]
			)		

		# But if the source adds more data...
		srs.extend(simpleAddition)

		# ... means the scanner consumes the new data
		self.assertEqual(
			[[r._tuple for r in g] for g in scanner],
			[[(11, 1), (12, 0), (13, 1)], 
			 [(14, 0), (15, 1), (16, 0)]]
			)

		# resetting the scanner means it will replay the whole dataset
		scanner.reset()

		self.assertEqual(
			[[r._tuple for r in g] for g in scanner],
			[[(1, 0), (2, 1), (3, 0), (4, 1)],
			 [(5, 0), (6, 1)],
			 [(7, 0), (8, 1), (9, 0)],
			 [(11, 1), (12, 0), (13, 1)],
			 [(14, 0), (15, 1), (16, 0)]]
			)



suite1 = unittest.TestLoader().loadTestsFromTestCase(ReplayingElementScannerTestCase)
suite2 = unittest.TestLoader().loadTestsFromTestCase(ReplayingChunkScannerTestCase)
suite3 = unittest.TestLoader().loadTestsFromTestCase(ReplayingRecordScannerTestCase)
suite4 = unittest.TestLoader().loadTestsFromTestCase(ReplayingGroupScannerTestCase)
allTests = unittest.TestSuite([suite1, suite2, suite3, suite4])
unittest.TextTestRunner(verbosity=2).run(allTests)