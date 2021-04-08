import unittest


from ligature.recordset import RecordSet

from ligature.transforms.regroup import Regroup


class RegroupTestCase(unittest.TestCase):

	def test_basic(self):

		rsa = [(1,2,3,4),(5,6),(7,8,9)]
		rsb = [(0,1,0,1),(0,1),(0,1,0)]
		rsc = [(1,2,3),(4,),(5,6,7,8,9)]
		rsd = [(0,1,0),(1,),(0,1,0,1,0)]

		# source
		rss = RecordSet(recordType='ab')
		for g in zip(rsa,rsb):
		    rss.append(v for v in zip(*g))

		# target
		rst = RecordSet(recordType='ef')
		for g in zip(rsc,rsd):
		    rst.append(v for v in zip(*g))

		regroup = Regroup(rss, rst)

		# verify it has the same columns as the target
		self.assertEqual(
			regroup.results._RecordType._fields,
			('e', 'f')
			)

		self.assertEqual(
			[[record._tuple for record in group] for group in regroup],
			[[(1, 0), (2, 1), (3, 0), (4, 1)], 
			 [(5, 0), (6, 1)], 
			 [(7, 0), (8, 1), (9, 0)]] 
			)

	def test_misalignment_1(self):

		# source
		rsa = [(1,2,3,4),(5,6),(7,8,9)]
		rsb = [(0,1,0,1),(0,1),(0,1,0)]
		rss = RecordSet(recordType='ab')
		for g in zip(rsa,rsb):
		    rss.append(v for v in zip(*g))

		# target - shorter
		rsa = [(1,2,3),(4,),(5,6,7)]
		rsb = [(0,1,0),(1,),(0,1,0)]
		rst = RecordSet(recordType='ef')
		for g in zip(rsa,rsb):
		    rst.append(v for v in zip(*g))

		regroup = Regroup(rss, rst)


		# verify it has the same columns as the target
		self.assertEqual(
			regroup.results._RecordType._fields,
			('e', 'f')
			)

		# Up to 7 records can be grouped. The final source group
		#   must be incomplete, and is omitted
		self.assertEqual(
			[[record._tuple for record in group] for group in regroup],
			[[(1, 0), (2, 1), (3, 0), (4, 1)], 
			 [(5, 0), (6, 1)]]
			)

		# adding three more means the target has at least enough to complete
		rst.append( [(8,1),(9,0)] )

		# note that the last is omitted, though, 
		#   since the source doesn't have a group to map to it
		self.assertEqual(
			[[record._tuple for record in group] for group in regroup],
			[[(1, 0), (2, 1), (3, 0), (4, 1)], 
			 [(5, 0), (6, 1)], 
			 [(7, 0), (8, 1), (9, 0)]]
			)		


	def test_misalignment_2(self):

		# source
		rsa = [(1,2,3,4)]
		rsb = [(0,1,0,1)]
		rss = RecordSet(recordType='ab')
		for g in zip(rsa,rsb):
		    rss.append(v for v in zip(*g))

		# target - longer
		rsa = [(1,2,3),(4,),(5,6,7),(8,9,10)]
		rsb = [(0,1,0),(1,),(0,1,0),(1,0,10)]
		rst = RecordSet(recordType='ef')
		for g in zip(rsa,rsb):
		    rst.append(v for v in zip(*g))

		regroup = Regroup(rss, rst)


		# verify it has the same columns as the target
		self.assertEqual(
			regroup.results._RecordType._fields,
			('e', 'f')
			)

		# Source only has one group, so that alone gets mapped
		self.assertEqual(
			[[record._tuple for record in group] for group in regroup],
			[[(1, 0), (2, 1), (3, 0), (4, 1)]]
			)

		# adding two more groups...
		rss.extend( [
			 ((5,0),(6,1)),
             ((7,0),(8,1),(9,0))
            ] )

		# ... allows two more groups to be added.
		# Again, note that the last target record is omitted, though, 
		#   since the source doesn't have a group to map to it
		self.assertEqual(
			[[record._tuple for record in group] for group in regroup],
			[[(1, 0), (2, 1), (3, 0), (4, 1)], 
			 [(5, 0), (6, 1)], 
			 [(7, 0), (8, 1), (9, 0)]]
			)		



def runTests():
	suite = unittest.TestLoader().loadTestsFromTestCase(RegroupTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)



if __name__ == '__main__':
    unittest.main()