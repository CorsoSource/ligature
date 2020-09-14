import unittest
import math

from shared.data.expression import convert_to_postfix, Expression


class ExpressionTestCase(unittest.TestCase):


    def test_parsing(self):
        pass


    def test_tupleing(self):

        self.assertEqual(Expression( '1,2' )(), (1,2) )
        self.assertEqual(Expression( '1,2,3,4,5' )(), (1,2,3,4,5) )
        self.assertEqual(Expression('1,2,(3,4),5')(), (1, 2, (3, 4), 5) )


    def test_passthru(self):

        self.assertEqual(Expression( 'math.pi' )(), math.pi)
        self.assertEqual(Expression( '22.37' )(), 22.37)
        self.assertEqual(Expression( 'a' )(12), 12)
        self.assertEqual(Expression( 'a' )(a=12), 12)


    def test_precedence(self):

        # Ensure commutive properties are properly parsed
        self.assertEqual(Expression( 'c + a - b' )(a=1,b=2,c=4), 3)
        self.assertEqual(Expression( 'c - b + a' )(a=1,b=2,c=4), 3)

        # Ensure algebraic precedence is held
        self.assertEqual(Expression( 'a + b * c' )(a=1,b=2,c=4), 9)
        self.assertEqual(Expression( 'b * c + a' )(a=1,b=2,c=4), 9)

    def test_parentheticals(self):

        testStack = ((2, '1'), (2, '2'), (2, '3'), (51, '*'), (51, '+'))

        expressions = [ '1 + 2 * 3', 
                        '1 + (2 * 3)',
                        '(1 + (2 * 3))',
                        '(1 + ((2) * 3))'
                        ]
        for expression in expressions:
            self.assertEqual(testStack, convert_to_postfix(expression))

    def test_externals(self):
        
        self.assertEqual(Expression('max(a)')([1,2,3,8,3,2,-1]), 8)
        self.assertEqual(Expression('min(a)')([1,2,3,8,3,2,-1]), -1)

        self.assertEqual(Expression('math.cos(math.pi)')(), -1.0)


    def test_arguments(self):

        expression = Expression( 'a + b * c' )

        self.assertEqual(expression(1,2,4), 9)

        # Specific overrides
        self.assertEqual(expression(1,2,c=4), 9)
        self.assertEqual(expression(1,2,b=3,c=4), 13)
        with self.assertRaises(IndexError):
            _ = expression(1,2,b=3) # missing c!
        self.assertEqual(expression(1,2,4,b=3), 13)
        self.assertEqual(expression(a=1,b=2,c=4), 9)

        # expansion semantics
        self.assertEqual(expression(*(1,2,4)), 9)
        self.assertEqual(expression(**{'a':1,'b':2,'c':4}), 9)
        self.assertEqual(expression(**{'a':1,'b':2,'c':4}), 9)
        self.assertEqual(expression(**dict(a=1,b=2,c=4)), 9)
        self.assertEqual(expression(*(1,2,4),**dict(a=10,b=20,c=40)), 810)


    def test_safety(self):
        pass



def runTests():
    suite = unittest.TestLoader().loadTestsFromTestCase(ExpressionTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
    