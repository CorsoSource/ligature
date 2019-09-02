import operator as op
import tokenize
from StringIO import StringIO


tokenTypeLookup = {
    getattr(tokenize, tokenType): tokenType
    for tokenType
    in dir(tokenize)
    if isinstance(getattr(tokenize, tokenType), int)
}


def overload_concat_add(*args):
    if len(args) == 1:
        return op.pos(args[0])
    if all(hasattr(type(v),'__iter__') for v in (args)):
        return op.concat(*args)
    else:
        return op.add(*args)

def overload_neg_sub(*args):
    if len(args) == 1:
        return op.neg(args[0])
    else:
        return op.sub(*args)

def overload_mul_rep(*args):
    if hasattr(type(args[0]),'__iter__') and isinstance(args[1],int):
        return op.repeat(*args)
    else:
        return op.mul(*args)

def extend_iterable(left,right):
    return (left,) + tuple(right)


one_argument_operators = {
    'not': op.not_, '!': op.not_,
}

two_argument_operators = {
    '+' : overload_concat_add,
    'in': op.contains,
    '/' : op.truediv, # see PEP 238
    '//': op.floordiv,
    '&' : op.and_, 'and': op.and_,
    '^' : op.xor,
    '~' : op.invert,
    '|' : op.or_, 'or': op.or_,
    '**': op.pow,
    'is': op.is_,
    #'is not': op.is_not,
    '<<': op.lshift,
    '%' : op.mod,
    '*' : overload_mul_rep, #op.mul,
    '-' : overload_neg_sub,
    '>>': op.rshift,
    '<' : op.lt,
    '<=': op.le,
    '==': op.eq,
    '!=': op.ne,
    '>=': op.ge,
    '>' : op.gt,
    ',': extend_iterable,
}


precedence = {
    20: ['(',')','[',']'],
    1:  ['lambda'],
    #2:  ['if else'],
    3:  ['or'],
    4:  ['and'],
    5:  ['!','not'],
    6:  ['in', 'is', '<', '<=', '>', '>=', '<>', '!=', '=='], # 'not in', 'is not', 
    7:  ['|'],
    8:  ['^'],
    9:  ['&'],
    10: ['<<','>>'],
    11: ['+','-'],
    12: ['*','/','//','%'],
    13: ['~'], # ['+','-'] # bitwise not, pos, neg
    14: ['**'],
    #15: [index, slices, x(call), x.attribute]
    #16: [(tuple), [list], {key: value},
}

precedenceLookup = {
    token: key
    for key,tokens in precedence.items()
    for token in tokens
}



def gather_tokens(expression):
    return (
        (tokenType, token)
        for tokenType, token, (srow,scol), (erow,ecol), line 
        in tokenize.generate_tokens(StringIO(expression).readline)
    )


def convert_to_postfix(expression):
    opstack = []
    output = []

    tokens = gather_tokens(expression)

    # Handle the tokens gathered in order.
    # Assume that tokens are provided in INFIX notation
    for tokenTuple in tokens:
        tokenType, token = tokenTuple
        
        # Stop doing work once we get to the end.
        # Importantly: this works for just one line!
        if tokenType == tokenize.ENDMARKER: break
            
        if tokenType == tokenize.OP:
            # Closing a group is treated a bit specially, since it can partly drain the opstack
            if token == ')':
                # Drain the stack until we get to a closing parenthesis 
                #   or the calling function name 
                #   (names only appear on the stack when immediately followed by a parens) 
                while opstack and not (   opstack[-1] == (tokenize.OP,'(') 
                                       or opstack[-1][0] == tokenize.NAME):
                    output.append(opstack.pop())
                # If the parens was preceded by a call, now move that to the output
                if opstack and opstack[-1][0] == tokenize.NAME:
                    output.append(opstack.pop())
                # ... otherwise it must just be the opening parens operator
                else:
                    _ = opstack.pop()
                    
            # Otherwise the token is just a part of the formula 
            else:
                # If we're starting a parenthetical group, check if there's a name right before it.
                #   If so, then assume it's a call, and add that to the stack instead of the '('
                if token == '(' and output and output[-1][0] == tokenize.NAME:
                    opstack.append(output.pop())
                elif token == '.':
                    opstack.append(tokenTuple)
                # Otherwise it's a normal token that has to follow the rules of precedence
                else:
                    # get the value of this token in relation to others
                    tokenPrecedence = precedenceLookup[token]
                    # ... and drain the opstack of things that should come first
                    while (opstack 
                           and precedenceLookup.get(opstack[-1][1],-20) > tokenPrecedence 
                           and not (opstack[-1][0] == tokenize.OP
                                    and opstack[-1][1] in ('(','[','{'))):
                        opToken = opstack.pop()
#                         if not (opToken[0] == tokenize.OP and opToken[1] in ('(',')')):
#                             output.append(opToken)
                        output.append(opToken)
                    # ... and once we know this is the highest precedence operation, add it to the stack
                    opstack.append(tokenTuple)

        # All non-operators get pushed to the stack.
        # These are normal things like numbers and names
        else:
            if tokenType == tokenize.NAME and opstack and opstack[-1][1] == '.':
                output.append(tokenTuple)
                output.append(opstack.pop())
            else:
                output.append(tokenTuple)

        #print '%-50s    %s' % ('OPS> %r' % opstack, '<OUT %r' % output)

    while opstack:
        output.append(opstack.pop())
    
    return tuple(output)


CONSTANT_REFERENCE = -2
ARGUMENT_REFERENCE = -4
FUNCTION_REFERENCE = -8

class Expression(object):
    
    __slots__ = ('_arguments', '_fields', 
                 '_constants', 
                 '_functions',
                 '_eval_func'
                )
    
    _reference_names = {
        FUNCTION_REFERENCE: '_functions',
        ARGUMENT_REFERENCE: '_arguments',
        CONSTANT_REFERENCE: '_constants',
    }
    
    
    def __init__(self, expression):
        # convert the expression to something we can resolve reliably
        postfixStack = convert_to_postfix(expression)
        # ... and map it to the properties here
        self._resolve_function(postfixStack)

    def _resolve_function(self, postfixStack):
        
        arguments = []
        constants = []
        functions = []
        opstack = []
    
        for tokenType,token in postfixStack:

            if tokenType == tokenize.OP:
                if token in one_argument_operators:
                    (argType1,argIx1)= opstack.pop()
                    
                    argRef1 = self._reference_names[argType1]
                    
                    functions.append(one_argument_operators[token])
                    oix = len(functions) - 1
                    
                    if argType1 == FUNCTION_REFERENCE:
                        functions.append(lambda self=self, oix=oix, ar1=argRef1, aix1=argIx1: self._functions[oix](
                                            getattr(self, ar1)[aix1]()
                                        ) )
                    else:
                        functions.append(lambda self=self, oix=oix, ar1=argRef1, aix1=argIx1: self._functions[oix](
                                            getattr(self, ar1)[aix1]
                                        ) )                    
                    fix = len(functions) - 1
                    opstack.append( (FUNCTION_REFERENCE, fix) )

                elif token in two_argument_operators: 
                    (argType2,argIx2), (argType1,argIx1) = opstack.pop(), opstack.pop()
                    
                    argRef1 = self._reference_names[argType1]
                    argRef2 = self._reference_names[argType2]
                    
                    functions.append(two_argument_operators[token])
                    oix = len(functions) - 1
                    
                    # Resolve the way we call the arguments in
                    #   it needs a late binding, so it kind of has to be broken out like this 
                    #   (I don't know how to do this without adding more indirection)
                    if argType1 == FUNCTION_REFERENCE:
                        if argType2 == FUNCTION_REFERENCE:
                            functions.append(lambda self=self, oix=oix, ar1=argRef1, aix1=argIx1, ar2=argRef2, aix2=argIx2: self._functions[oix](
                                                getattr(self, ar1)[aix1](),
                                                getattr(self, ar2)[aix2]()
                                            ) )
                        else:
                            functions.append(lambda self=self, oix=oix, ar1=argRef1, aix1=argIx1, ar2=argRef2, aix2=argIx2: self._functions[oix](
                                                getattr(self, ar1)[aix1](),
                                                getattr(self, ar2)[aix2]
                                            ) )
                    else:
                        if argType2 == FUNCTION_REFERENCE:
                            functions.append(lambda self=self, oix=oix, ar1=argRef1, aix1=argIx1, ar2=argRef2, aix2=argIx2: self._functions[oix](
                                                getattr(self, ar1)[aix1],
                                                getattr(self, ar2)[aix2]()
                                            ) )
                        else:
                            functions.append(lambda self=self, oix=oix, ar1=argRef1, aix1=argIx1, ar2=argRef2, aix2=argIx2: self._functions[oix](
                                                getattr(self, ar1)[aix1],
                                                getattr(self, ar2)[aix2]
                                            ) )
                    fix = len(functions) - 1
                    opstack.append( (FUNCTION_REFERENCE, fix) )

            elif tokenType == tokenize.NAME:
                # Check if it's a variable or module we trust
                # as we encounter the '.' operator, whitelist
                if False and resolved:

                    pass

                else:
                    if not token in arguments:
                        arguments.append(token)
                        aix = len(arguments) - 1
                        opstack.append( (ARGUMENT_REFERENCE, aix) )

            elif tokenType == tokenize.NUMBER:
                constants.append(literal_eval(token))
                cix = len(constants) - 1
                opstack.append( (CONSTANT_REFERENCE, cix) )

            elif tokenType == tokenize.STRING:
                constants.append(str(token))
                cix = len(constants) - 1
                opstack.append( (CONSTANT_REFERENCE, cix) )


        self._fields = tuple(arguments)
        self._constants = tuple(constants)
        self._functions = tuple(functions)
        
        _, fix = opstack.pop()
        self._eval_func = self._functions[fix]


    def __call__(self, *args, **kwargs):
        self._arguments = args        
        return self._eval_func()

    

    



# TESTS
raise KeboardInterrupt

print convert_to_postfix( '1 + 2 * 3' )
print convert_to_postfix( '1 + (2 * 3)' )
print convert_to_postfix( '(1 + (2 * 3))' )
print convert_to_postfix( '(1 + ((2) * 3))' )

((2, '1'), (2, '2'), (2, '3'), (51, '*'), (51, '+'))
((2, '1'), (2, '2'), (2, '3'), (51, '*'), (51, '+'))
((2, '1'), (2, '2'), (2, '3'), (51, '*'), (51, '+'))
((2, '1'), (2, '2'), (2, '3'), (51, '*'), (51, '+'))


x = Expression('a + b * 3')
assert x(1,2) == 7
assert x(1,3) == 10