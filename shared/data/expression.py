import __builtin__
import operator as op
import tokenize
from ast import literal_eval
from importlib import import_module
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
                    
                    # Count the compounding
                    dots = 0
                    while opstack and opstack[-1][0] == tokenize.NAME:
                        dots += 1
                        output.append(opstack.pop())
                    for i in range(dots):
                        if opstack and opstack[-1][0] == tokenize.OP and opstack[-1][1] == '.':
                            output.append(opstack.pop())
                        else:
                            raise AttributeError("Unexpected token: was expecting the '.' operator, but the stack is this instead\nopstack:%s\noutput:%s" % (
                                str(opstack), str(output)))
                        
                # ... otherwise it must just be the opening parens operator
                else:
                    _ = opstack.pop()
                    
            # Otherwise the token is just a part of the formula 
            else:
                # If we're starting a parenthetical group, check if there's a name right before it.
                #   If so, then assume it's a call, and add that to the stack instead of the '('
                if token == '(' and output and output[-1][0] == tokenize.NAME:
                    # Check if this is a compound attribute, if so pop an extra off
                    if opstack and opstack[-1][0] == tokenize.OP and opstack[-1][1] == '.':
                        opstack.append(output.pop())
                        if output and output[-1][0] == tokenize.NAME:
                            opstack.append(output.pop())
                        else:
                            raise NotImplementedError('Attributes in expressions must be on named tokens')
                    else:
                        opstack.append(output.pop())
                elif token == '.':
                    opstack.append(tokenTuple)
                # Otherwise it's a normal token that has to follow the rules of precedence
                else:
                    # get the value of this token in relation to others
                    tokenPrecedence = precedenceLookup[token]
                    # ... and drain the opstack of things that should come first
                    while (opstack 
                           and precedenceLookup.get(opstack[-1][1],-20) >= tokenPrecedence 
                           and not (opstack[-1][0] == tokenize.OP
                                    and opstack[-1][1] in ('(','[','{'))):
                        opToken = opstack.pop()
                        output.append(opToken)
                    # ... and once we know this is the highest precedence operation, add it to the stack
                    opstack.append(tokenTuple)

        # All non-operators get pushed to the stack.
        # These are normal things like numbers and names
        else:
            output.append(tokenTuple)

        #print '%-50s    %s' % ('OPS> %r' % opstack, '<OUT %r' % output)

    while opstack:
        output.append(opstack.pop())
    
    return tuple(output)


def isCallable(obj):
    try:
        return bool(obj.__call__)
    except AttributeError:
        return False

        
whitelisted_modules = {
    'shared',
    'math'
}


whitelisted_builtins = {
    'max','min'
}


CONSTANT_REFERENCE = -2
ARGUMENT_REFERENCE = -4
FUNCTION_REFERENCE = -8
EXTERNAL_REFERENCE = -16


class Expression(object):
    
    __slots__ = ('_fields', '_eval_func',
                 '_arguments', '_constants', '_functions', '_externals'
                )
    
    def __init__(self, expression):
        if isinstance(expression, str):
            # convert the expression to something we can resolve reliably
            postfixStack = convert_to_postfix(expression)
            # ... and map it to the properties here
            self._resolve_function(postfixStack)
        else:
            self._resolve_function(expression)
        
    def _resolve_function(self, postfixStack):
        
        self._arguments = []
        self._constants = []
        self._functions = []
        self._externals = []
        opstack = []
    
        references = {
            FUNCTION_REFERENCE: self._functions,
            ARGUMENT_REFERENCE: self._arguments,
            CONSTANT_REFERENCE: self._constants,
            EXTERNAL_REFERENCE: self._externals,
        }
        
        reference_names = {
            FUNCTION_REFERENCE: 'func',
            ARGUMENT_REFERENCE: 'args',
            CONSTANT_REFERENCE: 'const',
            EXTERNAL_REFERENCE: 'ext',
        }
        
    
        for tokenType,token in postfixStack:

            if tokenType == tokenize.OP:

                if token == '.':
                    #raise NotImplementedError('Better to preprocess and resolve first')
                    
                    # though not strictly necessary since the dot will work as a normal operator, 
                    #   this will resolve down and optimize a bit
                    (argType2,argIx2), (argType1,argIx1) = opstack.pop(), opstack.pop()
                    
                    if argType1 == FUNCTION_REFERENCE and self._functions[argIx1] in self._externals and argType2 == ARGUMENT_REFERENCE:
                        
                        attribute = self._arguments.pop(argIx2)
                        
                        external = getattr(self._functions[argIx1], attribute)
                        self._externals.append(external)
                        
                        if isCallable(external):
                            argType3,argIx3 = opstack.pop()
                            argRef3 = references[argType3]
                            
                            if argType3 == FUNCTION_REFERENCE:
                                self._functions.append(lambda function=external, ar1=argRef3, aix1=argIx3: function(
                                                    ar1[aix1]()
                                                ) )
                            else:
                                self._functions.append(lambda function=external, ar1=argRef3, aix1=argIx3: function(
                                                    ar1[aix1]
                                                ) )

                            opstack.append( (FUNCTION_REFERENCE, len(self._functions) - 1) )
                        else:
                            self._constants.append(external)
                            opstack.append( (CONSTANT_REFERENCE, len(self._constants) - 1) )
                        #self._externals.append(getattr(self._externals[argIx1], attribute))
                        #opstack.append( (EXTERNAL_REFERENCE, len(self._externals) - 1) )
                        
                    else:
                        raise AttributeError('Not sure what to do with this:\nArg 1: type:%s %s\nArg 2: type:%s %s' (argType1,argIx1,argType2,argIx2))
                    
                
                elif token in two_argument_operators: 
                    (argType2,argIx2), (argType1,argIx1) = opstack.pop(), opstack.pop()
                    
                    argRef1 = references[argType1]
                    argRef2 = references[argType2]
                    
                    function = two_argument_operators[token]
                    
                    # Resolve the way we call the arguments in
                    #   it needs a late binding, so it kind of has to be broken out like this 
                    #   (I don't know how to do this without adding more indirection)
                    if argType1 == FUNCTION_REFERENCE:
                        if argType2 == FUNCTION_REFERENCE:
                            self._functions.append(lambda self=self, function=function, ar1=argRef1, aix1=argIx1, ar2=argRef2, aix2=argIx2: function(
                                                ar1[aix1](),
                                                ar2[aix2]()
                                            ) )
                        else:
                            self._functions.append(lambda self=self, function=function, ar1=argRef1, aix1=argIx1, ar2=argRef2, aix2=argIx2: function(
                                                ar1[aix1](),
                                                ar2[aix2]
                                            ) )
                    else:
                        if argType2 == FUNCTION_REFERENCE:
                            self._functions.append(lambda self=self, function=function, ar1=argRef1, aix1=argIx1, ar2=argRef2, aix2=argIx2: function(
                                                ar1[aix1],
                                                ar2[aix2]()
                                            ) )
                        else:
                            self._functions.append(lambda self=self, function=function, ar1=argRef1, aix1=argIx1, ar2=argRef2, aix2=argIx2: function(
                                                ar1[aix1],
                                                ar2[aix2]
                                            ) )
                            
                    opstack.append( (FUNCTION_REFERENCE, len(self._functions) - 1) )

                if token in one_argument_operators:
                    (argType1,argIx1)= opstack.pop()
                    
                    argRef1 = references[argType1]
                    
                    if argType1 == FUNCTION_REFERENCE:
                        self._functions.append(lambda self=self, function=function, ar1=argRef1, aix1=argIx1: function(
                                            ar1[aix1]()
                                        ) )
                    else:
                        self._functions.append(lambda self=self, function=function, ar1=argRef1, aix1=argIx1: function(
                                            ar1[aix1]
                                        ) )
                        
                    opstack.append( (FUNCTION_REFERENCE, len(self._functions) - 1) )
                    
            elif tokenType == tokenize.NAME:
                # Check if it's a variable or module we trust
                # as we encounter the '.' operator, whitelist
                if token in whitelisted_modules:
                    # math.sin(x) --> ((1, 'math'), (1, 'sin'), (51, '.'), (1, 'x'))
                    self._externals.append(import_module(token))
                    self._functions.append(self._externals[-1])
                    opstack.append( (FUNCTION_REFERENCE, len(self._functions) - 1) )
                    #opstack.append( (EXTERNAL_REFERENCE, len(self._externals) - 1) )
                
                elif token in whitelisted_builtins:
                    
                    function = getattr(__builtin__,token)

                    self._externals.append(function)
                    #self._functions.append(self._externals[-1])
                    #opstack.append( (FUNCTION_REFERENCE, len(self._functions) - 1) )

                    (argType1,argIx1)= opstack.pop()
                    
                    argRef1 = references[argType1]
                    
                    if argType1 == FUNCTION_REFERENCE:
                        self._functions.append(lambda self=self, function=function, ar1=argRef1, aix1=argIx1: function(
                                            ar1[aix1]()
                                        ) )
                    else:
                        self._functions.append(lambda self=self, function=function, ar1=argRef1, aix1=argIx1: function(
                                            ar1[aix1]
                                        ) )
                        
                    opstack.append( (FUNCTION_REFERENCE, len(self._functions) - 1) )                    
                    
                else:
                    if not token in self._arguments:
                        self._arguments.append(token)
                        opstack.append( (ARGUMENT_REFERENCE, len(self._arguments) - 1) )

            elif tokenType == tokenize.NUMBER:
                self._constants.append(literal_eval(token))
                opstack.append( (CONSTANT_REFERENCE, len(self._constants) - 1) )

            elif tokenType == tokenize.STRING:
                self._constants.append(str(token))
                opstack.append( (CONSTANT_REFERENCE, len(self._constants) - 1) )

            # print 'Token: %s, %s' % (tokenTypeLookup[tokenType], token)
            # print '  Opstack: ',['%s[%d]=%s' % (reference_names[t],ix,references[t][ix]) for t,ix in opstack]
            
        self._fields = tuple(self._arguments)
        self._arguments[:] = []
        
        opType,opIx = opstack.pop()
        if opType in (CONSTANT_REFERENCE, ARGUMENT_REFERENCE):
            self._eval_func = lambda: references[opType][opIx]
        else:
            self._eval_func = references[opType][opIx]


    def __call__(self, *args, **kwargs):
        if kwargs:
            self._arguments[:] = [kwargs.get(field) or args[i] for i,field  in enumerate(self._fields)]
        else:
            self._arguments[:] = args        
        return self._eval_func()    

