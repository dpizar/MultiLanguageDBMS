from enum import IntEnum


class SliceOP(IntEnum):
    EQ = 1
    GT = 2
    LT = 3


class QueryCondition:

    def __init__(self, column, equalityOperator, literal):
        # make sure the equality operator is of type SliceOP
        self._column = column
        if equalityOperator not in SliceOP:
            print("The argument sent is not a valid type. The '=' type operator will be used.")
            self._equalityOperator = SliceOP.EQ
        else:
            self._equalityOperator = equalityOperator
        self._literal = literal

    """Setters and Getters """

    @property
    def column(self):
        return self._column

    @property
    def equalityOperator(self):
        return self._equalityOperator

    @property
    def literal(self):
        return self._literal
