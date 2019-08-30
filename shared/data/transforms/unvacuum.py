from shared.data.transform import Transform


class Unvacuum(Transform):
    """Inject empty groups to fill an index.
    Thus, if the index function is by value, and it's currently
    {1,2,3,6,7,9}
    a delta of 1 starting with 1 would add empty groups for
    {4,5,8}
    """
    raise NotImplementedError