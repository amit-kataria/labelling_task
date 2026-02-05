class AllocationError(Exception):
    pass


class NoEligibleUsersError(AllocationError):
    pass


class AllocationTransientError(AllocationError):
    pass
