class NapastakError(Exception):
    pass


class AuthError(NapastakError):
    pass


class PermissionError(NapastakError):
    pass


class TableNotFoundError(NapastakError):
    pass
