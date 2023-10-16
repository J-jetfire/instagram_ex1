class BaseCustomException(Exception):
    status_code = None

    def __init__(self, detail, status_code=None):
        super().__init__(detail)
        if status_code is not None:
            self.status_code = status_code


class ProfileIsPrivateException(BaseCustomException):
    status_code = 403


class ProfilesAreIdenticalException(BaseCustomException):
    status_code = 400
