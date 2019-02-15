def create_when_then_return(*expected_args, return_value):
    def when_then_return(*args):
        assert expected_args == args
        return return_value

    return when_then_return


def create_when_then_return_kwargs(expected_kwargs, return_value):
    def when_then_return(*args, **kwargs):
        assert expected_kwargs == kwargs
        return return_value

    return when_then_return
