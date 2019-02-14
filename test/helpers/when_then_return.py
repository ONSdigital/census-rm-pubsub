
def create_when_then_return(*expected_args, return_value):
    def when_then_return(*args):
        assert expected_args == args
        return return_value

    return when_then_return
