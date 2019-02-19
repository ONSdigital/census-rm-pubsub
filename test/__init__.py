def create_stub_function(*expected_args, expected_kwargs={}, return_value):
    def when_then_return(*args, **kwargs):
        assert expected_args == args
        assert expected_kwargs == kwargs
        return return_value

    return when_then_return
