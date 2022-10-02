from rich.console import Console
import functools

console = Console(log_path=False)

def spinner(console, msg, spinner="dots"):
    def decorator_spinner(func):
        @functools.wraps(func)
        def wrapper_decorator(*args, **kwargs):
            with console.status(msg, spinner=spinner):
                value = func(*args, **kwargs)
            return value

        return wrapper_decorator

    return decorator_spinner
