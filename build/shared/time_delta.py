from functools import wraps
import time
from .common import Shared

def timeit(fn):
    @wraps(fn)

    def time_delta(*args, **kwargs):
        common = Shared()    
        common.log("""{}()""".format(fn.__name__),level='info')
        common.print_msg("""{}()""".format(fn.__name__))
        # Estimating the Time Delta

        start_time = time.perf_counter()
        retval =fn(*args, **kwargs)
        elapsed_time = time.perf_counter() - start_time

        common.log("""Time elapsed is {0:.4}""".format(elapsed_time),level='info')
        common.print_msg("""Time elapsed is {0:.4}""".format(elapsed_time))
    return time_delta