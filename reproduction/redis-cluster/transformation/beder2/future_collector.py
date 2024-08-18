import concurrent.futures


class FutureCollector:
    def __init__(self, pool):
        self.pool = pool
        self.futures = {}

    def submit(self, *args, **kwargs):
        f = self.pool.submit(*args, **kwargs)
        self.futures[f] = args[0]  # The function
        return f

    def shutdown(self):
        self.pool.shutdown(wait=False)
        for future in concurrent.futures.as_completed(self.futures.keys()):
            try:
                future.result()
            except Exception as e:
                print(f"Error in function: {self.futures[future]}")
                raise e
        self.pool.shutdown()
