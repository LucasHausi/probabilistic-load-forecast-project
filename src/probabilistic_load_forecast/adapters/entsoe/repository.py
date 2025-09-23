from itertools import chain
class EntsoeRepository:
    def __init__(self, fetcher, mapper):
        self.fetcher = fetcher
        self.mapper = mapper

    def get_data(self, start, end, **kwargs):
        raw_data = self.fetcher.fetch(start, end, **kwargs)
        mapped_data = [self.mapper.map(data) for data in raw_data if data is not None]
        return chain.from_iterable(mapped_data)
