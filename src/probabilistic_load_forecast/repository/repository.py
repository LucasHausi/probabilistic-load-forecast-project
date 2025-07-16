class EntsoeRepository:
    def __init__(self, fetcher, mapper):
        self.fetcher = fetcher
        self.mapper = mapper

    def get_data(self, params):
        raw_data = self.fetcher.fetch(params)
        return self.mapper.map(raw_data)