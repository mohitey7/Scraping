import scrapy

class MamaearthLinks(scrapy.Item):
    def __setitem__(self, key, value):
        if key not in self.fields:
            self.fields[key] = scrapy.Field()
        self._values[key] = value
        super().__setitem__(key, value)

class MamaearthData(scrapy.Item):
    def __setitem__(self, key, value):
        if key not in self.fields:
            self.fields[key] = scrapy.Field()
        self._values[key] = value
        super().__setitem__(key, value)