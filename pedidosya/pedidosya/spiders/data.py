import scrapy, sys, json
sys.path.append('../pedidosya')
from scrapy.cmdline import execute


class DataSpider(scrapy.Spider):
    name = 'data'

    def start_requests(self):
        cookies = {
            '_pxvid': '9773b316-5c45-11ed-8e28-4b615a4c526a',
            '_pxhd': 'Dsgya385Vx5Yk7okC8XlBDcW5TuKQx/3ALSagfZmoY/Xt1ikBRAUfSNjWOIlQGEyfMiry1vuYw7sWig7StwblA==:WS/dGVdJFMpcbvUEppFKNBDnOkr-VGgnA2fz48cA4OTxTZFui2A7NVgV5-lLDO9R9fmgjt0QiIsg/mFdLSGIzz9GRCdppppvkeV/oMDdoKg=',
            'pxcts': '4b5c61f4-5e6c-11ed-9df8-776d7a42684c',
            '_px3': 'c8d5f043d43b8df1bcf7f52657ab077b361fe5ddaa4e9a0b909e9bac37165dda:blGsDqnpyy9nlCeeEvZ5w55clV1l7HxFnSO9nBC7TTZbEgaIwF5svW4miSpeGb6is9cu1RTb2wF/t6BH0zmCrg==:1000:owTTzY5prxlxs+2E/h7+t/TC2GQ+6OCVWq2GT9f8OuvacCUBE+i+0MXzRX7Y9hqhzX+YQS/x94qLZAfCpU0KPn6HfVljTAjvp35WmdW/1Pub+fV7yGrpfLCGLV1dFp1+6dDoXJvikBMhNHDDOeJg75EGIHgrdqogHajL0SuKOt0wL0SvxAxrUirRPSQbsoASeeDAv03g0lMMGhbsEdE7YA==',
            'dhhPerseusGuestId': '1667898530998.561289683593688000.tnexw9ps28o',
            'dhhPerseusSessionId': '1667898530998.961734493896612900.wdnzftmiqkh',
            'dhhPerseusHitId': '1667898530998.374553479064646500.d8c4utyvfui',
            '__Secure-peya.sid': 's%3A1be762a3-97e6-4e76-8285-30ccc3449c78.kleVC7FMakUet7BbG%2Bx4AYs7q1qM3%2F1%2BG5h1y841kyY',
            '__Secure-peyas.sid': 's%3A20b3915d-b9c8-4a44-ab82-ff22ae3ed6c8.k9LcKq0BOxPNpr35%2BftbrjgljFN5NupuYlOSelSMma0',
            '__cf_bm': 'fPuU_nqU9wt.ZqL3.9nerV4.ERo54pIeAR8tl9R.0PQ-1667898531-0-Ad8GjpEm7aYD9VpOu4r8z1+dVP2yc8Hn1oUomUXoeXV4t3LtjJYM6K7lBka450t3+7qmRcacdLLZOB343NZChs4=',
        }

        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'referer': 'https://www.pedidosya.cl/restaurantes?address=Winston%20Churchill&city=Coquimbo&lat=-29.9687541&lng=-71.33804409999999',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
            }

        url = 'https://www.pedidosya.cl/restaurantes'
        yield scrapy.Request(url=url, headers=headers, cookies=cookies, callback=self.parse)

    def parse(self, response):
        print(response.text)

if __name__ == '__main__':
    execute("scrapy crawl data".split())
