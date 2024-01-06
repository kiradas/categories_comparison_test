import requests
import pandas as pd
import json

class MagnitScraper:
    def __init__(self, num_categories_per_request=3):
        self.num_categories_per_request = num_categories_per_request
        self.categories_url = "https://web-gateway.middle-api.magnit.ru/v2/goods/categories?StoreCode=992301"
        self.goods_url = "https://web-gateway.middle-api.magnit.ru/v3/goods"
        self.headers = {
            "Accept": "*/*",
            "Connection": "keep-alive",
            "Host": "web-gateway.middle-api.magnit.ru",
            "Origin": "https://magnit.ru",
            "Referer": "https://magnit.ru/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "x-app-version": "0.1.0",
            "x-client-name": "magnit",
            "x-device-id": "j8nnqm5s9l",
            "x-device-platform": "Web",
            "x-device-tag": "disabled",
            "x-platform-version": "window.navigator.userAgent",
            "Content-Type": "application/json; charset=UTF-8"
        }
        self.store_codes = ["992301"]
        self.categories_df = self.fetch_categories()
        self.categories_to_parse = pd.Series(self.categories_df['children_id'].dropna().unique()).to_list()
        self.resdf = pd.DataFrame()

    def fetch_categories(self):
        response = requests.get(self.categories_url, headers=self.headers)
        all_categories_raw = pd.DataFrame(eval(response.content))
        all_categories_expl = pd.json_normalize(all_categories_raw.explode('children').to_dict(orient='records')).drop(columns='children')
        all_categories_expl = pd.json_normalize(all_categories_expl.explode('children.children').to_dict(orient='records')).drop(columns='children.children')
        all_categories_expl = pd.json_normalize(all_categories_expl.explode('children.children.children').to_dict(orient='records')).drop(columns='children.children.children')
        all_categories_expl = all_categories_expl[all_categories_expl.columns[~all_categories_expl.columns.str.contains('url|image', case=False)]]
        all_categories_expl[[x for x in all_categories_expl.columns if '.id' in x]] = all_categories_expl[[x for x in all_categories_expl.columns if '.id' in x]].astype(pd.Int32Dtype())
        all_categories_expl.columns = all_categories_expl.columns.str.replace('.', '_')
        return all_categories_expl

    def get_foods_from_cat(self, cats, page=1):
        payload = {
            "categoryIDs": cats,
            "includeForAdults": True,
            "onlyDiscount": False,
            "order": "desc",
            "pagination": {"number": page, "size": 500},
            "shopType": "1",
            "sortBy": "price",
            "storeCodes": self.store_codes,
            "filters": []
        }

        response = requests.post(self.goods_url, headers=self.headers, data=json.dumps(payload))
        pagination = json.loads(response.content)['pagination']
        if 'totalPages' in pagination.keys():
            totalPages = pagination['totalPages']
        else:
            totalPages = 0

        return pd.DataFrame(json.loads(response.content)['goods']), totalPages

    def scrape_data(self):
        cat_size = self.num_categories_per_request
        for i in range(0, len(self.categories_to_parse[:]), cat_size):
            cur_data = self.get_foods_from_cat(self.categories_to_parse[i:i + cat_size])
            self.resdf = pd.concat([self.resdf, cur_data[0]])
            if cur_data[1] > 1:
                cur_data = self.get_foods_from_cat(self.categories_to_parse[i:i + cat_size], page=2)
                self.resdf = pd.concat([self.resdf, cur_data[0]])
            print(self.categories_to_parse[i:i + cat_size], 'was scrapped')
        return self.resdf

if __name__ == '__main__':
    magnit_scraper = MagnitScraper()
    result_df = magnit_scraper.scrape_data()
    result_df.to_parquet('res_magnit.parquet')

