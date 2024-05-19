import re
import os
import sys
import json
import scrapy
import random
import traceback
from scrapy.crawler import CrawlerProcess
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)

from helpers_method.utils import (
    TextCleaner, UserAgentManager, OutputHandler, IndexGenerator)
text_cleaner = TextCleaner()
output_handler = OutputHandler()
index_generator = IndexGenerator()

class rolver(scrapy.Spider):
    name = "rolver_crawler"
    base_url = "https://rollover.com.pk/"
    count = 0
    custom_settings = {
        "CONCURRENT_REQUESTS": 128,
        "CONCURRENT_ITEMS": 128,
        "COOKIES_ENABLED": True,
        "RETRY_TIMES": 20,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 505, 522, 524, 408, 429, 403, 405, 455]
    }
    output_array = []
    filenum = 1

    def start_requests(self):
      self.categories = []
      yield scrapy.Request(
        self.base_url,
        callback = self.parse, 
        headers = self.set_headers(),
      )

    def set_headers(self):
        return {
            'user-agent' : random.choice(UserAgentManager.get_user_agents(parent_dir))
        }
  
    def parse(self, response):
        try:
            categories = response.xpath(
                '//li[@class="type_mega menu_wid_full menu-item has-children menu_has_offsets menu_default pos_default"]/a/@href'
            ).getall()
            categories = list(set(categories))
            
            for category in categories:
                if 'https' not in category:
                    category_url = f'{self.base_url}{category}'
                else:
                    category_url = category                
                yield scrapy.Request(
                    category_url, 
                    callback = self.process_products_url,
                    headers = self.set_headers(),
                    cb_kwargs=dict({
                        'category_url': category_url
                    })
                )
        except Exception as e:
            print(f'Exception in reading categories: {e}')
            traceback.print_exc()

    def process_products_url(self, response, category_url, page_size = 1): 
        try:
            if "No products were found matching your selection." in str(response.body):
                return          
            
            page_url = category_url + f"?page={page_size}"
            page_size += 1
            yield scrapy.Request(
                page_url,
                callback = self.process_products_url,
                headers = self.set_headers(),
               cb_kwargs=dict(
                    category_url = category_url,
                    page_size = page_size
                ),
            )

            product_pattern = r'<a\s+[^>]*href="([^"]*)"'
            product_matches = re.findall(product_pattern, str(response.body))
            product_urls = [url for url in product_matches if url.startswith('/collections')]
            product_urls = [url for url in product_urls if re.search(r'\d{4}$', url)]

            for product_url in product_urls:
                if "https://rollover.com.pk" not in product_url:
                    product_url = f"{self.base_url}{product_url}"
                else:
                    product_url = product_url
                
                yield scrapy.Request(
                    product_url, 
                    callback = self.process_products, 
                    headers = self.set_headers(),
                    cb_kwargs=dict({
                        'product_url': product_url})
                )
        
        except Exception as e:
            print("*********** Exception: ", e)

    def process_products(self, response, **kwargs):
        try:
            product_url = str(kwargs['product_url'])

            product_title = response.xpath('//h1[@class="product_title entry-title"]/text()').get()
            product_title = text_cleaner.clean_text(product_title) if product_title else None

            current_price = response.xpath('//span[@class="price_varies current_price"]/ins/text()').get()
            current_price = float(current_price.replace("Rs.","").replace(",","")) if current_price else None

            regular_price = response.xpath('//span[@class="price_varies current_price"]/del/text()').get()
            regular_price = float(regular_price.replace("Rs.","").replace(",","")) if regular_price else None

            price = current_price if current_price else regular_price

            if current_price and regular_price:
                discount_price = regular_price - current_price
                discount_percentage = (discount_price/regular_price)*100
                discount_percentage = round(discount_percentage , 2)
            else:
                discount, discount_percentage = None, None

            sku = response.xpath('//span[@id="pr_sku_ppr"]/text()').get()
            sku = text_cleaner.clean_text(sku.strip()) if sku else None

            brand_pattern = r'"brand"\s*:\s*"([^"]+)"'
            brand = re.findall(brand_pattern, str(response.body))
            brand = list(set(brand)) if brand else None
            brand = brand[0] if brand else None

            size = re.findall(r'variants":(.*)},"pag', str(response.body))
            size = size[0] if size else None
            try:
                size = json.loads(size) if size else None
            except Exception as e:
                size = re.sub(r'\\', '', size)
                size = size.strip("'")
                size = json.loads(size) if size else None

            image = response.xpath('//meta[@property="og:image" ]').get()
            image_url = re.search(r'content="(.*?)"', image).group(1) if image else None
            image = image_url if image_url else None

            image_urls = [image] if image else None
            if image_urls:
                new_url =[]
                for image_url in image_urls:
                    new_image = text_cleaner.clean_text(image_url)
                    new_url.append(new_image)
            else:
                new_url = None

            categories_name = response.xpath('//nav[@class="sp-breadcrumb"]//a/text()').getall()
            main_category = categories_name[-2] if categories_name else None
            sub_category = categories_name[-1] if categories_name else None
            
            sizes = None
            record ={"product_url": product_url, "sku" : sku,  "product_title": product_title, "price": price, "discount_percentage": discount_percentage,
            "size" : size, "main_category" : main_category, "sub_category" : sub_category, "brand" : brand,
            "image": image, "new_url": new_url}

            if size:
                for sizes in size:
                    self.pass_data(record,sizes)
            else:
                self.pass_data(record,sizes) 
                                   
        except Exception as e:
            traceback.print_exc()
            print(e)
     
    def pass_data(self, record, sizes):
        product_info = { 
        'index' : index_generator.get_index(),        
        'item_id' : sizes['sku'] if sizes['sku'] else record['sku'],          
        'url': record['product_url'],
        'sku': sizes['sku'] if sizes['sku'] else record['sku'],
        'title': sizes['name'] if sizes['name'] else record['product_title'],
        'price': float(str(sizes['price'])[:-2]) if sizes['price'] else record['price'],
        'brand' : record['brand'],
        'size' : sizes['public_title'] if sizes['public_title'] else None,
        'main_category' : record['main_category'],
        'sub_category' : record['sub_category'],
        'image' : record['image'],
        'image_urls' : record['new_url'],
        "is_dropship" : False             
        }

        output_handler.send_data_to_output_channel(product_info, "rolver")
        self.count += 1
        print(product_info)
        print(f"************* COUNT: {self.count}, ITEM ID: {product_info['item_id']}, URL: {product_info['url']} ******")

   
if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(rolver)
    process.start()
