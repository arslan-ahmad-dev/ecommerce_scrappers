import re
import os
import sys
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

class fengshuimall(scrapy.Spider):
    name = "fengshuimall_crawler"
    base_url = "https://www.fengshuimall.com/"
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

    def get_user_agents(self):
        user_agents_path = os.path.join(parent_dir, 'user_agents.txt')
        with open(user_agents_path, 'r') as f:
            data = f.read()
            user_agents = data.split('\n')
            return user_agents
  
    def parse(self, response):
        try:
            categories = response.xpath(
                "//ul[@class='box-category']/li/a/@href | //ul[@class='box-category']//li//ul/li/a/@href").getall()
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
            if "There are no products to list in this category." in str(response.body):
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

            product_urls = response.xpath(
                '//div[@class="name"]/a/@href').getall()
            
            for product_url in product_urls:
                if "https://www.fengshuimall.com" not in product_url:
                    product_url = f"{self.base_url}{product_url}"
                else:
                    product_url = product_url
                
                yield scrapy.Request(
                    product_url, 
                    callback = self.process_products, 
                    headers = self.set_headers(),
                    cb_kwargs=dict({
                        'product_url': product_url}))
                
        except Exception as e:
            print("*********** Exception: ", e)        


    def process_products(self, response, **kwargs):
        try:
            product_url = str(kwargs['product_url'])

            product_title = response.xpath('//h1[@class="producttitle"]/span/text()').get()
            product_title = text_cleaner.clean_text(product_title) if product_title else None

            current_price = response.xpath('//div[@class="right"]//span[@class="price-new"]/text()').get()
            current_price = float(current_price.replace("US$","").replace(",","")) if current_price else None
            if current_price is None:
                current_price = response.xpath('//div[@class="right"]/div[@class="price"]/text()').get()
                current_price = float(current_price.replace("US$","").replace(",","")) if current_price else None

            regular_price = response.xpath('//div[@class="right"]//span[@class="price-old"]/text()').get()
            regular_price = float(regular_price.replace("US$","").replace(",","")) if regular_price else None

            price = current_price if current_price else regular_price

            if current_price and regular_price:
                discount_price = regular_price - current_price
                discount_percentage = (discount_price/regular_price)*100
                discount_percentage = round(discount_percentage , 2)
            else:
                discount, discount_percentage = None, None

            sku = response.xpath(
                '//div[@class="description"]/span[contains(text(), "SKU:")]/following-sibling::text()[1]').get()
            sku = text_cleaner.clean_text(sku.strip()).split(" ")[0].replace("(Out","") if sku else None
            
            description = response.xpath('//div[@id="tab-description"]/p/text()').getall()
            description = text_cleaner.clean_text("".join(description))

            dimensions, dimensions_unit, size, weight, weight_unit = None, None, None, None, None
            dimensions = response.xpath('//div[@class="description"]/span[contains(text(), "Dim:")]/following-sibling::text()[1]').get()
            dimensions = dimensions.strip() if dimensions else None
            if dimensions and "See" in dimensions:
                dimensions = None
            if dimensions and "mm" in dimensions:
                dimensions_unit = "mm"
            if dimensions and "in" in dimensions:
                dimensions_unit = "inches"
             
            size = response.xpath(
                '//div[@class="options"]//b[contains(text(), "SIZE:")]/following-sibling::select/option/text()').getall()
            size = (text_cleaner.clean_text(str(size))).strip() if size else None
            size = (eval(size))[1:] if size else None

            image = response.xpath('//div[@class="image-additional"]/a/img/@src').get()
            image = image if image else None

            image_urls = response.xpath('//div[@class="image-additional"]/a/img/@src').getall()
            if image_urls:
                new_url =[]
                for image_url in image_urls:
                    new_image = text_cleaner.clean_text(image_url)
                    new_url.append(new_image)
            else:
                new_url = None       

            weights = response.xpath(
                '//div[@class="description"]/span[contains(text(), "Weight")]/following-sibling::text()[1]').get()
            weights = (text_cleaner.clean_text(weights)).strip() if weights else None
            weights = re.findall(r'\((.*?)\)', weights) if weights else None
            weights = (weights[0].strip()).split(" ") if weights else None
            weight, weight_unit = float(weights[0]), weights[1] if weights else None

            categories_name = response.xpath('//div[@class="breadcrumb"]/a/text()').getall()
            main_category = categories_name[-3] if len(categories_name)>=3 else categories_name[-2]
            sub_category = categories_name[-2] if len(categories_name)>=3 else categories_name[-1]

            sizes = None
            record ={"product_url": product_url, "sku" : sku,  "product_title": product_title, "price": price, "discount_percentage": discount_percentage, "weight" : weight, "weight_unit" : weight_unit,
            "size" : size, "main_category" : main_category, "sub_category" : sub_category, "dimensions" : dimensions,
            "dimensions_unit" : dimensions_unit, "description": description, "image": image, "new_url": new_url}

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
        'item_id' : record['sku'],          
        'url': record['product_url'],
        'sku': record['sku'],
        'title': record['product_title'],
        'price': record['price'],
        'size' : sizes if record['size'] else None,
        'description' : record['description'],
        'main_category' : record['main_category'],
        'sub_category' : record['sub_category'],
        'item_weight' : record['weight'],
        'weight_unit' : record['weight_unit'],
        'discount_percentage' : record['discount_percentage'],
        'dimensions' : record['dimensions'],
        'dimensions_unit' : record['dimensions_unit'],
        'image' : record['image'],
        'image_urls' : record['new_url'],            
        }

        output_handler.send_data_to_output_channel(product_info, "frengshumail")
        self.count += 1
        print(product_info)
        print(f"************* COUNT: {self.count}, ITEM ID: {product_info['item_id']}, URL: {product_info['url']} ******")
   
if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(fengshuimall)
    process.start()
