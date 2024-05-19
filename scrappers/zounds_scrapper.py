import re
import os
import sys
import math
import scrapy
import traceback
from scrapy.crawler import CrawlerProcess
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)

from helpers_method.utils import (
    TextCleaner, OutputHandler, IndexGenerator)
text_cleaner = TextCleaner()
output_handler = OutputHandler()
index_generator = IndexGenerator()

class zounds_crawler(scrapy.Spider):
    name = "zounds_crawler"
    base_url = "https://www.zzounds.com/"
    count = 0
    custom_settings = {
        "CONCURRENT_REQUESTS": 128,
        "CONCURRENT_ITEMS": 128,
        "COOKIES_ENABLED": True,
        "RETRY_TIMES": 20,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 505, 522, 524, 408, 429, 403, 405, 455]
    }
    filenum = 1

    def start_requests(self):
        self.skus = []
        yield scrapy.Request(
            "https://www.zzounds.com/",
            callback = self.grab,
        )

    def grab(self,response):
        try:
            main_categories = response.xpath("//div[@aria-labelledby='hd-cat']//a/@href").extract()
            self.total_categories = len(main_categories)
            
            for category in main_categories:
                yield scrapy.Request(
                    category,
                    callback = self.sub_category_page,
                )
        
        except Exception as e:
            print(f"Issue in grab method: {str(e)}")
            traceback.print_exc()

    def sub_category_page(self,response):
        try:
            sub_category = response.xpath("//div[@class='media-body text-right']/a/@href").extract()
            
            if sub_category:
                for category in sub_category:
                    yield scrapy.Request(
                        category,
                        callback = self.pagination,
                    )
            else:
                yield scrapy.Request(
                    response.url,
                    callback = self.pagination,
                )
        
        except Exception as e:
            print(f"Issue in sub category method: {str(e)}")

    def pagination(self, response):
        try:
            items_count = response.xpath("//em[@class='navbar-text']/text()").extract()
            items_count = re.findall(r'\d+', items_count[0]) if items_count else 20
            
            pages = math.ceil(int(items_count[0])/20)
            page = 1
            
            while page <= pages:
                page_url = response.url + f"&p={page}"
                page += 1
                yield scrapy.Request(
                    page_url,
                    callback = self.product_on_page,
                )
        
        except Exception as e:
            print(f"Issue in pagination: {str(e)}")
            traceback.print_exc()

    def product_on_page(self, response):
        try:
            products = response.xpath("//div[@class='card-body main-body']//a/@href").extract()
            for product in products:
                yield scrapy.Request(
                    product,
                    callback = self.fetch_variations,
                )
        except Exception as e:
            print(f"Issue in product on page: {str(e)}")
            traceback.print_exc()

    def fetch_variations(self, response):
        try:
            variations = response.xpath("//form[@class='item-select']//button[contains(@name,'siid')]/@value").extract()
            if variations:
                variation = True
                for variant in variations:
                    variant_url = response.url + f"?siid={variant}"
                    yield scrapy.Request(
                        variant_url,
                        callback = self.fetch_product_product_info,
                        cb_kwargs = dict(
                            variation = variation
                        ),
                    )
            else: 
                variation = False
                self.fetch_product_product_info(response, variation)
        except Exception as e:
            print(f"Issue in fetch variations: {str(e)}")
            traceback.print_exc()

    def fetch_product_product_info(self, response, variation):
        try:

            title = response.xpath("//h1[@itemprop='name']/text()").extract()
            title = text_cleaner.clean_text(title[0]) if title else ''
            
            description = response.xpath("//meta[@name='description']/@content").extract()
            description = text_cleaner.clean_text(description[0]) if description else ''
            
            upc = response.xpath("//meta[@itemprop='gtin14']/@content").extract()
            upc = upc[0].strip() if upc else ''
            
            sku = response.xpath("//div[@class='financing-container pl-2']/@data-siid").extract()
            if not sku:
                sku = response.xpath("//input[@name='i']/@value").extract()
            sku = sku[0] if sku else ''
            
            brand = response.xpath("//div[@itemprop='brand']/meta[@itemprop='name']/@content").extract()
            brand = text_cleaner.clean_text(brand[0]) if brand else ""
            
            if sku:
                if "siid=" in response.url:
                    url = response.url
                else:
                    url = response.url + f"?siid={sku}"
            
            price = response.xpath("//span[@itemprop='price']/text()").extract()
            price = float(price[0].replace("$","").replace(",",""))
            
            availability = response.xpath("//meta[@itemprop='availability']/@content").extract()
            if availability:
                availability = True if "InStock" or "limited" in availability[0] else False
            else:
                availability = True
            
            variant = response.xpath("//button[@aria-current='true' and @class='btn product-button btn-primary active']/text()").extract()
            if variant:
                variant = text_cleaner.clean_text(variant[0])
                title = title + f" ({variant})"
            else:
                variant = ''
            
            images  = response.xpath("//a[@class='rs-carousel__slide']/@href").extract()
            image = images[0] if images else ""
            
            if "%3Fsiid%" in url:
                url = (url.split("%3Fsiid%"))[0]
            
            offer = response.xpath("//h6[@class='font-weight-bold']/text()").extract()
            offer = text_cleaner.clean_text(offer[0]) if offer else ""
            
            mpn = response.xpath("//div[@class='rsProductOfferContainer']/@data-rs-item-id").extract()
            mpn = mpn[0] if mpn else ""
            
            product_info = {
                'url': url,
                'title': title,
                'description': description,
                "barcode": upc,
                "barcode_type": "GTIN",
                "availability": availability,
                "price": price,
                "hasVariations": variation,
                "isPriceExcVAT": False,
                "brand": brand,
                "mpn": mpn,
                "sku": sku,
                "size": '',
                "color": '',
                "offer": offer,
                "image": image,
                "images": images if images else ""
            }
           
            if product_info['sku'] not in self.skus:
                self.skus.append(product_info['sku'])
                output_handler.send_data_to_output_channel(product_info, "zounds")
                self.count += 1
                print(product_info)
                print(f"************* COUNT: {self.count}, ITEM ID: {product_info['sku']}, URL: {product_info['url']} ******")
        
        except Exception as e:
            print(f"Issue in fetch product_info: {str(e)}")
            traceback.print_exc()

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(zounds_crawler)
    process.start()
