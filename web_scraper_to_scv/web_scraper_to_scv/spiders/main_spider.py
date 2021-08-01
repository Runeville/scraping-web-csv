import scrapy
import pandas as pd
import re
from scrapy.crawler import CrawlerProcess


def clean_data(data):
    data = re.sub(r"\\[a-zA-Z0-9]+ ", "", data)
    data = data.replace("\r\n", "").replace('\xa0', "")
    data = re.sub(r" +", " ", data)
    data = data.strip()
    return data


class MainSpider(scrapy.Spider):
    name = "main_spider"
    url_pattern = "https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string={}&Go.x=12&Go.y=8"

    files_path = "C:/Users/varkh/PycharmProjects/Freelance projects/scraping-web-csv/web_scraper_to_scv/files/"
    filename = "32.csv"
    us_dot = [i for i in pd.read_csv(files_path + filename)["DOT_NUMBER"]]

    start_urls = []
    for number in us_dot:
        url = url_pattern.replace("{}", str(number))
        start_urls.append(url)

    custom_settings = {
        "FEED_FORMAT": "csv",
        "FEED_URI": "result_" + filename
    }

    main_table_xpath = '//center/table[@summary="For formatting purpose"]/tbody/tr'

    def parse(self, response, **kwargs):
        tds = response.xpath('//td[@valign="top"]')[1:]
        try:
            dot_number = clean_data(tds[6].xpath('.//text()').extract()[0])
        except IndexError:
            dot_number = ""
        try:
            entity_type = clean_data(tds[0].xpath('.//text()').extract()[0])
        except IndexError:
            entity_type = ""
        try:
            operating_status = clean_data(response.xpath('//td[@class="queryfield"]')[1].xpath('.//text()').extract()[0])
        except IndexError:
            operating_status = ""
        try:
            legal_name = clean_data(tds[1].xpath('.//text()').extract()[0])
        except IndexError:
            legal_name = ""
        try:
            dba_name = clean_data(tds[2].xpath('.//text()').extract()[0])
        except IndexError:
            dba_name = ""
        try:
            physical_address = clean_data(tds[3].xpath('.//text()').extract()[0] + tds[3].xpath('.//text()').extract()[1])
        except IndexError:
            physical_address = ""
        try:
            phone = clean_data(tds[4].xpath('.//text()').extract()[0])
        except IndexError:
            phone = ""
        try:
            mc_mx_number = clean_data(tds[8].xpath('.//a/text()').extract()[0])
        except IndexError:
            mc_mx_number = ""
        try:
            power_units = clean_data(tds[10].xpath('.//text()').extract()[0])
        except IndexError:
            power_units = ""
        try:
            out_of_service = clean_data(response.xpath('//td[@class="queryfield"]')[2].xpath('.//text()').extract()[0])
        except IndexError:
            out_of_service = ""
        try:
            drivers = clean_data(tds[11].xpath('.//text()').extract()[0])
        except IndexError:
            drivers = ""
        try:
            op_classification = []
            op_classification += tds[14].xpath('./table/tr/td[text()="X"]/following-sibling::td//text()').extract()
            op_classification += tds[15].xpath('./table/tr/td[text()="X"]/following-sibling::td//text()').extract()
            op_classification += tds[16].xpath('./table/tr/td[text()="X"]/following-sibling::td//text()').extract()
            op_classification = ", ".join(op_classification)
        except IndexError:
            op_classification = ""
        try:
            car_op = []
            car_op += tds[18].xpath('./table/tr/td[text()="X"]/following-sibling::td//text()').extract()
            car_op += tds[19].xpath('./table/tr/td[text()="X"]/following-sibling::td//text()').extract()
            car_op += tds[20].xpath('./table/tr/td[text()="X"]/following-sibling::td//text()').extract()
            car_op = ", ".join(car_op)
        except IndexError:
            car_op = ""
        try:
            cargo = []
            cargo += tds[22].xpath('./table/tr/td[text()="X"]/following-sibling::td//text()').extract()
            cargo += tds[23].xpath('./table/tr/td[text()="X"]/following-sibling::td//text()').extract()
            cargo += tds[24].xpath('./table/tr/td[text()="X"]/following-sibling::td//text()').extract()
            cargo = ", ".join(cargo)
        except IndexError:
            cargo = ""

        yield {
            "DOT_NUMBER": dot_number,
            "Entity type": entity_type,
            "Operating Status": operating_status,
            "Legal name": legal_name,
            "DBA Name": dba_name,
            "Physical Address": physical_address,
            "Phone": phone,
            "MC/MX Number": mc_mx_number,
            "Power units": power_units,
            "Out of Service Date": out_of_service,
            "Drivers": drivers,
            "Operation Classification": op_classification,
            "Carrier Operation": car_op,
            "Cargo Carried": cargo
        }


process = CrawlerProcess(settings=MainSpider.custom_settings)
process.crawl(MainSpider)
process.start()
