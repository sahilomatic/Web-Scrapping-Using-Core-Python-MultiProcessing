
import logging
import requests
from lxml import html
from multiprocessing import Pool
import pandas as pd
from configparser import ConfigParser


class smartShoppersCrawler():
    name = "smartShoppers"
    results = []

    def htmlToTree(self,page):
        """
            Parse the html, returning a single element/document.

            This tries to minimally parse the chunk of text, without knowing if it
            is a fragment or a document.

            Args:

            page (HTML ELement Object): The first parameter.

        Returns:
        HTML Element Object: The return value after parsing
        """
        tree = html.fromstring(page.content)
        return tree

    def start_requests(self):
        """
            This function call all supporting methods to create dataframe
            and convert it into csv file
                """


        # Fetch target url from config.ini file and assign it to variable url
        config = ConfigParser()
        config.read('config.ini')

        url = config.get('settings','input_link')

        # fetch target page assigned at given url
        page = requests.get(url)
        print(page)

        # convert html page to tree structure
        pageTree = self.htmlToTree(page)

        # Parse tree structure to fetch data
        self.parse(pageTree)

        # Create data frame from results received
        output_df = pd.DataFrame(smartShoppersCrawler.results , columns=['name', 'price',
                                        'availability', 'brand',
                                          'productCode', 'description'])
        # Write Dataframe data into csv file
        output_df.to_csv('output_result.csv',index=False)


    def fetchData(self,item):
        """
        1) Get href link from node.
        2) Get HTML Elemnt Object of given url link
        3) Parse HTML element tree to fetch desired data from it.




        Args:

        item (HTML ELement Object): element tree of listed product.

        Returns:
        (list): The return value is a list containing data 'name', 'price', 'availability',
        'brand','productCode', 'description'
                        """

        item = html.fromstring(item)
        link = item.xpath(".//@href")
        print(link)

        page = requests.get(link[0])

        pageTree = self.htmlToTree(page)
        return self.itemParser(pageTree)


    def parse(self, pageTree):
        """
                    1) Get HTML element where product camera lense are listed
                    2) Use multiprocessing for  fetching text data from HTML elements
                    3) Check if next page exist, run this method recursively until
                    next page is None



                    Args:

                    pageTree (HTML ELement Object): The first parameter.

                Returns:
                None
                """



        # fetch all <a /> tags where parent node id is 'products'
        listItems = pageTree.xpath("//*[@id='products']/div/div/div/div/div/div/a")

        # As HTML element objects can't be serialized during multiprocessing.
        # list of HTML element object is converted to list of string
        stringListItems = [html.tostring(x) for x in listItems]
        print(stringListItems)


        # using with context manager for Pool will handle pool.close() and pool.join() coommands
        with Pool() as pool:

            # call function fetchData over each item in list stringListItems across multiple
            # cores of computer

            results_list = pool.map(self.fetchData, stringListItems)

            # Push output to Static Variable so it can be shared across functions
            smartShoppersCrawler.results.extend(results_list)


        print('result list len is ', len(smartShoppersCrawler.results))

        # fetch next page url from html element where text is '>' and parent node id is 'content'
        nextPageUrl = pageTree.xpath("//*[@id='content']/div[6]/div[1]/ul/li/a[text()='>']/@href")
        print(nextPageUrl)

        # check if next page url exists
        if len(nextPageUrl) >0:
            #fetch next page from next page url
            nextPage = requests.get(nextPageUrl[0])
            # convert page content to HTML element Object tree
            pageTree = self.htmlToTree(nextPage)

            # recursively call self
            self.parse(pageTree)







    def itemParser(self, individualItem):
        """
                Get desired data from different nodes.

                Args:

                item (HTML ELement Object): element tree of 1 single product.

                Returns:
                (list): The return value is a list containing data 'name', 'price', 'availability',
                'brand','productCode', 'description'
                                """


        # find name of product from given xpath
        name = individualItem.xpath("//*[@id='content']/div/div[1]/div/div[2]/h1/text()")
        # find price of product from given xpath
        price = individualItem.xpath("//*[@id='content']/div/div[1]/div/div[2]/div[2]/ul/li/span[1]/text()")
        # find availability of product from given xpath
        availability = individualItem.xpath("//*[@id='content']/div/div[1]/div/div[2]/div[2]/ul/li/span[1]/text()")
        # find brand of product from given xpath
        brand = individualItem.xpath("//*[@id='content']/div/div[1]/div/div[2]/ul[2]/li[2]/a/text()")
        # find productCode of product from given xpath
        productCode = individualItem.xpath("//*[@id='content']/div/div[1]/div/div[2]/ul[2]/li[3]/text()")

        # find description of product from given xpath
        # As data is present in multiple tags , fetch data from self node(id='tab-description')
        # and all child nodes
        description = individualItem.xpath("//*[@id='tab-description']/descendant-or-self::*/text()")

        # create output list after data massaging
        outputList = self.createList(name,price,availability,brand,productCode,description)
        print(outputList)
        return outputList





    def createList(self,name,price,availability,brand,productCode,description):
        """
        Check if data exist and return processed data

        Args:

        name (list): list of single element containing name of product.
        price (list): list of single element containing price of product.
        availability (list): list of single element containing availability of product.
        brand (list): list of single element containing brand of product.
        productCode (list): list of single element containing productCode of product.
        description (list): list of single element containing description of product.
        Returns:
        (list): The return value is a list containing data 'name', 'price', 'availability',
        'brand','productCode', 'description'
                                        """


        # in following conditions ; check if len of list is greater than 0
        # if data is not present , put empty string in attributes

        if (len(name) > 0):
            name = name[0]
        else:
            name = ''



        if (len(price) > 0):
            price = price[0]
        else:
            price = ''

        if (len(availability) > 0):
            availability = availability[0]
        else:
            availability = ''


        if (len(brand) > 0):
            brand = brand[0]
        else:
            brand = ''


        if(len(productCode)>0):
            productCode  = productCode[0]
        else:
            productCode=  ''


        # As description is a list of text fetched from different nested nodes.
        # Merge all text together with ' '  seperation
        mergeDescription = ' '.join(description)
        description= mergeDescription

        result = [name, price, availability, brand, productCode, description]
        return result


if __name__ == '__main__':
    obj = smartShoppersCrawler()
    obj.start_requests()
