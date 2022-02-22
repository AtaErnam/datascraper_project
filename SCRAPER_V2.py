from bs4 import BeautifulSoup as bs
import datetime

class App():
    def __init__(self,AppName,Uid,IconUrl,LastUpdate,DetailUrl,InsertionDate):
        self.AppName = AppName
        self.Uid = Uid
        self.IconUrl = IconUrl
        self.LastUpdate = LastUpdate
        self.DetailUrl = DetailUrl
        self.InsertionDate = InsertionDate


class Scraper():

    def Scrape(session,Uid):

        Apppage = session.get("https://play.google.com/store/apps/details?id="+Uid).text
        DetailUrl = "https://play.google.com/store/apps/details?id="+Uid
        Appsoup = bs(Apppage,"lxml")

        if Appsoup.find("h1", attrs={"itemprop":"name"}) != None:
            
            
            App_Name = Appsoup.find("h1", attrs={"itemprop":"name"}).text
            #App_Genre = Appsoup.find("a" , attrs={"itemprop":"genre"}).text
            App_Icon_url = Appsoup.find("img", attrs={"itemprop":"image"})["src"]

            #App_Publisher = Appsoup.find("span", class_ = "T32cc UAO9ie").text

            App_Info = Appsoup.find("div", class_ = "IxB2fe" )
            App_Info = App_Info.find_all("span", class_ = "htlgb")

            #App_Version = App_Info[6].text
            Last_Update = App_Info[0].text
            InsertionDate = datetime.datetime.now()
            
            app = App(App_Name,Uid,App_Icon_url,Last_Update,DetailUrl,InsertionDate)
            """    
            print(f"App Name: {App_Name}")
            print(f"App Genre: {App_Genre}")
            print(f"App Version: {App_Version}")
            print(f"App Icon URL: {App_Icon_url}")
            print(f"App Uid Name: {self.url}")
            print("")
            """

            return app
        else:
            return "Null"

    