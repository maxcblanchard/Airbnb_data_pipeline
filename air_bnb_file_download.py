from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from bs4 import BeautifulSoup
import requests


def main():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  #if you don't want the GUI to pop up
    driver = webdriver.Chrome(options=chrome_options)
    driver.get('http://insideairbnb.com/get-the-data.html')
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    denver_lines = soup.find('table', {"class": "table table-hover table-striped denver"})
    denver_list = []
    for a in denver_lines.find_all('a', href=True):
        denver_list.append(a['href'])
    denver_csv_listings_list = []
    denver_csv_reviews_list = []
    denver_csv_gz_listings_list = []
    denver_csv_gz_calendar_list = []
    denver_csv_gz_reviews_list = []
    denver_neighborhood_csv_list = []
    denver_neighborhood_geojson_list = []
    for i in denver_list:
        if 'csv.gz' in i:
            if 'listings' in i:
                denver_csv_gz_listings_list.append(i)
                continue
            if 'calendar' in i:
                denver_csv_gz_calendar_list.append(i)
                continue
            if 'reviews' in i:
                denver_csv_gz_reviews_list.append(i)
                continue
        if 'neighbourhood' in i:
            if 'neighbourhoods.csv' in i:
                denver_neighborhood_csv_list.clear()
                denver_neighborhood_csv_list.append(i)
                continue
            if 'neighbourhoods.geojson' in i:
                denver_neighborhood_geojson_list.clear()
                denver_neighborhood_geojson_list.append(i)
                continue
        else:
            if 'listings' in i:
                denver_csv_listings_list.append(i)
                continue
            if 'reviews' in i:
                denver_csv_reviews_list.append(i)
                continue

    def save_airbnb_files(file_list, save_length, date_value_1, date_value_2):
        for z in range(len(file_list)):
            url = file_list[z]
            r = requests.get(url)
            if 'listings' in url:
                with open('/Users/maxblanchard/PycharmProjects/Airbnb_data_pipeline/AirBNB_Downloads/Listings/'
                          + url[-date_value_1:-date_value_2] + '_' + url[-save_length:], 'wb') as f:
                    f.write(r.content)
            if 'reviews' in url:
                with open('/Users/maxblanchard/PycharmProjects/Airbnb_data_pipeline/AirBNB_Downloads/Reviews/'
                          + url[-date_value_1:-date_value_2] + '_' + url[-save_length:], 'wb') as f:
                    f.write(r.content)
            if 'neighbourhood' in url:
                with open('/Users/maxblanchard/PycharmProjects/Airbnb_data_pipeline/AirBNB_Downloads/Neighbourhoods/'
                          + url[-date_value_1:-date_value_2] + '_' + url[-save_length:], 'wb') as f:
                    f.write(r.content)
            print(r.status_code)
            print(r.headers['content-type'])
            print(r.encoding)

    save_airbnb_files(denver_csv_listings_list, 12, 38, 28)
    save_airbnb_files(denver_csv_reviews_list, 11, 37, 27)
    save_airbnb_files(denver_neighborhood_csv_list, 18, 44, 34)




if __name__ == '__main__':
    main()