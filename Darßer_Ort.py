import datetime
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
from sqlalchemy import create_engine

URL = 'https://www.wetter.com/wetter_aktuell/wettervorhersage/16_tagesvorhersage/deutschland/born-a-darss/darsser-ort/DE2938883.html#liste' #url des ortes zum auslesen
get = requests.get(URL).text
#Soup = BeautifulSoup(URL.content, "html.parser")
Soup = BeautifulSoup(get, "html5lib")

tage = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12 ,13, 14, 15]
tage = pd.DataFrame(tage) # bildet die tage in der Zukunft ab 

datum = pd.DataFrame(Soup.find_all("h3", class_="mb--")) #Datum ausgae
temperatur = pd.DataFrame(Soup.find_all("td", class_="text--center beta pl-- pr-- pb-"))#Temperatur
sonne = pd.DataFrame(Soup.find_all("td", class_="text--center pl-- pb-"))#Sonnenstunden
sonne = sonne.iloc[:,-1] #bearbeiten der Sonnenstunden
niederschlag = pd.DataFrame(Soup.find_all('td', class_='text--center pl-- pr-- pb-')) #Niederschlagsmengen
niederschlag['Niederschlag'] = niederschlag.iloc[:,2] #isolieren des niederschlages
niederschlag = niederschlag.iloc[:, 3] # habe es nicht anders hinbekommen
niederschlag = pd.DataFrame(niederschlag) #zum pddataframe
niederschlag = niederschlag.rename_axis('index').reset_index() 
niederschlag = niederschlag[(niederschlag['index'] % 2) ==1].reset_index()
niederschlag = pd.DataFrame(niederschlag.iloc[:, -1])

start_date = datetime.now()
heute = start_date
end_date = heute + timedelta(days = 1)
day_count = (end_date - heute).days + 13

single_date = {}
single_date = [single_date for single_date in (start_date + timedelta(n) for n in range(day_count))]
print(single_date)
single_date = pd.DataFrame(single_date) #schleife mit datumausgaben damit die zeit richtig gesetze ist

wetter = pd.concat([single_date, temperatur, sonne, niederschlag, tage], axis=1) #Tabellen zusammensetzen 

wetter.columns = ['datum', 'temperatur', 'sonne', 'niederschlag', 'tage'] #wetter zusammenfüge

wetter['niederschlag'] = wetter['niederschlag'].replace(r'\s+|\\n', ' ', regex=True) #setzt die {n weg was die sache echt vereinfacht }
wetter['temperatur'] = wetter['temperatur'].replace(r'\s+|\\n', ' ', regex=True) 

# Credentials to database connection
hostname="localhost"
dbname="mathis"
uname="root"
pwd="**"


engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))

# Convert dataframe to sql table                                   
wetter.to_sql('Darßer_Ort', engine, index=False, if_exists='append', chunksize=1000)
