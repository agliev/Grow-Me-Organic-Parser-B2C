import pandas as pd
import os
from source import  b2c, prepairing

industries, needed_ind, terr_dict, codes_dict, cities, cities_to_do = prepairing()

for _, city in cities[:100].iterrows(): # Необходимы лишь первые 100 городов из списка

    if              (city['name'] not in [name[:-5] for name in os.listdir('data/cities')]) \
                and (city['country'] not in ['Russia', 'Ukraine']): # Условие заказчика, избегаем перекосов в выборке
                
        try:
                print(f"trying {city['name']}")

                b2c(result=pd.DataFrame(),
                    city=city,
                    codes_dict=codes_dict,
                    terr_dict=terr_dict,
                    industries=industries)

                print(city['name'])

        except:

            cities_to_do.append({city['name']: city['country']})
            print(city['name']+'_to_do')
    else:

        if city['name'] in [f[:-5] for f in os.listdir('data/cities/needed')]:
            
            try:
                    print(f"trying {city['name']}")

                    b2c(result=pd.read_excel(f'data/cities/{city["name"]}.xlsx'),
                        city=city,
                        codes_dict=codes_dict,
                        terr_dict=terr_dict,
                        industries=needed_ind[city['name']])

                    print(city['name'])

            except:
                
                cities_to_do.append({city['name']: city['country']})
                print(city['name']+'_to_do')
