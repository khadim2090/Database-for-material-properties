# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 15:14:14 2020

@author: Alexandre Py
"""
#----------------------------------------------------------------------------------------------#
#Let's start by importing the necessary modules

from mpds_client import MPDSDataRetrieval, MPDSDataTypes, APIError
import pandas as pd
import os

#----------------------------------------------------------------------------------------------#
#We define here the client we will use, namely MPDS Data Retrieval, allowing us to access the MPDS database.
os.environ['MPDS_KEY'] = 'c6psXqU4KWudmweWhFOvCUgW0TQzQJUkzx36r9xWocQCbIX7'

client = MPDSDataRetrieval(dtype=MPDSDataTypes.PEER_REVIEWED)

#----------------------------------------------------------------------------------------------#
#Let's now get our data and organize it.
#First of all, let's start with a list of properties we will be interested in.

props = {'enthalpy of formation': [-2000, 0], 'band gap': [0, 6], 'heat capacity at constant pressure': [0, 500], 'thermal conductivity': [0 ,300], 'linear thermal expansion coefficient': [5E-7, 8E-4], 'sound velocity': [0, 10000], 'microhardness': [0, 200], 'isothermal bulk modulus': [0, 2000], 'coefficient of third-order term in heat capacity': [-20, 20]}


#Now, we initialize our dataframe
data_frame = pd.DataFrame({'Chemical formula':[],
                           'Space group': [],
                         'Elements':[]
    })

data_frame_quat = pd.DataFrame({'Chemical formula':[],
                                'Space group': [],
                         'Elements':[]
    })

data_frame_bin = pd.DataFrame({'Chemical formula':[],
                               'Space group': [],
                         'Elements':[]
    })

#It's time to download the data:
#we will download the data in our list of property to a temporary dataframe.
for prop in props:
    Temp_prop = " ".join([prop, 'temperature'])
    
    if prop == 'band gap':
        
            data_frame_temp = client.get_dataframe(
                {"classes": "ternary", "props": prop},
                fields={'P': [
                    'sample.material.chemical_formula',
                    'sample.material.condition[0].scalar[0].value',
                    'sample.measurement[0].property.scalar',
                    'sample.measurement[0].property.name',
                    'sample.material.chemical_elements'
                    ]},
                    columns=['Chemical formula', 'Space group', prop, 'Bandgap category', 'Elements']
                    )
        
    else:
        
            data_frame_temp = client.get_dataframe(
                {"classes": "ternary", "props": prop},
                fields={'P': [
                    'sample.material.chemical_formula',
                    'sample.material.condition[0].scalar[0].value',
                    'sample.measurement[0].property.scalar',
                    'sample.measurement[0].condition[0].scalar',
                    'sample.material.chemical_elements'
                    ]},
                    columns=['Chemical formula', 'Space group', prop, Temp_prop, 'Elements']
                    )
    
    
    data_frame_temp[prop] = [float(item) for item in data_frame_temp[prop]]
    #data_frame_temp['Elements'] = tuple(data_frame_temp['Elements'])
        
    #if prop != 'structural properties':
    data_frame_temp = data_frame_temp[(data_frame_temp[prop] > props[prop][0]) & (data_frame_temp[prop] < props[prop][1])] 
    
    #----------------------------------------------------------------------------------------------#
    #Once the data are downloaded in our temporary dataframe, we merge the temporary dataframe with the pre-existing one.
    #This will add the new properties to existing materials and create a new line for "new" materials (meaning materials not already existing in the dataframe)
    converted_elmts = [tuple(item) for item in data_frame_temp['Elements']]
    data_frame_temp['Elements'] = converted_elmts
    
    data_frame = pd.merge(data_frame, data_frame_temp, on= ['Chemical formula', 'Space group', 'Elements'], how='outer')

    data_frame = data_frame.drop_duplicates(subset= ['Chemical formula', 'Space group'], keep= 'first')
    #----------------------------------------------------------------------------------------------#
    #If we want to get rid of the compounds containing oxygen
    # data_frame = data_frame[(~data_frame['Chemical formula'].str.contains('O'))]
    
        

#----------------------------------------------------------------------------------------------#
    #Now for the quaternaries

for prop in props:
    Temp_prop = " ".join([prop, 'temperature'])
    
    if prop == 'band gap':
        
            data_frame_temp = client.get_dataframe(
                {"classes": "quaternary", "props": prop},
                fields={'P': [
                    'sample.material.chemical_formula',
                    'sample.material.condition[0].scalar[0].value',
                    'sample.measurement[0].property.scalar',
                    'sample.measurement[0].property.name',
                    'sample.material.chemical_elements'
                    ]},
                    columns=['Chemical formula', 'Space group', prop, 'Bandgap category', 'Elements']
                    )
        
    else:
        
            data_frame_temp = client.get_dataframe(
                {"classes": "quaternary", "props": prop},
                fields={'P': [
                    'sample.material.chemical_formula',
                    'sample.material.condition[0].scalar[0].value',
                    'sample.measurement[0].property.scalar',
                    'sample.measurement[0].condition[0].scalar',
                    'sample.material.chemical_elements'
                    ]},
                    columns=['Chemical formula', 'Space group', prop, Temp_prop, 'Elements']
                    )
    
    
    data_frame_temp[prop] = [float(item) for item in data_frame_temp[prop]]
    #data_frame_temp['Elements'] = tuple(data_frame_temp['Elements'])
        
    #if prop != 'structural properties':
    data_frame_temp = data_frame_temp[(data_frame_temp[prop] > props[prop][0]) & (data_frame_temp[prop] < props[prop][1])] 
    
    #----------------------------------------------------------------------------------------------#
    #Once the data are downloaded in our temporary dataframe, we merge the temporary dataframe with the pre-existing one.
    #This will add the new properties to existing materials and create a new line for "new" materials (meaning materials not already existing in the dataframe)
    converted_elmts = [tuple(item) for item in data_frame_temp['Elements']]
    data_frame_temp['Elements'] = converted_elmts
    
    data_frame_quat = pd.merge(data_frame_quat, data_frame_temp, on= ['Chemical formula', 'Space group', 'Elements'], how='outer')
    
    data_frame_quat = data_frame_quat.drop_duplicates(subset= ['Chemical formula', 'Space group'], keep= 'first')
    #----------------------------------------------------------------------------------------------#
    #If we want to get rid of the compounds containing oxygen
    # data_frame_quat = data_frame_quat[(~data_frame_quat['Chemical formula'].str.contains('O'))]
    

dfrm = data_frame.append(data_frame_quat, sort=False, ignore_index=True)

#Let's clear some memory space
del data_frame
del data_frame_quat
#----------------------------------------------------------------------------------------------#
    #And now for the binaries

for prop in props:
    Temp_prop = " ".join([prop, 'temperature'])
    
    if prop == 'band gap':
        
            data_frame_temp = client.get_dataframe(
                {"classes": "binary", "props": prop},
                fields={'P': [
                    'sample.material.chemical_formula',
                    'sample.material.condition[0].scalar[0].value',
                    'sample.measurement[0].property.scalar',
                    'sample.measurement[0].property.name',
                    'sample.material.chemical_elements'
                    ]},
                    columns=['Chemical formula', 'Space group', prop, 'Bandgap category', 'Elements']
                    )
        
    else:
        
            data_frame_temp = client.get_dataframe(
                {"classes": "binary", "props": prop},
                fields={'P': [
                    'sample.material.chemical_formula',
                    'sample.material.condition[0].scalar[0].value',
                    'sample.measurement[0].property.scalar',
                    'sample.measurement[0].condition[0].scalar',
                    'sample.material.chemical_elements'
                    ]},
                    columns=['Chemical formula', 'Space group', prop, Temp_prop, 'Elements']
                    )
    
    
    data_frame_temp[prop] = [float(item) for item in data_frame_temp[prop]]
    #data_frame_temp['Elements'] = tuple(data_frame_temp['Elements'])
        
    #if prop != 'structural properties':
    data_frame_temp = data_frame_temp[(data_frame_temp[prop] > props[prop][0]) & (data_frame_temp[prop] < props[prop][1])] 
    
    #----------------------------------------------------------------------------------------------#
    #Once the data are downloaded in our temporary dataframe, we merge the temporary dataframe with the pre-existing one.
    #This will add the new properties to existing materials and create a new line for "new" materials (meaning materials not already existing in the dataframe)
    converted_elmts = [tuple(item) for item in data_frame_temp['Elements']]
    data_frame_temp['Elements'] = converted_elmts
    
    data_frame_bin = pd.merge(data_frame_bin, data_frame_temp, on= ['Chemical formula', 'Space group', 'Elements'], how='outer')

    data_frame_bin = data_frame_bin.drop_duplicates(subset= ['Chemical formula', 'Space group'], keep= 'first')
    #----------------------------------------------------------------------------------------------#
    #If we want to get rid of the compounds containing oxygen
    # data_frame_bin = data_frame_bin[(~data_frame_bin['Chemical formula'].str.contains('O'))]


dfrm = dfrm.append(data_frame_bin, sort=False, ignore_index=True)

#Let's clear some memory space
del data_frame_bin

#----------------------------------------------------------------------------------------------#
#Last but not least, let's remove the duplicates from the dataframe (we don't want to have 12 entries for the same material)
dfrm = dfrm.drop_duplicates(subset= ['Chemical formula', 'Space group'], keep= 'first')

# dfrm = dfrm.dropna(thresh= 8)

#----------------------------------------------------------------------------------------------#
#Let's download the structural data for the density computation
struct_list = []
vol_list = []
density_list = []

for index in dfrm.iterrows():
    
    try :
        compound = index[1].at['Chemical formula']
        space_group = index[1].at['Space group']
        test_struct = client.get_data({'props': 'atomic structure', 'formulae': compound, 'sgs': space_group},
                                      fields={'S':[
                                          'sg_n',
                                          'phase_id',
                                          'entry',
                                          'chemical_formula',
                                          'cell_abc',
                                          'volume',
                                          'density'
                                          ]})
    except APIError:
        test_struct = [['bla', 'bla', 'bla', [0, 0, 0, 0, 0, 0], 0, 0, 0]]
        
    cell_param = [0, 0, 0, 0, 0, 0]
    vol = 0
    density = 0
    number = 0
    
    test_struct = [x for x in test_struct if x!= [] ]
    
    for entry in test_struct:
        cell_param_temp = entry[4]
        vol_temp = entry[5]
        dens_temp = entry[6]
        
        # for index_temp in range(5):
        #     cell_param[index_temp] += cell_param_temp[index_temp]
            
        vol += vol_temp
        density += dens_temp
        
        number += 1
    
    # for i in range(5):    
    #     cell_param[i] = cell_param[i] /number
        
    if number != 0:
        vol = vol / number
    
        density = density / number
        
    # struct_list.append(cell_param)
    
    vol_list.append(vol)
    
    density_list.append(density)
        
# dfrm['Structural parameters'] = struct_list

dfrm['Density'] = density_list   

#----------------------------------------------------------------------------------------------#
#----------------------------------------------------------------------------------------------#

dfrm.count()

#----------------------------------------------------------------------------------------------#
#And now, we can export it to an excel and pickle file

dfrm.to_excel(r"C:\Users\Alexandre Py\Documents\Code_simu\Output\MPDS_ML_full_Temp.xlsx")
dfrm.to_pickle(r"C:\Users\Alexandre Py\Documents\Code_simu\Output\MPDS_ML_full_Temp.pkl")

