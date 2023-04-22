# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 07:46:57 2023

@author: Sergt
"""

import logging

#Creating and Configuring Logger

Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename = "C:/Users/Sergt/Udacity/Data Engineering/FreeProject/Project/logfile.log",
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.INFO)

logger = logging.getLogger()

formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')

console = logging.StreamHandler()

console.setLevel(logging.INFO)

console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)