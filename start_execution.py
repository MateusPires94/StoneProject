import pandas as pd
import json
import auxtools
import platform

if platform.system() == 'Linux':
    TMP = '/tmp/'
else:
    TMP = 'C:/Users/mateus.ricardo/Desktop/tmp/'

Controller = auxtools.ExecutionController('MOVIE',TMP=TMP,use_controller=1)
Controller.start_new_execution()