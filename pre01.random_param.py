#! /usr/bin/env python3
import sys, os, shutil, re, yaml
from scipy.stats import norm
from numpy import *

class Parameter:
   def __init__(self, name, used, minval, meanval, maxval, data):
      self.name = name
      self.used = used
      self.minval = minval
      self.meanval = meanval
      self.maxval = maxval
      self.data = data

if len(sys.argv) > 2:
   experiment = sys.argv[1]
   ex_no = sys.argv[2]
ex_no = '%3.3d'%int(ex_no)

# Control
control_file = open('./exp/'+experiment+ex_no+'/control.yaml', 'r')
p = yaml.safe_load(control_file)
control_file.close()
root_dir = p['meta']['root_dir']
start_date = p['meta']['start_date']
nmember = p['da']['nmember']
para_control = p['da']['para_control']
used_parameters = p['da']['parameters']
rain_control = p['da']['rain_control']
used_raingrids = p['da']['raingrids']
nparameter = p['model']['nparameter']
nsubcatchment = p['model']['nsubcatchment']
nconduit = p['model']['nconduit']
nstreet = p['model']['nstreet']

# Directories
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
parameter_dir = root_dir+'/exp/'+experiment+ex_no+'/parameter/'+start_date
if not os.path.isdir(parameter_dir): os.system('mkdir -p '+parameter_dir)

# Member
member = list(range(nmember)) # 0: ensemble mean
for imember in range(nmember): member[imember] = '%8.8d'%imember

# Parameter
parameter = []
parameter_file = const_dir+'/parameters.txt'
text_file = open(parameter_file,'r')
line = text_file.readlines()
text_file.close()
for i in range(1,nparameter+rain_control+1):
   tmp = re.split('\t',line[i].strip())
   nchar = tmp.count('')
   for j in range(nchar): tmp.remove('')
   name = tmp[0].strip()
   if name in used_parameters: used = True
   elif name in ('SVRI'): used = True
   else: used = False
   minval = float(tmp[1])
   meanval = float(tmp[2])
   maxval = float(tmp[3])
   if not used: data = zeros((1,1,2))
   elif para_control == 1: data = zeros((1,nmember,2))
   elif para_control == 2:
      if name in ('Roughness',): data = zeros((nconduit,nmember,2))
      elif name in ('S_Roughness'): data = zeros((nstreet,nmember,2))
      elif name in ('SVRI'): data = zeros((len(used_raingrids),nmember,2))
      else: data = zeros((nsubcatchment,nmember,2))
   print(name, used, minval, meanval, maxval)
   parameter.append(Parameter(name, used, minval, meanval, maxval, data))
#sys.exit(0)

# Random
for i in range(nparameter+rain_control):
   if not parameter[i].used: continue
   n, m, l = parameter[i].data.shape
   parameter[i].data[:,:,0] = random.normal(0.,1.,n*nmember).reshape(n,nmember)
   parameter[i].data[:,0,0] = parameter[i].data[:,1:,0].mean(axis=1)
   parameter[i].data[:,:,1] = (parameter[i].maxval-parameter[i].minval)*norm.cdf(parameter[i].data[:,:,0]) + parameter[i].minval
   print(parameter[i].name, parameter[i].data[0,1])
#sys.exit(0)

# Write
for imember in range(nmember):
   parameter_file = parameter_dir+'/para'+member[imember]
   text_file = open(parameter_file, 'w')
   for i in range(nparameter+rain_control):
      if not parameter[i].used: continue
      n, m, l = parameter[i].data.shape
      text_file.write(parameter[i].name+'\t'+str(n)+'\n')
      for j in range(n): text_file.write('%10.6f'%parameter[i].data[j,imember,0]+'\t'+'%10.6f'%parameter[i].data[j,imember,1]+'\n')
   text_file.close()
pass
