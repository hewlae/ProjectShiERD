import json, yaml
import datetime
import sys, os, re
from swmm_api import read_inp_file
from swmm_api import read_out_file

if len(sys.argv) > 4:
    analysis_date = sys.argv[1] # 201206290300
    end_date = sys.argv[2] # 201206291300
    experiment = sys.argv[3] # Mangwon or Bellinge
    ex_no = sys.argv[4] # 3
ex_no = '%3.3d'%int(ex_no)

# Control 
control_file = open('./exp/'+experiment+ex_no+'/control.yaml', 'r')
p = yaml.safe_load(control_file)
control_file.close()
root_dir = p['meta']['root_dir']
#start_date = p['meta']['start_date']
start_date = analysis_date
cycle = p['da']['cycle']
interval = p['da']['interval']
nmember = p['da']['nmember']

# Directories
bin_dir = root_dir+'/build'
bin_file = bin_dir+'/runswmm'
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
rainfall_dir = root_dir+'/exp/'+experiment+ex_no+'/rainfall'
parameter_dir = root_dir+'/exp/'+experiment+ex_no+'/parameter/'+analysis_date
analysis_dir = root_dir+'/exp/'+experiment+ex_no+'/analysis/'+analysis_date
forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date
obs_dir = root_dir+'/exp/'+experiment+ex_no+'/observation'
obs_file = root_dir+'/exp/'+experiment+ex_no+'/observation/obs'+analysis_date
flood_dir =  root_dir+'/exp/'+experiment+ex_no+'/flood/'+start_date+'_'+end_date
if not os.path.isdir(flood_dir): os.system('mkdir -p '+flood_dir)
flood_file = flood_dir+'/flood'+'%8.8d'%0

# Datetime
date = datetime.datetime(int(start_date[:4]),int(start_date[4:6]),int(start_date[6:8]),int(start_date[8:10]),int(start_date[10:12]))
end_date = datetime.datetime(int(end_date[:4]),int(end_date[4:6]),int(end_date[6:8]),int(end_date[8:10]),int(end_date[10:12]))
analysis_date = date.strftime('%Y%m%d%H%M')

# Read INP file
template_file = const_dir+'/input.txt'
inp = read_inp_file(template_file)
links = inp.CONDUITS
roads = links.create_new_empty()
# xlinks = inp.XSECTIONS
# xroads = links.create_new_empty()

# Read OUT file
flooded = set()
while date < end_date:
    output_file = forecast_dir+'/output'+'%8.8d'%0
    out = read_out_file(output_file)
    for conduit in links.keys():
        if conduit[-1] != 'R': continue
        flooding = out.get_part('link', conduit).depth
        if not flooding[flooding > 0.4].empty:
            print(conduit, flooding.max)
            flooded.add(conduit)
    date += datetime.timedelta(minutes=cycle)
    analysis_date = date.strftime('%Y%m%d%H%M')
    forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date

for conduit in flooded:
    roads[conduit] = links[conduit]
    # xroads[conduit] = inp.XSECTIONS[conduit]

flood_template_file = const_dir+'/flood.txt'
flood_inp = read_inp_file(flood_template_file)
flood_inp.add_new_section(roads)
print(flood_inp.CONDUITS)
# flood_inp.add_new_section(xroads)
flood_inp.write_file(flood_file)