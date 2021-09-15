from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt

def CreateMapBackground(edges=(-180,180,-90,90),buffer=0):
    '''Edges = (Minimum Longitude, Maximum Longitude, Minimum Latitude, Maximum Latitude)
       Buffer is the number of degrees between Min/Max Lon/Lat around the map'''
    m = Basemap(llcrnrlon=edges[0]-buffer, llcrnrlat=edges[2]-buffer,urcrnrlon=edges[1]+buffer,urcrnrlat=edges[3]+buffer,lon_0=0,lat_0=0)
    m.drawmapboundary(fill_color='#A6CAE0', linewidth=0)
    m.fillcontinents(color='grey', alpha=0.7, lake_color='grey')
    m.drawcoastlines(linewidth=0.1, color="white")
    return m
