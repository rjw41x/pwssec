'''
Source| index| job| ip| vm| host| program| deployment|
geoip| region_name| latitude| ip| area_code| continent_code| country_code3| country_code2| city_name| longitude| timezone| country_name| postal_code| real_region_name| dma_code| location|
geoip| region_name| latitude| ip| continent_code| country_code3| country_code2| city_name| longitude| country_name| timezone| real_region_name| location|
UAA| origin| thread_name| type| pid| identity_zone_id| data| principal|
UAA| origin| thread_name| type| pid| remote_address| identity_zone_id| data| principal|
'''
import sys
import json
import gzip

source_keys = [ 'index', 'ip', 'job', 'vm', 'host', 'program', 'deployment' ] # 7 fields
source_dict = {}

geoip_keys = [ 'region_name', 'latitude', 'ip', 'area_code', 'continent_code', 'country_code3', 'country_code2', 'city_name', 'longitude', 'timezone', 'country_name', 'postal_code', 'real_region_name', 'dma_code', 'location' ]  #  15 fields
geoip_dict = {}

uaa_keys = [ 'origin', 'thread_name', 'type', 'pid', 'remote_address', 'identity_zone_id', 'data', 'principal' ]  #  8 fields
uaa_dict = {}

try:
    f = gzip.open( sys.argv[1], 'r' )
except IOError:
    print ( "Error opening file " + sys.argv[1] + " exiting" )
    sys.exit( 1 )

# debug - prints out a header for whichever list is specified
# for key in uaa_keys:
    # sys.stdout.write( key + '|')
# print

for line in f:
    line_out=[]
    # load a line of raw json
    j_content = json.loads(line)
    raw = j_content['@raw']
    try:
        sys.stdout.write( str( raw ).replace('|',' ') )
    except UnicodeEncodeError:
        sys.stdout.write( '' )

    if j_content.has_key('tags'):
        try:
            sys.stdout.write( '|'+str(j_content['tags']) )
        except UnicodeEncodeError:
            sys.stdout.write( '|'+'' )
    else:
            sys.stdout.write( '|'+'' )

    if j_content.has_key('@timer'):
        try:
            sys.stdout.write( '|'+str(j_content['@timer']) )
        except UnicodeEncodeError:
            sys.stdout.write( '|'+'' )
    else:
            sys.stdout.write( '|'+'' )

    if j_content.has_key('@timestamp'):
        try:
            sys.stdout.write( '|'+str(j_content['@timestamp']) )
        except UnicodeEncodeError:
            sys.stdout.write( '|'+'' )
    else:
            sys.stdout.write( '|'+'' )

    if j_content.has_key('@source'):
        for key in source_keys:
            if j_content['@source'][key]:
                source_dict[key] = j_content['@source'][key]
            else:
                if key == 'index':
                    source_dict[key] = j_content['@source'][key]
                else:
                    source_dict[key] = ''

        source = j_content['@source']

        for key in source_keys:
            try:
                sys.stdout.write( '|'+str( source_dict[key] ))
            except:
                sys.stdout.write( '|'+ '' )
        # print  source
    else:
        # write blank columns when we are missing the block
        for key in source_keys:
            sys.stdout.write( '|'+'' )

    if j_content.has_key('geoip'):
        for key in geoip_keys:
            if j_content['geoip'].has_key(key):
                geoip_dict[key] = j_content['geoip'][key]
            else:
                geoip_dict[key] = ''
        geoip = j_content['geoip']

        # for key in geoip_dict.keys():
        for key in geoip_keys:
            try:
                sys.stdout.write( '|'+str( geoip_dict[key] ) )
            except UnicodeEncodeError:
                sys.stdout.write( '|'+'' )
    else:
        # write blank columns when we are missing the block
        for key in geoip_keys:
            sys.stdout.write( '|'+'' )

    if j_content.has_key('UAA'):
        for key in uaa_keys:
            if j_content['UAA'].has_key(key):
                uaa_dict[key] = j_content['UAA'][key]
            else:
                uaa_dict[key] = ''
        uaa = j_content['UAA']

        # for key in geoip_dict.keys():
        for key in uaa_keys:
            try:
                sys.stdout.write( '|'+str( uaa_dict[key] ) )
            except UnicodeEncodeError:
                sys.stdout.write( '|'+'' )
    else:
        # write blank columns when we are missing the block
        for key in uaa_keys:
            sys.stdout.write( '|'+'' )
    # write out the line w/out newline to maintain all of the data
    # sys.stdout.write( '|'+line.rstrip() )
    # RJW - don't think we need the rstrip b/c we want the \n
    sys.stdout.write( '|'+line )
    # print
    # source.clear()
    # geoip.clear()
    # uaa.clear()
    source_dict.clear()
    geoip_dict.clear()
    uaa_dict.clear()
