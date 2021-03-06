import requests
from datetime import datetime
from datetime import timedelta as timedelta
import dendra_api_client as dendra
import pandas

# Get annotations related to a datastream
# between a start and end date.
# Return a list of times, annotation values, and labels for the annotation.

def getAnnotations(datastream, begins_at, ends_before):
    # nitrate mgl = 5f5988118265505b31a142c1
    # nitrate uM = 5f58fb5b826550070aa14299
    url = 'https://api.edge.dendra.science/v2/'
    headers = {"Content-Type": "application/json"}
    # "actions":[{"attrib":{"TempCHandMeter":18.9}}]
    query = {
        'datastream_ids': datastream,  # 'datastream_ids[$in]' : [wtemp, nitrate],
        '$sort[title]': 1,
        '$select[title]': 1,
        'intervals.begins_at[$gte]': begins_at,
        'intervals.ends_before[$lt]': ends_before,
        '$select[intervals]': 1,
        '$select[actions.flag]': 1,
        '$select[actions.exclude]': 1,
        '$select[actions.attrib.TempCHandMeter]': 1,
        '$select[actions.attrib.Water_Temp_C]': 1,
        '$select[actions.attrib.HandMeterWater_Temp_C]': 1,
        '$select[actions.attrib.NO3uM]': 1,
        '$select[actions.attrib.GrabsampleNO3_uM]': 1,
        '$select[actions.attrib.Salinity_PSU]': 1,
        '$select[actions.attrib.HandMeterSalinity_PSU]': 1,
        '$select[actions.attrib.DO_Percent_Sat]': 1,
        '$select[actions.attrib.HandMeterDO_Percent_Sat]': 1,
        '$select[actions.attrib.HandMeterDO_MgL]': 1,
        '$select[actions.attrib.DO_MgL]': 1,
        '$select[actions.attrib.fDOM_QSU]': 1,
        '$select[actions.attrib.BGA_PE_ug_L]': 1,
        '$select[actions.attrib.Absorbance254nm]': 1,
        '$select[actions.attrib.Absorbance350nm]': 1,
        '$select[actions.attrib.Bromide_mg_L_]': 1,
        '$select[actions.attrib.Chlorophyll_ug_L_1]': 1,
        '$select[actions.attrib.Chlorophyll_ug_L_2]': 1,
        '$select[actions.attrib.ODO_mg_L]': 1,
        '$select[actions.attrib.Nitrate__uM_]': 1,
        '$select[actions.attrib.Nitrate_mg_L_]': 1,
        '$select[actions.attrib.RMSE_fit_perc]': 1,
        '$select[actions.attrib.Relative_Humidity_perc]': 1,
        '$select[actions.attrib.Sal_psu]': 1,
        '$select[actions.attrib.SpCond_mS_cm]': 1,
        '$select[actions.attrib.Spectrum_Average]': 1,
        '$select[actions.attrib.Turbidity_FNU]': 1,
        '$select[actions.attrib.Depth_m]': 1,
        '$select[actions.attrib.Temp_deg_C]': 1,
        '$limit': 2016
    }
    # Request JSON from Dendra
    r = requests.get(url + 'annotations', headers=headers, params=query)
    print(r)
    assert r.status_code == 200
    anno_station = r.json()['data']
    print('Number of Buoy annotations:', len(anno_station))
    wtemp = []
    times = []
    labels = []
    annoindex = 0
    lastannoindex = 0
    actionvalue = None
    tmpval = None
    tmplabel = None
    for annodict in anno_station:
        lastannoindex = annoindex
        annoindex += 1
        # print(annodict)
        # print(annodict)
        for key, values in annodict.items():
            # print(key + ' ' + str(values))
            if key == 'actions':
                actdict = values[0]
                for key2, values2 in actdict.items():
                    isadict = True
                    try:
                        tmpval = values2[0]
                    except (KeyError, TypeError):
                        # print('not a dict')
                        isadict = False
                    # print(tmpval)
                    if isinstance(values2, dict) or (isadict and not isinstance(values2, list)):
                        for key3, values3 in values2.items():
                            wtemp.append(values3)
                            actionvalue = values3
                    else:
                        actionvalue = values2
                        wtemp.append(values2)
            elif key == 'intervals':
                actdict = values[0]
                secondval = False
                for key2, values2 in actdict.items():
                    # print(values2)
                    # print(type(values2))
                    if secondval:
                        # print('SECOND DT')
                        wtemp.append(actionvalue)
                        labels.append(tmplabel)
                    datet = datetime.strptime(values2, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M')
                    # dt = dt.strftime(dt, '')
                    times.append(datet)
                    secondval = True
            elif key == 'title':
                labels.append(values)
                tmplabel = values
    # print(anno_station)
    print(times)
    print(wtemp)
    # annotationdf = {'datetime': times, 'annoataionwtemp': wtemp}
    return times, wtemp, labels

# get data points for a datastream between a start and end date.
# associate annotations that are flags to data points that are flagged.
# return annotations and datapoints.
def getDSvalsAddLabels(datastream,  begins_at, ends_before, annotimes, annovals, labels):
    df = dendra.get_datapoints(datastream,begins_at,ends_before,time_type='utc')
    print(type(df))
    colname = ''
    for col in df.columns:
        if 'Unh' in col:
          colname = col
          print(col)
    count = 0
    count2 = 0
    newtemps = []
    newtimes = []
    newlabels = []
    for time, wtem, label in zip(annotimes, annovals, labels):
        for index, row in df.iterrows():
            # print(index)
            if type(index) is pandas.Timestamp:
                dftimetemplow = index - timedelta(minutes=5)
                dftimetemphigh = index + timedelta(minutes=5)
            else:
                dftimetemplow = datetime.strptime(index, '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(minutes=5)
                dftimetemphigh = datetime.strptime(index, '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(minutes=5)
            # dftimetemplow = dftimetemplow.strftime('%Y-%m-%d %H:%M')
            # dftimetemphigh = dftimetemphigh.strftime('%Y-%m-%d %H:%M')
            # dftimetemp = datetime.strptime(row['timestamp_local'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d %H:%M')
            annotime = datetime.strptime(time, '%Y-%m-%d %H:%M')
            count2 += 1

            if annotime >= dftimetemplow and annotime <=dftimetemphigh:
                count += 1
                # print(annotime)
                # print(annotime >= dftimetemplow)
                # print(annotime <= dftimetemphigh)
                # print(dftimetemplow)
                # print(dftimetemphigh)
                try:
                    tempval = float(wtem)
                    newtemps.append(wtem)
                    newtimes.append(row['timestamp_local'])
                    newlabels.append(label)
                except:
                    # tempval = row['UnhGreatBayBuoy_Water_Temperature']
                    # print('HERE 2')
                    # print(label)
                    # print(annotime)
                    # print(row[colname])
                    # print(row['timestamp_local'])
                    newtemps.append(row[colname])
                    newtimes.append(row['timestamp_local'])
                    newlabels.append(label)
                # print(time)
               #  print(wtemp)
    # print(count2)
    # print(newtimes)

    times = newtimes # annotimes +
    annovals =  newtemps # annovals +
    labels =  newlabels #
    # print(times)
    # print(wtemp)
    # print(count)
    # print(labels)
    annotationdf = {'datetime': times, 'annotaions':annovals, 'labels': labels}
    return annotationdf, df
