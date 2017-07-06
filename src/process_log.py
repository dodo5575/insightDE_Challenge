import sys, json
import numpy as np
import pandas as pd


########################## Load the batch log
events = []
for line in open (sys.argv[1], 'r'):
    if line != '\n':
        events.append(json.loads(line))

# Collect parameters D and T
par = events.pop(0)


# Convert events record to pandas DataFrame
events_df = pd.DataFrame(events)


# Collect purchase history
pHis_df = events_df[events_df.id.notnull()]
pHis_df = pHis_df.drop(['id1', 'id2', 'event_type'], axis=1)
pHis_df['amount'] = pHis_df['amount'].astype('float')
pHis_df['timestamp'] = pd.to_datetime(pHis_df.timestamp)
pHis_df = pHis_df.set_index('id')


print('Finished loading batch log.')
print('Start building social network...')

########################## Build the network

def buildNetwork(event, id1, id2):

    global network_dict

    # If the event is 'befriend'
    if event == 'befriend':

        # If id1 has not been added to network_dict
        if id1 not in network_dict:
            network_dict[ id1 ] = np.array([id2])

        # If id1 has been added to network_dict, simply append to the friend list
        else:
            network_dict[ id1 ] = np.append(network_dict[ id1 ], id2)
    
        # Repeat with id2
        if id2 not in network_dict:
            network_dict[ id2 ] = np.array([id1])
        else:
            network_dict[ id2 ] = np.append(network_dict[ id2 ], id1)
    
    
    # If the event is 'unfriend'
    elif event == 'unfriend':
    
        # If id1 has not been added to network_dict or id2 was not in the friend list of id1
        if id1 in network_dict and id2 in network_dict[ id1 ]:
            network_dict[ id1 ] = np.delete(network_dict[ id1 ], np.where(network_dict[ id1 ] == id2))
    
        # Repeat with id2
        if id2 in network_dict and id1 in network_dict[ id2 ]:
            network_dict[ id2 ] = np.delete(network_dict[ id2 ], np.where(network_dict[ id2 ] == id1))

    return



network_dict = {}

# if a record has id1 and id2, it is a friendship event.
for e, id1, id2 in events_df.loc[ events_df.id1.notnull() & events_df.id2.notnull(), ['event_type', 'id1', 'id2']].values:

    buildNetwork(e, id1, id2)


print('Finished building social network.')
print('Start loading streaming data...')

########################## Collect streaming data

def findGroup(currentD, id):

    global group_Array

    group_Array = np.append(group_Array, id)

    # If current degree has reached the D parameter, terminate.
    if currentD == int(par['D']):
        return
    else:
        
        # An isolated user could have a purchase record 
        if id not in network_dict:
            return

        # Recursively loop through each friend in the list.
        else:
            friend_list = network_dict[id]
            for i in friend_list:
            
                findGroup(currentD + 1, i)
            return 


# Load the streaming data
stream = []
for line in open (sys.argv[2], 'r'):
    if line != '\n':
        stream.append(json.loads(line))

stream_df = pd.DataFrame(stream)

with open(sys.argv[3], 'w') as output:
    for i in stream_df.index.values:
    
    
        # If this is not a purchase record, update the network.
        if pd.isnull(stream_df.iloc[i].loc['id']):
            e, id1, id2 = stream_df.iloc[i].loc[['event_type', 'id1', 'id2']].values
            
            buildNetwork(e, id1, id2)
    
    
        # If this is a purchase record, check for anamoly.
        else: 
    
            id = stream_df.iloc[i].loc['id']
            group_Array = np.array([])
            
                    
            findGroup(0, id)
            group_Array = np.unique(group_Array)
            group_Array = np.delete(group_Array, np.where(group_Array == id))
            
    
            # Find the intersection between the local group and the ids in the purchase history
            intersect_Array = np.intersect1d(group_Array, pHis_df.index.values)
            if len(intersect_Array) > 0:
            
                pHis_local_df = pHis_df.loc[intersect_Array]
                pHis_local_df = pHis_local_df.dropna(axis=0)
    
                # Only check anomaly if there are more than 2 purchase history
                if len(pHis_local_df) > 2:
                    pHis_local_df = pHis_local_df.sort_values(by='timestamp')
    
                    pHis_local_new_df = pHis_local_df[-int(par['T']):]       

                    # If the purchase amount is larger than 3 * std + mean, it is an anomaly.    
                    if float(stream_df.iloc[i].loc['amount']) > float(pHis_local_new_df.amount.std(ddof=0) * 3 + pHis_local_new_df.amount.mean()):
                        output.write('{"event_type":"%s", "timestamp":"%s", "id": "%s", "amount": "%s", "mean": "%.2f", "sd": "%.2f"}\n' \
                        % (stream_df.iloc[i].loc['event_type'],
                           stream_df.iloc[i].loc['timestamp'],
                           stream_df.iloc[i].loc['id'],
                           stream_df.iloc[i].loc['amount'],
                           np.floor(100*pHis_local_new_df.amount.mean())/100,
                           np.floor(100*pHis_local_new_df.amount.std(ddof=0))/100))






