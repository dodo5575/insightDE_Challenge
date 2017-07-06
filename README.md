# insightDE_Challenge
The code, process_log.py, is in the "src" folder. It is written in Python 3.5. It requires module sys, json, numpy 1.12.1 and pandas 0.20.1.

Based on the task, the code is divided into three parts:

1. Load the batch log data: In this part, the batch log file will be read. The parameters 'D' and 'T' will be extracted and stored in a dictionary called 'par'. Then, the purchase history of each id will be converted to a pandas DataFrame called 'pHis_df'.

2. Build the network: The friendship information from the batch log will be processed to build the social network. Since the friendship is bi-directional, I used a dictionary called 'network_dict', with keys for each id and values for the friend list of given id.  

3. Collect streaming data: In this part, the streaming log will be loaded and processed one record at a time. If the record is a friendship event ('befriend' or 'unfriend'), this record will be used to update the social network. If it is a purchase event, I wrote a function 'findGroup' to recursively traverse the local network given the user id of the event and the degree parameter 'D'. The function will store all of the neighbors of the given id in an array 'group_Array'. Next, all of the purchase history from those neighbors, sorted by the timestamp and with the most recent 'T' number of purchase, will be extracted into a pandas DataFrame 'pHis_local_new_df'. Finally, if the purchase amount of this record is larger than three standard deviations from the mean of the recent purchase history of the neighbors, this record is flagged as an anomaly and saved in the output file.




