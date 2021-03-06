import requests, json

time_start="1357880865185"
end_time="1357880943469"

job_list_url = "http://152.7.99.67:19888/ws/v1/history/mapreduce/jobs"
job_list_json = requests.get(job_list_url)

#job_list_query_url = job_list_url  + "?startedTimeBegin="+time_start+"&startedTimeEnd="+end_time
#job_list_json = requests.get(job_list_query_url)


mapStartTime = {}
mapEndTime = {}
reduceStartTime = {}
reduceShuffleTime = {}
reduceMergeTime = {}
reduceEndTime = {}
reduceBytes = {}

#print job_list_url
#print job_list_json.json()
for job in job_list_json.json()["jobs"]["job"]:
	if job["state"] != "SUCCEEDED":
		continue
	task_list_per_job_url = job_list_url + "/" + job["id"] + "/tasks"
	task_list_per_job_json = requests.get(task_list_per_job_url)
	print task_list_per_job_url
	if not ("tasks" in task_list_per_job_json.json()):
		print "Exception"
		continue
	#print task_list_per_job_json.json()
	for task in task_list_per_job_json.json()["tasks"]["task"]:
		task_list_url = task_list_per_job_url + "/" + task["id"]
		task_list_json = requests.get(task_list_url)
		attempt_list_url = task_list_url + "/attempts"
		attempt_list_json = requests.get(attempt_list_url)
		for attempt in attempt_list_json.json()["taskAttempts"]["taskAttempt"]:
			attempt_url = attempt_list_url + "/" + attempt["id"]
			attempt_json = requests.get(attempt_url)
			attempt_id = attempt_json.json()["taskAttempt"]["id"]
                        attempt_type = attempt_json.json()["taskAttempt"]["type"]
                        attempt_state = attempt_json.json()["taskAttempt"]["state"]
                        attempt_startTime = attempt_json.json()["taskAttempt"]["startTime"]
			attempt_endTime = attempt_json.json()["taskAttempt"]["finishTime"]
			if attempt_type == "MAP":
				mapStartTime[attempt_id] = int(attempt_startTime)/1000;
				mapEndTime[attempt_id] = int(attempt_endTime)/1000;
			elif attempt_type == "REDUCE":
				attempt_shuffleTime = attempt_json.json()["taskAttempt"]["shuffleFinishTime"]
				attempt_mergeTime = attempt_json.json()["taskAttempt"]["mergeFinishTime"]
				reduceStartTime[attempt_id] = int(attempt_startTime)/1000
                                reduceEndTime[attempt_id] = int(attempt_endTime)/1000
				reduceShuffleTime[attempt_id] = int(attempt_shuffleTime)/1000
				reduceMergeTime[attempt_id] = int(attempt_mergeTime)/1000

runningMaps = {}
shufflingReduces = {}
mergingReduces = {}
runningReduces = {}
startTime = min(reduce(min, mapStartTime.values()),
                reduce(min, reduceStartTime.values()))
endTime = max(reduce(max, mapEndTime.values()),
              reduce(max, reduceEndTime.values()))

#print startTime, endTime
for t in range(startTime, endTime):
	runningMaps[t] = 0
 	shufflingReduces[t] = 0
 	mergingReduces[t] = 0
 	runningReduces[t] = 0

for map in mapStartTime.keys():
 	for t in range(mapStartTime[map], mapEndTime[map]):
		runningMaps[t] += 1
for reduce in reduceStartTime.keys():
	for t in range(reduceStartTime[reduce], reduceShuffleTime[reduce]):
		shufflingReduces[t] += 1
	for t in range(reduceShuffleTime[reduce], reduceMergeTime[reduce]):
		mergingReduces[t] += 1
	for t in range(reduceMergeTime[reduce], reduceEndTime[reduce]):
		runningReduces[t] += 1

print "time maps shuffle merge reduce"
for t in range(startTime, endTime):
	print t - startTime, runningMaps[t], shufflingReduces[t], mergingReduces[t], 
	print runningReduces[t]
