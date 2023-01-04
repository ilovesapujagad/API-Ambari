import json
import os
import requests
import io
import time
import datetime
from sys import stderr
from flask import Flask, request, jsonify, send_file
from requests.auth import HTTPBasicAuth
from paramiko import SSHClient, AutoAddPolicy

app = Flask(__name__)

api_key = os.environ.get("API_KEY", "")
if api_key == "":
    print("api key is required", file=stderr)

api_base_url = "https://api.stagingv3.microgen.id/query/api/v1/" + api_key
#baseurl= "10.207.26.20"
baseurl= "10.10.65.1"
username= "sapujagad"
password=  "kayangan"
cluster = "sapujagad"
@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask</h2>'

@app.get("/hosts")
def hosts():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/hosts?fields=Hosts/rack_info,Hosts/host_name,Hosts/maintenance_state,Hosts/public_host_name,Hosts/cpu_count,Hosts/ph_cpu_count,alerts_summary,Hosts/host_status,Hosts/host_state,Hosts/last_heartbeat_time,Hosts/ip,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/service_name,host_components/HostRoles/display_name,host_components/HostRoles/desired_admin_state,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/dfs/FSNamesystem/HAState,Hosts/total_mem,stack_versions/HostStackVersions,stack_versions/repository_versions/RepositoryVersions/repository_version,stack_versions/repository_versions/RepositoryVersions/id,stack_versions/repository_versions/RepositoryVersions/display_name&minimal_response=true,host_components/logging&page_size=10&from=0&sortBy=Hosts/host_name.asc&_=1671421446029'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))
    return response.json()

@app.get("/host/memory")
def hostMem():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/hosts?fields=metrics/memory/mem_total,metrics/memory/mem_free,metrics/memory/mem_cached'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/host/cpu")
def hostCPU():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/hosts?fields=metrics/cpu/cpu_wio&_=1671421419180'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/host/disk")
def hostDisk():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/hosts?fields=metrics/disk/disk_free,metrics/disk/disk_total&_=1671421419194'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hive/summary")
def hive():
    url = 'http:/'+baseurl+':8080/api/v1/clusters/'+cluster+'/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER%7CServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name&minimal_response=true&_=1667968440999'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    d = dict()
    b = 0
    d['items'] = {}
    for i in range(len(x["items"])):
        if x["items"][i]["ServiceComponentInfo"]["service_name"] == "HIVE":
            a = x["items"][i]
            d['items'][b] = a
            b += 1
    
    return d

@app.post("/hdfs/rename")
def hdfsRename():
    url = 'http://'+baseurl+':8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/rename'
    data = request.get_json()
    response = requests.post(url,json = data, auth = HTTPBasicAuth(username, password))
   

    return response.json()

def urlDownload(path):
    extensions = ['.png','jpg','jpeg','pdf', '.doc','.docx', '.xls','xlsx','.csv', '.tsv']
    if all(ext not in path for ext in extensions):
        url = 'http://'+baseurl+':8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/download/zip/generate-link'
        #data = request.get_json()
        data = {"download":True,
                "entries":["/"+ path]}
        response= requests.post(url, json=data, auth = HTTPBasicAuth(username, password))
        x = response.json()
        url1='http:/'+baseurl+':8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/download/zip?requestId='
        url1 += x['requestId']
        response1 = requests.get(url1, auth = HTTPBasicAuth(username, password))

        x = path 
        last = x.rsplit('/', 1)[-1]+".zip"

        z = response1.content
        memory_file = last
        with open(memory_file, 'wb') as zf:
            zf.write(z)
        
        return send_file(memory_file, attachment_filename=last, as_attachment=True)
    else:   
        url = 'http://'+baseurl+':8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/download/browse?path=/' + path + '&download=true'
        response = requests.get(url, auth = HTTPBasicAuth(username, password))
        x = path 
        last = x.rsplit('/', 1)[-1]
        return send_file(io.BytesIO(response.content), attachment_filename=last, as_attachment=True)
    
@app.get("/hdfs/download/<path:path>")
def hdfsDownload(path):
    zf = urlDownload(path)
    return zf


@app.get("/hdfs/bytesw")
def hdfsBytesWrite():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/services/HDFS/components/DATANODE?fields=host_components/metrics/dfs/datanode/bytes_written&format=null_padding&_=1669268400225'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))
    x = response.json()
    return x

@app.get("/hdfs/gctime")
def hdfsGCTime():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/services/HDFS/components/DATANODE?fields=host_components/metrics/jvm/gcTimeMillis&format=null_padding&_=1671422921134'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/memuse")
def hdfsMemUsed():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/services/HDFS/components/DATANODE?fields=host_components/metrics/jvm/memHeapUsedM&format=null_padding&_=1669268400291'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/memcommit")
def hdfsMemCommit():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/services/HDFS/components/DATANODE?fields=host_components/metrics/jvm/memHeapCommittedM&format=null_padding&_=1669268400306'
    username = "admin"
    password = "admin"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/processdisk")
def hdfsProcessDisk():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/services/HDFS/components/DATANODE?fields=host_components/metrics/dfs/datanode/bytes_read,host_components/metrics/dfs/datanode/bytes_written,host_components/metrics/dfs/datanode/TotalReadTime,host_components/metrics/dfs/datanode/TotalWriteTime&format=null_padding&_=1669268400419'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/processnet")
def hdfsProcessNet():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/services/HDFS/components/DATANODE?fields=host_components/metrics/dfs/datanode/RemoteBytesRead,host_components/metrics/dfs/datanode/reads_from_remote_client,host_components/metrics/dfs/datanode/RemoteBytesWritten,host_components/metrics/dfs/datanode/writes_from_remote_client&format=null_padding&_=1669268400463'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/spaceutil")
def hdfsSpaceUtil():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/services/HDFS/components/DATANODE?fields=host_components/metrics/FSDatasetState/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl/Remaining,host_components/metrics/dfs/datanode/Capacity&format=null_padding&_=1669268400383'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/bytesr")
def hdfsBytesRead():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/services/HDFS/components/DATANODE?fields=host_components/metrics/dfs/datanode/bytes_read&format=null_padding&_=1669268400355'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x
@app.get("/namenode/cpu")
def namenodeCPU():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    d = dict()
    b = 0
    d['items'] = {}
    for i in range(len(x["items"])):
        if x["items"][i]["ServiceComponentInfo"]["component_name"] == "NAMENODE":
            a = x["items"][i]["host_components"][0]['metrics']["cpu"]
            d['items'][b] = a
            b += 1

    return d

@app.get("/namenode/rpc")
def namenodeRPC():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    d = dict()
    b = 0
    d['items'] = {}
    for i in range(len(x["items"])):
        if x["items"][i]["ServiceComponentInfo"]["component_name"] == "NAMENODE":
            a = x["items"][i]["host_components"][0]['metrics']["rpc"]
            d['items'][b] = a
            b += 1

    return d

@app.get("/namenode/uptime")
def namenodeRunTime():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    d = dict()
    b = 0
    d['items'] = {}
    for i in range(len(x["items"])):
        if x["items"][i]["ServiceComponentInfo"]["component_name"] == "NAMENODE":
            a = x["items"][i]["host_components"][0]['metrics']["runtime"]
            d['items'][b] = a
            b += 1

    return d

@app.get("/namenode/hdfs")
def namenodeHDFS():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    d = dict()
    b = 0
    d['items'] = {}
    for i in range(len(x["items"])):
        if x["items"][i]["ServiceComponentInfo"]["component_name"] == "NAMENODE":
            a = x["items"][i]["host_components"][0]['metrics']["dfs"]["FSNamesystem"]
            d['items'][b] = a
            b += 1

    return d

@app.get("/namenode/heap")
def namenodeHeap():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    d = dict()
    b = 0
    d['items'] = {}
    for i in range(len(x["items"])):
        if x["items"][i]["ServiceComponentInfo"]["component_name"] == "NAMENODE":
            a = x["items"][i]["host_components"][0]['metrics']["jvm"]
            d['items'][b] = a
            b += 1

    return d

@app.get("/filesystem")
def hashtags():
    source = str(request.args.get('source'))
    a = source.replace("/", "%2F")
    url="http://"+baseurl+":8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/listdir?nameFilter=&path="+ a +"&_=1667963722829"
    response = requests.get(url, auth=(username, password))
    return response.json()

@app.post("/hdfs/restart")
def restart():
    url="http://"+baseurl+":8080/api/v1/clusters/"+cluster+"/requests"
    payload = '{"RequestInfo":{"context":"Execute REFRESH_NODES","command":"REFRESH_NODES"},"Requests/resource_filters":[{"service_name":"HDFS","component_name":"NAMENODE","hosts":"sapujagad-master01.kayangan.com"}]}'
    response = requests.post(url, auth=(username, password), data=payload)
    # print(response.status_code)
    return response.json()

@app.put("/hdfs/stop")
def stop():
    url="http://"+baseurl+":8080/api/v1/clusters/"+cluster+"/services/HDFS"
    payload = '{"RequestInfo":{"context":"_PARSE_.STOP.HDFS","operation_level":{"level":"SERVICE","cluster_name":"gudanggaram","service_name":"HDFS"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'
    response = requests.put(url, auth=(username, password), data=payload)
    # print(response.status_code)
    return response.json()

@app.put("/hdfs/start")
def start():
    url="http://"+baseurl+":8080/api/v1/clusters/"+cluster+"/services/HDFS"
    payload = '{"RequestInfo":{"context":"_PARSE_.START.HDFS","operation_level":{"level":"SERVICE","cluster_name":"gudanggaram","service_name":"HDFS"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}'
    response = requests.put(url, auth=(username, password), data=payload)
    # print(response.status_code)
    return response.json()

@app.get("/sqoop/summary")
def check():
    url="http://"+baseurl+":8080/api/v1/clusters/"+cluster+"/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name&minimal_response=true&_=1667968440999"

    response = requests.get(url, auth=(username, password))

    x = str(response.json())
    z = x.replace("'", '"' )
    a = z.replace('{"items":', "")
    b = a[:-1]
    data = json.loads(b)
    return jsonify(data)

@app.post("/mkdir")
def mkdir():
    url="http://"+baseurl+":8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/mkdir"
    content = request.json
    path = str(content['path'])
    response = requests.put(url, auth=(username, password), json={"path": path})
    return response.json() 

@app.post("/sqoop/restart")
def srestart():
    url="http://"+baseurl+":8080/api/v1/clusters/"+cluster+"/requests"
    payload = '{"RequestInfo":{"command":"RESTART","context":"Restart all components for SQOOP","operation_level":{"level":"SERVICE","cluster_name":"sapujagad","service_name":"SQOOP"}},"Requests/resource_filters":[{"service_name":"SQOOP","component_name":"SQOOP","hosts":"sapujagad-edge01.kayangan.com,sapujagad-edge02.kayangan.com,sapujagad-master01.kayangan.com,sapujagad-master02.kayangan.com,sapujagad-worker01.kayangan.com,sapujagad-worker02.kayangan.com,sapujagad-worker03.kayangan.com,sapujagad-worker04.kayangan.com,sapujagad-worker05.kayangan.com"}]}'
    response = requests.post(url, auth=(username, password), data=payload)
    # print(response.status_code)
    return response.json()

@app.delete("/hdfs/file")
def deletefile():
    url="http://"+baseurl+":8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/moveToTrash"
    content = request.json
    path = str(content['path'])
    recursive = "true"
    response = requests.post(url, auth=(username, password), json={"paths":[{"path": path,"recursive":recursive}]})
    # print(response.status_code)
    return response.json()


@app.delete("/hdfs/file/permanent")
def deletefilepermanent():
    url="http://"+baseurl+":8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/remove"
    content = request.json
    path = str(content['path'])
    recursive = "true"
    headers = {"X-Requested-By": "admin"}
    response = requests.post(url,headers=headers, auth=(username, password), json={"paths":[{"path": path,"recursive":recursive}]})
    # print(response.status_code)
    return response.json()

class my_dictionary(dict):        
    def __init__(self):
        self = dict()

    # Function to add key:value
    def add(self, key, value):
        self[key] = value
    
@app.get("/check/host")
def checkhost():
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/hosts?fields=metrics/disk/disk_free,metrics/disk/disk_total,metrics/load/load_one&minimal_response=true&page_size=100&from=0'
    response = requests.get(url, auth=(username, password))
    x = response.json()
    a = x['items']
    # [0]['Hosts']['host_name']   
    
    n = len(a)
    dict_obj = my_dictionary()   
    for  user in a:
        for i in range(0, n):
            dict_obj.add(i,user['Hosts']['host_name'])
    return dict_obj

@app.post("/hdfs/permission")
def hdfspermission():
    url = 'http://'+baseurl+':8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/chmod'
    content = request.json
    path = str(content['path'])
    mode = str(content['mode'])
    response = requests.post(url, auth=(username, password),json={"mode":mode,"path": path})
    return response.json()
class my_dictionary(dict):
    def __init__(self):
        self = dict()

    # Function to add key:value
    def add(self, key, value):
        self[key] = value

@app.get("/dashboard/metrics/cpu")
def metricscpu():
    d=str(datetime.datetime.now())
    p='%Y-%m-%d %H:%M:%S.%f'
    e = int(time.mktime(time.strptime(d,p))) - 3600
    x = str(e)
    url="http://"+baseurl+":8080/api/v1/"+cluster+"/sapujagad/hosts/sapujagad-master01.kayangan.com?fields=metrics/cpu&_="+x+""
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
    return response.json()

@app.get("/dashboard/metrics/avgcpu")
def metricsavgcpu():
    d=str(datetime.datetime.now())
    p='%Y-%m-%d %H:%M:%S.%f'
    a = int(time.mktime(time.strptime(d,p)))
    e = int(time.mktime(time.strptime(d,p))) - 3600
    z = str(a)
    x = str(e)
    url="http://"+baseurl+":8080/api/v1/clusters/"+cluster+"?fields=metrics/cpu/Nice._avg["+x+","+z+",15],metrics/cpu/System._avg["+x+","+z+",15],metrics/cpu/User._avg["+x+","+z+",15],metrics/cpu/Idle._avg["+x+","+z+",15]&_="+z+""
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
    return response.json()

@app.get("/dashboard/metrics/avgnetwork")
def metricsavgnetwork():
    d=str(datetime.datetime.now())
    p='%Y-%m-%d %H:%M:%S.%f'
    a = int(time.mktime(time.strptime(d,p)))
    e = int(time.mktime(time.strptime(d,p))) - 3600
    z = str(a)
    x = str(e)
    url="http://"+baseurl+":8080/api/v1/clusters/"+cluster+"/?fields=metrics/network/In._avg["+x+","+z+",15],metrics/network/Out._avg["+x+","+z+",15]&_="+z+""
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
    return response.json()

@app.get("/dashboard/metrics/clusterload")
def metricsclusterload():
    d=str(datetime.datetime.now())
    p='%Y-%m-%d %H:%M:%S.%f'
    a = int(time.mktime(time.strptime(d,p)))
    e = int(time.mktime(time.strptime(d,p))) - 3600
    z = str(a)
    x = str(e)
    url="http://"+baseurl+":8080/api/v1/clusters/"+cluster+"/?fields=metrics/load/1-min._avg["+x+","+z+",15],metrics/load/CPUs._avg["+x+","+z+",15],metrics/load/Nodes._avg["+x+","+z+",15],metrics/load/Procs._avg["+x+","+z+",15]&_="+z+""
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
    return response.json()


@app.get("/dashboard/metrics/avgmemoryusage")
def metricsavgmemoryusage():
    d=str(datetime.datetime.now())
    p='%Y-%m-%d %H:%M:%S.%f'
    a = int(time.mktime(time.strptime(d,p)))
    e = int(time.mktime(time.strptime(d,p))) - 3600
    z = str(a)
    x = str(e)
    url="http://"+baseurl+":8080/api/v1/clusters/"+cluster+"/?fields=metrics/memory/Buffer._avg["+x+","+z+",15],metrics/memory/Cache._avg["+x+","+z+",15],metrics/memory/Share._avg["+x+","+z+",15],metrics/memory/Swap._avg["+x+","+z+",15],metrics/memory/Total._avg["+x+","+z+",15],metrics/memory/Use._avg["+x+","+z+",15]&_="+z+""
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
    return response.json()

@app.get("/heatmets/yarn/totalallocatableram")
def totalallocatableram():
    d=str(datetime.datetime.now())
    p='%Y-%m-%d %H:%M:%S.%f'
    a = int(time.mktime(time.strptime(d,p)))
    z = str(a)
    url='http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/services/YARN/components/NODEMANAGER?fields=host_components/metrics/yarn/AllocatedGB,host_components/metrics/yarn/AvailableGB&format=null_padding&_='+z+''
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
    return response.json()

@app.get("/heatmaps")
def heatmaps():
    d=str(datetime.datetime.now())
    p='%Y-%m-%d %H:%M:%S.%f'
    a = int(time.mktime(time.strptime(d,p)))
    z = str(a)
    url = 'http://'+baseurl+':8080/api/v1/clusters/'+cluster+'/hosts?fields=Hosts/rack_info,Hosts/host_name,Hosts/maintenance_state,Hosts/public_host_name,Hosts/cpu_count,Hosts/ph_cpu_count,alerts_summary,Hosts/host_status,Hosts/host_state,Hosts/last_heartbeat_time,Hosts/ip,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/service_name,host_components/HostRoles/display_name,host_components/HostRoles/desired_admin_state,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/dfs/FSNamesystem/HAState,Hosts/total_mem,stack_versions/HostStackVersions,stack_versions/repository_versions/RepositoryVersions/repository_version,stack_versions/repository_versions/RepositoryVersions/id,stack_versions/repository_versions/RepositoryVersions/display_name&minimal_response=true,host_components/logging&page_size=100&from=0&sortBy=Hosts/host_name.asc&_='+z+''
    response = requests.get(url, auth=(username, password))
    x = response.json()
    a = x['items']
    return a 

@app.get("/pythonPackage")
def pythonPackage():
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())

    client.connect('10.207.26.22', username="apps", password="apps247")
    stdin, stdout, stderr = client.exec_command('python3 -m pip list --format json\n')
    time.sleep(2)
    x = json.load(stdout)
    d = dict()
    d["items"]= {}
    d["items"] = x
    return  d

if __name__ == "__main__":
    app.run(debug=True)
