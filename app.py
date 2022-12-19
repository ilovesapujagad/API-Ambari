import json
import os
import requests
import io
import time
from sys import stderr
from flask import Flask, request, jsonify, send_file
from requests.auth import HTTPBasicAuth
from paramiko import SSHClient, AutoAddPolicy

app = Flask(__name__)

api_key = os.environ.get("API_KEY", "")
if api_key == "":
    print("api key is required", file=stderr)

api_base_url = "https://api.stagingv3.microgen.id/query/api/v1/" + api_key

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask</h2>'

@app.get("/hosts")
def hosts():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/hosts?fields=Hosts/rack_info,Hosts/host_name,Hosts/maintenance_state,Hosts/public_host_name,Hosts/cpu_count,Hosts/ph_cpu_count,alerts_summary,Hosts/host_status,Hosts/host_state,Hosts/last_heartbeat_time,Hosts/ip,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/service_name,host_components/HostRoles/display_name,host_components/HostRoles/desired_admin_state,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/dfs/FSNamesystem/HAState,Hosts/total_mem,stack_versions/HostStackVersions,stack_versions/repository_versions/RepositoryVersions/repository_version,stack_versions/repository_versions/RepositoryVersions/id,stack_versions/repository_versions/RepositoryVersions/display_name&minimal_response=true,host_components/logging&page_size=10&from=0&sortBy=Hosts/host_name.asc&_=1671421446029'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))
    return response.json()

@app.get("/host/memory")
def hostMem():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/hosts?fields=metrics/memory/mem_total,metrics/memory/mem_free,metrics/memory/mem_cached'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/host/cpu")
def hostCPU():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/hosts?fields=metrics/cpu/cpu_wio&_=1671421419180'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/host/disk")
def hostDisk():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/hosts?fields=metrics/disk/disk_free,metrics/disk/disk_total&_=1671421419194'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hive/summary")
def hive():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER%7CServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name&minimal_response=true&_=1667968440999'
    username = "sapujagad"
    password = "kayangan"
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
    url = 'http://10.207.26.20:8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/rename'
    username = "sapujagad"
    password = "kayangan"
    data = request.get_json()
    response = requests.post(url,json = data, auth = HTTPBasicAuth(username, password))
   

    return response.json()

def urlDownload(path):
    extensions = ['.png','jpg','jpeg','pdf', '.doc','.docx', '.xls','xlsx','.csv', '.tsv']
    if all(ext not in path for ext in extensions):
        username = "sapujagad"
        password = "kayangan"
        url = 'http://10.207.26.20:8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/download/zip/generate-link'
        #data = request.get_json()
        data = {"download":True,
                "entries":["/"+ path]}
        response= requests.post(url, json=data, auth = HTTPBasicAuth(username, password))
        x = response.json()
        url1='http://10.207.26.20:8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/download/zip?requestId='
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
        username = "sapujagad"
        password = "kayangan" 
        url = 'http://10.207.26.20:80800/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/download/browse?path=/' + path + '&download=true'
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
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/services/HDFS/components/DATANODE?fields=host_components/metrics/dfs/datanode/bytes_written&format=null_padding&_=1669268400225'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/gctime")
def hdfsGCTime():
    url = 'http:/10.207.26.20:8080/api/v1/clusters/gudanggaram/services/HDFS/components/DATANODE?fields=host_components/metrics/jvm/gcTimeMillis&format=null_padding&_=1669268400267'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/memuse")
def hdfsMemUsed():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/services/HDFS/components/DATANODE?fields=host_components/metrics/jvm/memHeapUsedM&format=null_padding&_=1669268400291'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/memcommit")
def hdfsMemCommit():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/services/HDFS/components/DATANODE?fields=host_components/metrics/jvm/memHeapCommittedM&format=null_padding&_=1669268400306'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/processdisk")
def hdfsProcessDisk():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/services/HDFS/components/DATANODE?fields=host_components/metrics/dfs/datanode/bytes_read,host_components/metrics/dfs/datanode/bytes_written,host_components/metrics/dfs/datanode/TotalReadTime,host_components/metrics/dfs/datanode/TotalWriteTime&format=null_padding&_=1669268400419'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/processnet")
def hdfsProcessNet():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/services/HDFS/components/DATANODE?fields=host_components/metrics/dfs/datanode/RemoteBytesRead,host_components/metrics/dfs/datanode/reads_from_remote_client,host_components/metrics/dfs/datanode/RemoteBytesWritten,host_components/metrics/dfs/datanode/writes_from_remote_client&format=null_padding&_=1669268400463'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/spaceutil")
def hdfsSpaceUtil():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/services/HDFS/components/DATANODE?fields=host_components/metrics/FSDatasetState/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/FsDatasetImpl/Remaining,host_components/metrics/dfs/datanode/Capacity&format=null_padding&_=1669268400383'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x

@app.get("/hdfs/bytesr")
def hdfsBytesRead():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/services/HDFS/components/DATANODE?fields=host_components/metrics/dfs/datanode/bytes_read&format=null_padding&_=1669268400355'
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth = HTTPBasicAuth(username, password))

    x = response.json()
    return x
@app.get("/namenode/cpu")
def namenodeCPU():
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    username = "admin"
    password = "admin"
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
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    username = "admin"
    password = "admin"
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
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    username = "admin"
    password = "admin"
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
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    username = "admin"
    password = "admin"
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
    url = 'http://10.207.26.20:8080/api/v1/clusters/gudanggaram/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1670226192151'
    username = "admin"
    password = "admin"
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
