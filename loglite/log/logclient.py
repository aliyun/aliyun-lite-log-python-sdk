"""
LogClient class is the main class in the SDK. It can be used to communicate with
log service server to put/get data.

:Author: Aliyun
"""

import json
import requests
import six
import time
import logging
import os
import uuid
import sys
from datetime import date, datetime, timedelta

from aliyun.log.gethistogramsresponse import GetHistogramsResponse
from aliyun.log.getlogsresponse import GetLogsResponse
from aliyun.log.index_config_response import *
from aliyun.log.listlogstoresresponse import ListLogstoresResponse
from aliyun.log.logclient_operator import query_more
from aliyun.log.logexception import LogException
from aliyun.log.logstore_config_response import *
from aliyun.log.logtail_config_response import *
from aliyun.log.project_response import *
from aliyun.log.putlogsresponse import PutLogsResponse
from aliyun.log.util import Util, parse_timestamp, is_stats_query
from aliyun.log.version import API_VERSION, USER_AGENT
from aliyun.log.machinegroup_response import *

from aliyun.log.logclient import LogClient as AliyunLogClient

import responses

from kafka.admin import KafkaAdminClient, NewTopic

from kafka.producer.kafka import KafkaProducer
import struct

logger = logging.getLogger(__name__)

try:
	import lz4

	if not hasattr(lz4, 'loads') or not hasattr(lz4, 'dumps'):
		lz4 = None
	else:
		def lz_decompress(raw_size, data):
			return lz4.loads(struct.pack('<I', raw_size) + data)

		def lz_compresss(data):
			return lz4.dumps(data)[4:]

except ImportError:
	lz4 = None


CONNECTION_TIME_OUT = 120
MAX_LIST_PAGING_SIZE = 500
MAX_GET_LOG_PAGING_SIZE = 100

DEFAULT_QUERY_RETRY_COUNT = 1000000000
DEFAULT_QUERY_RETRY_INTERVAL = 0.2


class LogClient(AliyunLogClient):
	""" Construct the LogClient with endpoint, accessKeyId, accessKey.

	:type endpoint: string
	:param endpoint: log service host name, for example, ch-hangzhou.log.aliyuncs.com or https://cn-beijing.log.aliyuncs.com

	:type accessKeyId: string
	:param accessKeyId: aliyun accessKeyId

	:type accessKey: string
	:param accessKey: aliyun accessKey
	"""
	
	__version__ = API_VERSION
	Version = __version__
	
	def __init__(self, kafka_endpoint, elasticsearch_endpoint, accessKeyId, accessKey, securityToken=None, source=None):
		self._isRowIp = Util.is_row_ip(kafka_endpoint)
		self._setendpoint(kafka_endpoint, elasticsearch_endpoint)
		self._accessKeyId = accessKeyId
		self._accessKey = accessKey
		self._timeout = CONNECTION_TIME_OUT
		if source is None:
			self._source = Util.get_host_ip(self._logHost)
		else:
			self._source = source
		self._securityToken = securityToken
		
		self._user_agent = USER_AGENT
		
		self.kafka_endpoint = kafka_endpoint
		self.elasticsearch_endpoint = elasticsearch_endpoint
		try:
			self.kafka_admin_client = KafkaAdminClient(bootstrap_servers=kafka_endpoint, client_id='1')
			self.kafka_producer = KafkaProducer(bootstrap_servers=kafka_endpoint)
		except Exception as e:
			sys.stderr.write('[LOG_LITE_ERROR] init kafka error \n')
			raise e
		
		self.project_logstore = {}

	def _setendpoint(self, kafka_endpoint, elasticsearch_endpoint):
		self.http_type = 'http://'
		self._port = 80
		
		endpoint = kafka_endpoint.strip()
		pos = endpoint.find('://')
		if pos!=-1:
			self.http_type = endpoint[:pos + 3]
			endpoint = endpoint[pos + 3:]
		
		if self.http_type.lower()=='https://':
			self._port = 443
		
		pos = endpoint.find('/')
		if pos!=-1:
			endpoint = endpoint[:pos]
		pos = endpoint.find(':')
		if pos!=-1:
			self._port = int(endpoint[pos + 1:])
			endpoint = endpoint[:pos]
		self._logHost = endpoint
		self._endpoint = endpoint + ':' + str(self._port)
		self._elasticsearch_endpoint = elasticsearch_endpoint
	
	def _pseudo_send(self, method, project, body, resource, params, headers, respons_body_type='json'):
		if body:
			headers['Content-Length'] = str(len(body))
			headers['Content-MD5'] = Util.cal_md5(str(body))
		else:
			headers['Content-Length'] = '0'
			headers["x-log-bodyrawsize"] = '0'
		
		headers['x-log-apiversion'] = API_VERSION
		headers['x-log-signaturemethod'] = 'hmac-sha1'
		if self._isRowIp or not project:
			url = self.http_type + self._endpoint
		else:
			url = self.http_type + project + "." + self._endpoint
		
		if project:
			headers['Host'] = project + "." + self._logHost
		else:
			headers['Host'] = self._logHost
		
		headers['Date'] = self._getGMT()
		
		if self._securityToken:
			headers["x-acs-security-token"] = self._securityToken
		
		signature = Util.get_request_authorization(method, resource,
			self._accessKey, params, headers)
		
		headers['Authorization'] = "LOG " + self._accessKeyId + ':' + signature
		headers['x-log-date'] = headers['Date']  # bypass some proxy doesn't allow "Date" in header issue.
		url = url + resource
		
		return self._sendRequest(method, url, params, body, headers, respons_body_type)
	
	def _sendRequest(self, method, url, params, body, headers, respons_body_type='json'):
		(resp_status, resp_body, resp_header) = self._getHttpResponse(method, url, params, body, headers)
		header = {}
		for key, value in resp_header.items():
			header[key] = value
		
		requestId = Util.h_v_td(header, 'x-log-requestid', '')
		
		if resp_status==200:
			if respons_body_type=='json':
				exJson = self._loadJson(resp_status, resp_header, resp_body, requestId)
				exJson = Util.convert_unicode_to_str(exJson)
				return exJson, header
			else:
				return resp_body, header
		
		exJson = self._loadJson(resp_status, resp_header, resp_body, requestId)
		exJson = Util.convert_unicode_to_str(exJson)
		
		if 'errorCode' in exJson and 'errorMessage' in exJson:
			raise LogException(exJson['errorCode'], exJson['errorMessage'], requestId,
				resp_status, resp_header, resp_body)
		else:
			exJson = '. Return json is ' + str(exJson) if exJson else '.'
			raise LogException('LogRequestError',
				'Request is failed. Http code is ' + str(resp_status) + exJson, requestId,
				resp_status, resp_header, resp_body)
	
	def _getHttpResponse(self, method, url, params, body, headers):  # ensure method, url, body is str
		try:
			headers['User-Agent'] = self._user_agent
			# r = getattr(requests, method.lower())(url, params=params, data=body, headers=headers, timeout=self._timeout)
			# return r.status_code, r.content, r.headers
			if not isinstance(body, dict):
				body = json.loads(body)
			
			r = self._mock_response(responses.GET, url, body, 200)
			return r.status_code, r.content, r.headers
		except Exception as ex:
			raise LogException('LogRequestError', str(ex))
	
	@responses.activate
	def _mock_response(self, method, url, json, status_code):
		responses.add(method, url, json=json, status=status_code)
		resp = requests.get(url)
		return resp
		
	def create_project(self, project_name, project_des):
		""" Create a project
		Unsuccessful opertaion will cause an LogException.
	
		:type project_name: string
		:param project_name: the Project name
	
		:type project_des: string
		:param project_des: the description of a project
	
		:return: CreateProjectResponse
	
		:raise: LogException
		"""
		
		params = {}
		# body = {"projectName": project_name, "description": project_des}
		body = {}
		
		body = six.b(json.dumps(body))
		headers = {'Content-Type': 'application/json', 'x-log-bodyrawsize': str(len(body))}
		resource = "/"
		
		(resp, header) = self._pseudo_send("POST", project_name, body, resource, params, headers)
		# TODO (zhengxi) to make the response the same as SLS
		"""
		Now it returns
		body: {}
		headers: <class 'dict'>: {'Content-Type': 'application/json'}
		
		It should return
		body: ''
		headers:
		<class 'dict'>: {'Server': 'nginx', 'Content-Length': '0', 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Date': 'Thu, 04 Jul 2019 07:55:38 GMT', 'x-log-requestid': '5D1DB0F9F9C69B5210803B4B'}
		"""
		return CreateProjectResponse(header, resp)
	
	
	def get_project(self, project_name):
		""" get project
		Unsuccessful opertaion will cause an LogException.
	
		:type project_name: string
		:param project_name: the Project name
	
		:return: GetProjectResponse
	
		:raise: LogException
		"""
		headers = {}
		params = {}
		resource = "/"
		
		# TODO (zhengxi) need to make the response exactly the same as that of SLS
		body = {'createTime': '2019-07-04 15:55:38', 'description': 'SSH Tunnel Client log storing project',
				'lastModifyTime': '2019-07-04 15:55:38', 'owner': '1546188448726641', 'projectName': project_name,
				'region': 'cn-beijing', 'status': 'Normal'}
		body = six.b(json.dumps(body))
		
		(resp, header) = self._pseudo_send("GET", project_name, body, resource, params, headers)
		
		"""
		Expect response:
		body = {dict} <class 'dict'>: {'createTime': '2019-07-04 15:55:38', 'description': 'SSH Tunnel Client log storing project', 'lastModifyTime': '2019-07-04 15:55:38', 'owner': '1546188448726641', 'projectName': 'sshtunnelproject', 'region': 'cn-beijing', 'status': 'Normal'}
		createTime = {str} '2019-07-04 15:55:38'
		description = {str} 'SSH Tunnel Client log storing project'
		headers = {dict} <class 'dict'>: {'Server': 'nginx', 'Content-Type': 'application/json', 'Content-Length': '230', 'Connection': 'close', 'Access-Control-Allow-Origin': '*', 'Date': 'Thu, 04 Jul 2019 08:20:04 GMT', 'x-log-requestid': '5D1DB6B43C898C5EE07C0F95'}
		lastModifyTime = {str} '2019-07-04 15:55:38'
		owner = {str} '1546188448726641'
		projectName = {str} 'sshtunnelproject'
		region = {str} 'cn-beijing'
		status = {str} 'Normal'
		"""
		return GetProjectResponse(resp, header)
	
	def create_logstore(self, project_name, logstore_name,
						ttl=30,
						shard_count=2,
						enable_tracking=False,
						append_meta=False,
						auto_split=True,
						max_split_shard=64,
						preserve_storage=False
						):
		""" create log store
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name

		:type logstore_name: string
		:param logstore_name: the logstore name

		:type ttl: int
		:param ttl: the life cycle of log in the logstore in days, default 30, up to 3650

		:type shard_count: int
		:param shard_count: the shard count of the logstore to create, default 2

		:type enable_tracking: bool
		:param enable_tracking: enable web tracking, default is False

		:type append_meta: bool
		:param append_meta: allow to append meta info (server received time and IP for external IP to each received log)

		:type auto_split: bool
		:param auto_split: auto split shard, max_split_shard will be 64 by default is True

		:type max_split_shard: int
		:param max_split_shard: max shard to split, up to 64

		:type preserve_storage: bool
		:param preserve_storage: if always persist data, TTL will be ignored.

		:return: CreateLogStoreResponse

		:raise: LogException
		"""
		
		dic = {}
		dic['shard_count'] = shard_count
		dic['enable_tracking'] = enable_tracking
		dic['ttl'] = ttl
		dic['append_meta'] = append_meta
		dic['auto_split'] = auto_split
		dic['max_split_shard'] = max_split_shard
		dic['preserve_storage'] = preserve_storage
		if auto_split and (max_split_shard <= 0 or max_split_shard >= 64):
			max_split_shard = 64
		if preserve_storage:
			ttl = 3650
		
		params = {}
		resource = "/logstores"
		headers = {"x-log-bodyrawsize": '0', "Content-Type": "application/json"}
		body = {
			"logstoreName": logstore_name, "ttl": int(ttl), "shardCount": int(shard_count),
			"enable_tracking": enable_tracking,
			"autoSplit": auto_split,
			"maxSplitShard": max_split_shard,
			"appendMeta": append_meta
		}
		
		body_str = six.b(json.dumps(body))
		
		topic_list = list()
		topic_name = '__'.join((project_name, logstore_name))
		topic_list.append(NewTopic(name=topic_name, num_partitions=shard_count, replication_factor=2))
		
		try:
			self.kafka_admin_client.create_topics(new_topics=topic_list)
		except Exception as e:
			if str(e).find('TopicAlreadyExistsError') == -1:
				sys.stderr.write('[LOG_LITE_ERROR] create kafka topic error \n')
				raise e
		
		ttl_message = {}
		ttl_message['index_name'] = topic_name
		ttl_message['ttl'] = ttl

		curl_add_ttl = 'curl -H "Content-Type: application/json" -XPOST http://'+self.elasticsearch_endpoint+'/es-index-ttl-config/_doc/'+topic_name+' -d \''+str(ttl_message).replace('\'','"')+'\''
		add_ttl_result = eval(os.popen(curl_add_ttl).read())
		if 'result' in add_ttl_result:
			if add_ttl_result['result'] == 'updated' or add_ttl_result['result'] == 'created':
				pass
			else:
				raise Exception('add ttl fail: '+str(add_ttl_result))
		else:
			raise Exception('add ttl fail: '+str(add_ttl_result))


		try:
			(resp, header) = self._send("POST", project_name, body_str, resource, params, headers)
		except LogException as ex:
			if ex.get_error_code()=="LogStoreInfoInvalid" and ex.get_error_message()=="redundant key exist in json":
				logger.warning("LogStoreInfoInvalid, will retry with basic parameters. detail: {0}".format(ex))
				body = {"logstoreName": logstore_name, "ttl": int(ttl), "shardCount": int(shard_count),
						"enable_tracking": enable_tracking}
				
				body_str = six.b(json.dumps(body))
				
				(resp, header) = self._pseudo_send("POST", project_name, body_str, resource, params, headers)
			else:
				raise
		
		if (project_name + logstore_name) in self.project_logstore.keys():
			raise LogException('LogStoreAlreadyExist', '')
		self.project_logstore[project_name + logstore_name] = dic
		return CreateLogStoreResponse(header, resp)
	
	def get_logstore(self, project_name, logstore_name):
		""" get the logstore meta info
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name

		:type logstore_name: string
		:param logstore_name: the logstore name

		:return: GetLogStoreResponse

		:raise: LogException
		"""
		if not (project_name + logstore_name) in self.project_logstore.keys():
			raise LogException('LogStoreNotExist', '')
		
		headers = {}
		params = {}
		resource = "/logstores/" + logstore_name
		(resp, header) = self._pseudo_send("GET", project_name, {}, resource, params, headers)
		resp['logstoreName'] = logstore_name
		try:
			resp['ttl'] = self.project_logstore[project_name + logstore_name]['ttl']
		except Exception:
			resp['ttl'] = 30
		try:
			resp["shardCount"] = self.project_logstore[project_name + logstore_name]['shard_count']
		except Exception:
			resp["shardCount"] = 2
		try:
			resp["enable_tracking"] = self.project_logstore[project_name + logstore_name]['enable_tracking']
		except Exception:
			resp["enable_tracking"] = False
		try:
			resp["appendMeta"] = self.project_logstore[project_name + logstore_name]['append_meta']
		except Exception:
			resp["appendMeta"] = False
		try:
			resp["autoSplit"] = self.project_logstore[project_name + logstore_name]['auto_split']
		except Exception:
			resp["autoSplit"] = True
		try:
			resp["maxSplitShard"] = self.project_logstore[project_name + logstore_name]['max_split_shard']
		except Exception:
			resp["maxSplitShard"] = 64
		return GetLogStoreResponse(resp, header)
	
	
	def put_logs(self, request):
		""" Put logs to log service. up to 512000 logs up to 10MB size
				Unsuccessful opertaion will cause an LogException.

				:type request: PutLogsRequest
				:param request: the PutLogs request parameters class

				:return: PutLogsResponse

				:raise: LogException
				"""
		
		logstore = request.get_logstore()
		project = request.get_project()
		
		params = {}
		if request.get_hash_key() is not None:
			resource = '/logstores/' + logstore + "/shards/route"
			params["key"] = request.get_hash_key()
		else:
			resource = '/logstores/' + logstore + "/shards/lb"
		

		topic_name = '__'.join((project, logstore))
		send_messgae = []
		for item in request.get_log_items():
			dic = {}
			dic['__source__'] = self._source
			dic['__topic__'] = topic_name
			dic['__time__'] = item.get_time() if item.get_time() else int(time.time())
			for key, value in item.contents:
				dic[key] = value
			send_messgae.append(dic)
		
		headers = {'x-log-bodyrawsize': str(len(send_messgae)), 'Content-Type': 'application/x-protobuf'}
		
		if request.get_compress():
			if lz4:
				headers['x-log-compresstype'] = 'lz4'
			else:
				headers['x-log-compresstype'] = 'deflate'
		
		# send message to kafka
		
		try:
			for message in send_messgae:
				self.kafka_producer.send(topic_name, str(message).replace('\'','"').encode())
				self.kafka_producer.flush()
		except Exception as e:
			sys.stderr.write('[LOG_LITE_ERROR] send message to kafka error \n')
			raise e
		
		(resp, header) = self._pseudo_send('POST', project, {}, resource, params, headers)
		
		return PutLogsResponse(header, resp)
	
	
	def get_index_config(self, project_name, logstore_name):
		""" get index config detail of a logstore
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name

		:type logstore_name: string
		:param logstore_name: the logstore name

		:return: GetIndexResponse

		:raise: LogException
		"""
		
		headers = {}
		params = {}
		resource = "/logstores/" + logstore_name + "/index"
		(resp, header) = self._pseudo_send("GET", project_name, {}, resource, params, headers)
		resp['line'] = {"token": [a for a in ''',;  =(){}<>?/# \n\t"'[]:'''], "caseSensitive": False,
						"exclude_keys": "update_rows;latency;return_rows;update_rows;check_rows;isbind;origin_time".split(
							';')}
		return GetIndexResponse(resp, header)
	
	
	def create_index(self, project_name, logstore_name, index_detail):
		""" create index for a logstore
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name

		:type logstore_name: string
		:param logstore_name: the logstore name

		:type index_detail: IndexConfig
		:param index_detail: the index config detail used to create index

		:return: CreateIndexResponse

		:raise: LogException
		"""
		headers = {}
		params = {}
		resource = "/logstores/" + logstore_name + "/index"
		headers['Content-Type'] = 'application/json'
		if not isinstance(index_detail, dict):
			body = six.b(json.dumps(index_detail.to_json()))
		else:
			body = six.b(json.dumps(index_detail))
		headers['x-log-bodyrawsize'] = str(len(body))
		
		(resp, header) = self._pseudo_send("POST", project_name, {}, resource, params, headers)
		return CreateIndexResponse(header, resp)
	
	
	def update_index(self, project_name, logstore_name, index_detail):
		""" update index for a logstore
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name

		:type logstore_name: string
		:param logstore_name: the logstore name

		:type index_detail: IndexConfig
		:param index_detail: the index config detail used to update index

		:return: UpdateIndexResponse

		:raise: LogException
		"""
		
		headers = {}
		params = {}
		resource = "/logstores/" + logstore_name + "/index"
		headers['Content-Type'] = 'application/json'
		if not isinstance(index_detail, dict):
			body = six.b(json.dumps(index_detail.to_json()))
		else:
			body = six.b(json.dumps(index_detail))
		headers['x-log-bodyrawsize'] = str(len(body))
		
		(resp, header) = self._pseudo_send("PUT", project_name, {}, resource, params, headers)
		return UpdateIndexResponse(header, resp)
	
	
	def create_logtail_config(self, project_name, config_detail):
		""" create logtail config in a project
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name

		:type config_detail: LogtailConfigGenerator or SeperatorFileConfigDetail or SimpleFileConfigDetail or FullRegFileConfigDetail or JsonFileConfigDetail or ApsaraFileConfigDetail or SyslogConfigDetail or CommonRegLogConfigDetail
		:param config_detail: the logtail config detail info, use `LogtailConfigGenerator.from_json` to generate config: SeperatorFileConfigDetail or SimpleFileConfigDetail or FullRegFileConfigDetail or JsonFileConfigDetail or ApsaraFileConfigDetail or SyslogConfigDetail, Note: CommonRegLogConfigDetail is deprecated.

		:return: CreateLogtailConfigResponse

		:raise: LogException
		"""
		
		if config_detail.logstore_name:
			# try to verify if the logstore exists or not.
			self.get_logstore(project_name, config_detail.logstore_name)
		
		headers = {}
		params = {}
		resource = "/configs"
		headers['Content-Type'] = 'application/json'
		body = six.b(json.dumps(config_detail.to_json()))
		headers['x-log-bodyrawsize'] = str(len(body))
		(resp, headers) = self._pseudo_send("POST", project_name, {}, resource, params, headers)
		return CreateLogtailConfigResponse(headers, resp)
	
	
	def get_histograms(self, request):
		""" Get histograms of requested query from log service.
		Unsuccessful opertaion will cause an LogException.

		:type request: GetHistogramsRequest
		:param request: the GetHistograms request parameters class.

		:return: GetHistogramsResponse

		:raise: LogException
		"""
		headers = {}
		params = {}
		if request.get_topic() is not None:
			params['topic'] = request.get_topic()
		if request.get_from() is not None:
			params['from'] = request.get_from()
		if request.get_to() is not None:
			params['to'] = request.get_to()
		if request.get_query() is not None:
			params['query'] = request.get_query()

		topic = request.get_topic()
		query = request.get_query()

		from_time = request.get_from()

		to_time = request.get_to()


		params['type'] = 'histogram'
		logstore = request.get_logstore().lower()
		project = request.get_project().lower()
		resource = "/logstores/" + logstore
		(resp, header) = self._pseudo_send("GET", project, {}, resource, params, headers)

		header["X-Log-Progress"] = 'Complete'


		from_time_str = str(from_time) + "000"
		to_time_str = str(to_time) + "000"

		if None == topic or ""==topic:
			topic = " "
		else:
			topic = "__topic__:" + topic + " AND "
		if None == query or ""==query:
			query = " "
		else:
			query += " AND "

		query += topic
		request_date = "'{\"query\":{\"query_string\":{\"query\": \"" + query + " @timestamp:[" + from_time_str + " TO " + to_time_str + "] \"}}}'"

		url = 'http://' + self.elasticsearch_endpoint + '/' + project + '__' + logstore + '*/_count '
		sys.stderr.write(' DEBUG >>> curl -s -H \"Content-Type: application/json\" ' + url + ' -d  ' + request_date + '\n')
		log_result = os.popen('curl -s -H \"Content-Type: application/json\" ' + url + ' -d  ' + request_date).read()


		resp = []
		resp_one = {}

		resp_one['from'] = from_time
		resp_one['to'] = to_time
		resp_one['progress'] = 'Complete'
		try:
			log_result_json = json.loads(log_result)
			resp_one['count'] = log_result_json['count']
		except Exception:
			resp_one['count'] = 0

		resp.append(resp_one)
		return GetHistogramsResponse(resp, header)
	
	
	def get_log(self, project, logstore, from_time, to_time, topic=None,
				query=None, reverse=False, offset=0, size=100):
		""" Get logs from log service. will retry when incomplete.
		Unsuccessful opertaion will cause an LogException.
		Note: for larger volume of data (e.g. > 1 million logs), use get_log_all

		:type project: string
		:param project: project name

		:type logstore: string
		:param logstore: logstore name

		:type from_time: int/string
		:param from_time: the begin timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

		:type to_time: int/string
		:param to_time: the end timestamp or format of time in readable time like "%Y-%m-%d %H:%M:%S<time_zone>" e.g. "2018-01-02 12:12:10+8:00", also support human readable string, e.g. "1 hour ago", "now", "yesterday 0:0:0", refer to https://aliyun-log-cli.readthedocs.io/en/latest/tutorials/tutorial_human_readable_datetime.html

		:type topic: string
		:param topic: topic name of logs, could be None

		:type query: string
		:param query: user defined query, could be None

		:type reverse: bool
		:param reverse: if reverse is set to true, the query will return the latest logs first, default is false

		:type offset: int
		:param offset: line offset of return logs

		:type size: int
		:param size: max line number of return logs, -1 means get all

		:return: GetLogsResponse

		:raise: LogException
		"""
		
		# need to use extended method to get more when: it's not select query, and size > default page size
		if not is_stats_query(query) and (int(size)==-1 or int(size) > MAX_GET_LOG_PAGING_SIZE):
			return query_more(self.get_log, int(offset), int(size), MAX_GET_LOG_PAGING_SIZE,
							  project, logstore, from_time, to_time, topic,
							  query, reverse)

		ret = None
		for _c in range(DEFAULT_QUERY_RETRY_COUNT):
			headers = {}
			params = {'from': parse_timestamp(from_time),
					  'to': parse_timestamp(to_time),
					  'type': 'log',
					  'line': size,
					  'offset': offset,
					  'reverse': 'true' if reverse else 'false'}
			
			if topic:
				params['topic'] = topic
			if query:
				params['query'] = query
			
			resource = "/logstores/" + logstore
			(resp, header) = self._pseudo_send("GET", project, {}, resource, params, headers)
			header['X-Log-Progress'] = ''
			ret = GetLogsResponse(resp, header)
			if ret.is_completed():
				break
			
			time.sleep(DEFAULT_QUERY_RETRY_INTERVAL)
		
		return ret
	
	
	def get_logs(self, request):
		""" Get logs from log service.
		Unsuccessful opertaion will cause an LogException.
		Note: for larger volume of data (e.g. > 1 million logs), use get_log_all

		:type request: GetLogsRequest
		:param request: the GetLogs request parameters class.

		:return: GetLogsResponse

		:raise: LogException
		"""
		project = request.get_project().lower()
		logstore = request.get_logstore().lower()
		from_time = request.get_from()
		to_time = request.get_to()
		topic = request.get_topic()
		query = request.get_query()
		reverse = request.get_reverse()
		offset = request.get_offset()
		size = request.get_line()

		header = {}
		header['X-Log-Progress'] = 'Complete'

		resp = []
		resp_one = {}

		from_time_str = str(from_time) + "000"
		to_time_str = str(to_time) + "000"
		if reverse:
			order_str = "desc"
		else:
			order_str = "asc"

		if offset >= 10000:
			return GetLogsResponse(resp, header)
		elif offset <= 0:
			offset = 0

		if size <= 0:
			size = 200

		if None == topic or ""==topic:
			topic = " "
		else:
			topic = "__topic__:" + topic + " AND "
		if None == query or ""==query:
			query = " "
		else:
			query += " AND "

		query += topic
		request_date = "'{\"query\":{\"query_string\":{\"query\": \"" + query + " @timestamp:[" + from_time_str + " TO " + to_time_str + "] \"}},\"sort\": {\"@timestamp\": {\"order\": \"" + order_str + "\" }},\"from\": " + str(offset) + ",\"size\": " + str(size) + "}'"
		url = 'http://' + self.elasticsearch_endpoint + '/' + project + '__' + logstore + '-*' + '/_search '
		sys.stderr.write(' DEBUG >>> curl -s -H \"Content-Type: application/json\" -X GET ' + url + ' -d  ' + request_date + '\n')
		log_result = os.popen('curl -s -H \"Content-Type: application/json\" -X GET ' + url + ' -d  ' + request_date).read()

		try:
			log_result_json = json.loads(log_result)
			for messagelist in log_result_json['hits']['hits']:
				messagelist = messagelist['_source']
				resp_one = {}
				resp_one['__source__'] = self._source
				message_time = messagelist['@timestamp'][0:messagelist['@timestamp'].find('.')]
				timeArray = time.strptime(message_time, "%Y-%m-%dT%H:%M:%S")
				timeStamp = int(time.mktime(timeArray))
				resp_one['__time__'] = timeStamp

				for count_str in messagelist:
					no_message = ['@timestamp','@version','@topic','__topic__','__source__','__time__']
					if count_str == '__source__':
						resp_one['__source__'] = messagelist['__source__']
					elif count_str == '__time__':
						resp_one['__time__'] = messagelist['__time__']
					elif not count_str in no_message:
						resp_one[count_str] = messagelist[count_str]
				resp.append(resp_one)

		except Exception:
			resp_one['__source__'] = self._source
			times = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			timeArray = time.strptime(times, "%Y-%m-%d %H:%M:%S")
			timeStamp = int(time.mktime(timeArray))
			resp_one['__time__'] = timeStamp
			resp.append(resp_one)

		return GetLogsResponse(resp, header)
		
		
		
    # yingyang code
    
    
	def create_machine_group(self, project_name, group_detail):
		""" create machine group in a project
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name

		:type group_detail: MachineGroupDetail
		:param group_detail: the machine group detail config

		:return: CreateMachineGroupResponse

		:raise: LogException
		"""

		headers = {}
		params = {}
		resource = "/machinegroups"
		headers['Content-Type'] = 'application/json'
		body = six.b(json.dumps(group_detail.to_json()))
		headers['x-log-bodyrawsize'] = str(len(body))
		(resp, headers) = self._pseudo_send("POST", project_name, body, resource, params, headers)
		return CreateMachineGroupResponse(headers, resp)

	def apply_config_to_machine_group(self, project_name, config_name, group_name):
		""" apply a logtail config to a machine group
		Unsuccessful opertaion will cause an LogException.
	
		:type project_name: string
		:param project_name: the Project name
	
		:type config_name: string
		:param config_name: the logtail config name to apply
	
		:type group_name: string
		:param group_name: the machine group name
	
		:return: ApplyConfigToMachineGroupResponse
	
		:raise: LogException
		"""
		headers = {}
		params = {}
		resource = "/machinegroups/" + group_name + "/configs/" + config_name
		(resp, header) = self._pseudo_send("PUT", project_name, {}, resource, params, headers)
		return ApplyConfigToMachineGroupResponse(header, resp)

	def delete_project(self, project_name):
		""" delete project
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name 

		:return: DeleteProjectResponse 

		:raise: LogException
		"""
		headers = {}
		params = {}
		resource = "/"

		(resp, header) = self._pseudo_send("DELETE", project_name, {}, resource, params, headers)
		return DeleteProjectResponse(header, resp)


	def get_logtail_config(self, project_name, config_name):
		""" get logtail config in a project
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name 

		:type config_name: string
		:param config_name: the logtail config name

		:return: GetLogtailConfigResponse

		:raise: LogException
		"""
		headers = {}
		params = {}
		resource = "/configs/" + config_name
		(resp, headers) = self._pseudo_send("GET", project_name, {}, resource, params, headers)
		resp['inputDetail'] = {'logType':'','filePattern':'','logPath':'','separator':'','key':'','plugin':'','logBeginRegex':'','tag':''}
		resp['inputType'] = 'plugin'
		resp['configName'] = ''
		resp['outputDetail'] = {'logstoreName':''}

		return GetLogtailConfigResponse(resp, headers)


	def list_logstores(self, request):
		""" List all logstores of requested project.
		Unsuccessful opertaion will cause an LogException.

		:type request: ListLogstoresRequest
		:param request: the ListLogstores request parameters class.

		:return: ListLogStoresResponse

		:raise: LogException
		"""
		headers = {}
		params = {}
		resource = '/logstores'
		project = request.get_project().lower()
		(resp, header) = self._pseudo_send("GET", project, {}, resource, params, headers)

		all_index = os.popen('curl http://'+self.elasticsearch_endpoint+'/_cat/indices').read()
		
		resp['count'] = 0
		resp['total'] = 0
		logstores = []
		all_index_list = all_index.split('\n')
		for index in all_index_list:
			index_list = index.split(' ')
			if len(index_list) > 2:
				index = index_list[2].strip()
				if index.find(project) != 1:
					try:
						if len(index.split('__')) == 2:
							resp['count'] = resp['count'] +1
							resp['total'] = resp['total'] +1
							logstore = index.split('__')[1]
							logstores.append(logstore[0:len(logstore)-11])
					except exception:
						pass
					
		resp['logstores'] = logstores
		return ListLogstoresResponse(resp, header)


	def get_machine_group(self, project_name, group_name):
		""" get machine group in a project
		Unsuccessful opertaion will cause an LogException.

		:type project_name: string
		:param project_name: the Project name 

		:type group_name: string
		:param group_name: the group name to get

		:return: GetMachineGroupResponse

		:raise: LogException
		"""

		headers = {}
		params = {}
		resource = "/machinegroups/" + group_name
		(resp, headers) = self._pseudo_send("GET", project_name, {}, resource, params, headers)
		return GetMachineGroupResponse(resp, headers)