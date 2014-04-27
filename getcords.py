import urllib
import sys
import json
import urllib2
import time

from pymongo import MongoClient

from math import sqrt

url = "http://dadosabertos.rio.rj.gov.br/apiTransporte/apresentacao/rest/index.cfm/obterPosicoesDaLinha/474"
h = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
p_cords = [[-22.943733,-43.180713],[-22.948231,-43.181204]]
ref_coords = {"x_coord":-22.8925535, "y_coord":-43.2577202}
c_dist = 0.008983

def readResponse(jsn):
	if jsn:
		result = json.loads(jsn)
		result = [ r[:-1] for r in result['DATA']]
		return result

def checkDirection(col, data):
	
	for d in data:
		try:
			r = col.find({"numero": d[1]})[0]
				
			first_d = sqrt((r["x_coord"] - ref_coords["x_coord"]) ** 2 + (r["y_coord"] - ref_coords["y_coord"]) ** 2)
			curr_d = sqrt((d[3] - ref_coords["x_coord"]) ** 2 + (d[4] - ref_coords["y_coord"]) ** 2)
			
			if first_d > curr_d:
				print "update direction of  numero "+d[1]+" 1"
				col.update({"numero":d[1]},{"$set":{"direction": 1}})
			elif first_d < curr_d:	
				print "update direction of  numero "+d[1]+" 2"

				col.update({"numero":d[1]},{"$set":{"direction": 2}})
			else:
				col.update({"numero":d[1]},{"$set":{"direction": 0}})
		except IndexError:
			continue 

	return data

def checkCoords(data):
	result = []
	for item in data:
		#print item
		dist = sqrt((item["x_coord"] - p_cords[0][0]) ** 2 + (item["y_coord"] - p_cords[0][1]) ** 2)
	#	print dist
		if dist < c_dist and item["direction"] == 2:
			result.append(item)
	return result

def insertResults(col, data):
	if not  col and data:
		print "insertResults: Error no col or data"
		return

	for d in data:
		try:
			res = col.find({"numero": d[1]})[0]
			col.update({"numero": d[1]}, {"$set":{"x_coord":d[3], "y_coord":d[4]}})
		except IndexError:
			print "new bus inserted ",d[1]	
			col.insert({"data":d[0],
			    "numero":d[1],
			    "linha":d[2],
		            "x_coord":d[3],
			    "y_coord":d[4],
			    "direction":0})

def insertHit(col, data):
	print "Insert Hit number: ",len(data)
	for d in data:
		print d
		col.hit.insert({"data":d["data"],
			    "numero":d["numero"],
			    "linha":d["linha"],
		            "x_coord":d["x_coord"],
			    "y_coord":d["y_coord"]})


def startConn():
	client = MongoClient()
	db = client.oniTimes
	return db.data


def importThread():
	while 1:
		request = urllib2.Request(url, headers=h)
	#request.add_header(headers.items()[0][0], headers.items()[0][1])
		response = urllib2.build_opener().open(request)	
	
		jsn = response.read()
	
		data = readResponse(jsn)
	

		#data = 	checkCords(data)
	
		col = startConn()

		data = checkDirection(col, data)

		insertResults(col, data)
	
		result	= checkCoords(col.find())
	
		insertHit(col, result)

		time.sleep(10)		

def main(argv=None):
	importThread() 

if __name__ == "__main__":
	sys.exit(main())
