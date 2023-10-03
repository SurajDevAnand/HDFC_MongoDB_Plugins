#! /usr/bin/python3

import collections
import datetime as dt
import traceback
import time
import json
import urllib.parse

PLUGIN_VERSION = "1"
HEARTBEAT="true"

METRICS_UNITS = {}


class MongoDB(object):
    def __init__(self, args):
        self.args=args
        self.host=args.host
        self.port=args.port
        self.username=args.username
        self.password=args.password
        self.dbname=args.dbname
        self.authdb=args.authdb

        self.tls=args.tls
        
        if self.tls=="True":
        
            self.tls=True
            self.tlscertificatekeyfile=args.tlscertificatekeyfile
            self.tlscertificatekeyfilepassword=args.tlscertificatekeyfilepassword
            self.tlsallowinvalidcertificates=args.tlsallowinvalidcertificates
            self.tlsallowinvalidcertificates=args.tlsallowinvalidcertificates
            if self.tlsallowinvalidcertificates=="True":
                self.tlsallowinvalidcertificates=True
            else:
                self.tlsallowinvalidcertificates=False
                
        else:
            self.tls=False


        if(self.username!="None" and self.password!="None" and self.authdb!="None"):
            self.mongod_server = "{0}:{1}@{2}:{3}/{4}".format(self.username,urllib.parse.quote(self.password), self.host, self.port, self.authdb)
        elif(self.username!="None" and self.password!="None"):
            self.mongod_server = "{0}:{1}@{2}:{3}".format(self.username, self.password, self.host, self.port)
        elif(self.authdb!="None"):
            self.mongod_server = "{0}:{1}/{2}".format(self.host, self.port, self.authdb)
        else:
            self.mongod_server = "{0}:{1}".format(self.host, self.port)

    

    def metricCollector(self):
        data = {}
        data['plugin_version'] = PLUGIN_VERSION
        data['heartbeat_required']=HEARTBEAT

        try:
            import pymongo
            from pymongo import MongoClient
        except ImportError:
            data['status']=0
            data['msg']='pymongo module not installed\n Solution : Use the following command to install pymongo\n pip install pymongo \n(or)\n pip3 install pymongo'
            return data
        
        try:

            try:
                mongo_uri = 'mongodb://' + self.mongod_server
                if self.tls:
                    self.connection = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000,tls=self.tls,tlscertificatekeyfile=self.tlscertificatekeyfile,tlscertificatekeyfilepassword=self.tlscertificatekeyfilepassword,tlsallowinvalidcertificates=self.tlsallowinvalidcertificates)
                else:
                    self.connection = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)

                db = self.connection[self.dbname]
                replication_data = db.command({'replSetGetStatus'  :1})
                
                members=replication_data['members']
                primary_optime=None
                secondary=1

                for member in members:
                    if member['stateStr'] == 'PRIMARY' : 
                        primary_optime =member['optimeDate']
                        if not primary_optime:
                            continue
                            
                    if member['stateStr'] == 'SECONDARY' :
                        secondary_optime = member['optimeDate']
                        metric_name='Repl_lag_'+member['name']
                        if secondary_optime:
                            data[metric_name]=(primary_optime - secondary_optime).total_seconds()
                            secondary+=1
                        else:
                            continue
                            
                        METRICS_UNITS['Repl_lag_'+member['name']]="sec"
            
                self.connection.close()

            except pymongo.errors.ServerSelectionTimeoutError as e:
                data['status']=0
                data['msg']='No mongoDB server is available to connect.\n '+str(e)
                return data
            except pymongo.errors.ConnectionFailure as e:
                data['status']=0
                data['msg']='Connection to database failed. '+str(e)
                return data
            except pymongo.errors.ExecutionTimeout as e:
                data['status']=0
                data['msg']='Execution of database command failed'+str(e)
                return data


        except Exception:
            data['msg']=traceback.format_exc()
        data['units']=METRICS_UNITS
        return data

if __name__ == "__main__":


    host ="127.0.0.1"
    port ="27081"
    username ="None"
    password ="None"
    dbname ="admin"
    authdb="admin"

    # TLS/SSL Details
    tls="False"
    tlscertificatekeyfile=None
    tlscertificatekeyfilepassword=None
    tlsallowinvalidcertificates="True"

    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--host',help="Host Name",nargs='?', default= host)
    parser.add_argument('--port',help="Port",nargs='?', default= port)
    parser.add_argument('--username',help="username", default= username)
    parser.add_argument('--password',help="Password", default= password)
    parser.add_argument('--dbname' ,help="dbname",nargs='?', type=str,default= dbname)
    parser.add_argument('--authdb' ,help="authdb",nargs='?',type=str, default= authdb)

    parser.add_argument('--tls' ,help="tls setup (True or False)",nargs='?',default= tls)
    parser.add_argument('--tlscertificatekeyfile' ,help="tlscertificatekeyfile file path",default= tlscertificatekeyfile)
    parser.add_argument('--tlscertificatekeyfilepassword' ,help="tlscertificatekeyfilepassword",default= tlscertificatekeyfilepassword)
    parser.add_argument('--tlsallowinvalidcertificates' ,help="tlsallowinvalidcertificates",default= tlsallowinvalidcertificates)
    
    args=parser.parse_args()
    mongo_check = MongoDB(args)
    
    result = mongo_check.metricCollector()
    
    print(json.dumps(result, indent=4, sort_keys=True))
