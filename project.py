#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue April 23 12:14:21 2019

@author: handabaldeep
"""

from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))


def extract_email_network(rdd):
	"""
	input: takes an RDD rdd as argument 
	return: an RDD of triples (S, R, T) each of which representing an Email transmission
	from the sender S to the recipient R occurring at time T
	"""
    rdd_mail = rdd.map(lambda x: Parser().parsestr(x))
    rdd_full_email_tuples = rdd_mail.map(lambda x: (x.get('From'),re.findall(r'\S+@[^,]+',str(x.get('To'))) + \
                                                                re.findall(r'\S+@[^,]+',str(x.get('Cc'))) + \
                                                                re.findall(r'\S+@[^,]+',str(x.get('Bcc'))),date_to_dt(x.get('Date'))))
    rdd_email_triples = rdd_full_email_tuples.flatMap(lambda x: [(x[0],x[1][i],x[2]) for i in range(0,len(x[1]))])
	
	# check for email validity
    email_regex = '[a-zA-z0-9\.!#$%&\'*+-/=?^_`{|}~]+@([a-zA-z0-9]+\.)+[a-zA-z]+'
    valid_email = lambda s: True if re.compile(email_regex).fullmatch(s)!=None else False
    
	# self-loops must be excluded
	not_self_loop = lambda t: True if t[0]!=t[1] else False

	# email address must belong to the 'enron.com' domain
    ends_with_regex = '[@.]enron.com$'
    end_email = lambda s: True if re.search(ends_with_regex,s)!=None else False

    rdd_email_triples_enron = rdd_email_triples.filter(lambda x: not_self_loop(x)).filter(lambda x: valid_email(x[0])) \
                                            .filter(lambda x: valid_email(x[1])).filter(lambda x: end_email(x[0])) \
                                            .filter(lambda x: end_email(x[1]))
    
	# all triples must be distinct
	distinct_triples = rdd_email_triples_enron.distinct()
    return distinct_triples


def convert_to_weighted_network(rdd, drange=None):
	"""
	A weighted network is the network in which each edge is associated 
	with a positive integer, called the edge’s weight
	params: 
	rdd- takes one required argument rdd, which is an RDD that complies 
	with the output format of the function extract_email_network()
	drange-	a pair (d1, d2) of datetime objects with a default value of None
	return: RDD consisting of distinct triples (o, d, w)
	"""
    if drange!=None :
		# check if the record is in the date range
        in_range = lambda x: True if x>drange[0] and x<=drange[1] else False
        range_rdd = rdd.filter(lambda x: in_range(x[2]))
		# calculate weights
        weights_rdd = range_rdd.map(lambda x: ((x[0],x[1]),1)) \
                        .reduceByKey(lambda x,y: x+y) \
                        .map(lambda x: (x[0][0],x[0][1],x[1]))
    else :
        weights_rdd = rdd.map(lambda x: ((x[0],x[1]),1)) \
                            .reduceByKey(lambda x,y: x+y) \
                            .map(lambda x: (x[0][0],x[0][1],x[1]))
							
    return weights_rdd


def get_out_degrees(rdd):
	"""
	The weighted out-degree of a node n in a weighted network is the
	sum of the weights of all edges leaving n in the network
	params: RDD representing a weighted network
	return: an RDD of pairs (d, n)
	"""
    weights_rdd1 = rdd.map(lambda x: (x[0],x[2])) \
                                .reduceByKey(lambda x,y: x+y)
    weights_rdd2 = rdd.map(lambda x: (x[1],0)) \
                                .reduceByKey(lambda x,y: x+y)
    weights_rdd = weights_rdd1.union(weights_rdd2)
	
    out_rdd = weights_rdd.reduceByKey(lambda x,y: x+y) \
                                .map(lambda x: (x[1],x[0])) \
                                .sortBy(lambda x: (x[0],x[1]),ascending = False)
								
    return out_rdd


def get_in_degrees(rdd):
	"""
	The weighted in-degree of a node n in a weighted network is the
	sum of the weights of all edges entering n in the network
	params: RDD representing a weighted network
	return: an RDD of pairs (d, n)
	"""
    weights_rdd1 = rdd.map(lambda x: (x[0],0)) \
                        .reduceByKey(lambda x,y: x+y)
    weights_rdd2 = rdd.map(lambda x: (x[1],x[2])) \
                        .reduceByKey(lambda x,y: x+y)
    weights_rdd = weights_rdd1.union(weights_rdd2)
	
    in_rdd = weights_rdd.reduceByKey(lambda x,y: x+y) \
                        .map(lambda x: (x[1],x[0])) \
                        .sortBy(lambda x: (x[0],x[1]),ascending = False)
						
    return in_rdd

    
def get_out_degree_dist(rdd):
	"""
	weighted out-degree distributions for a weighted network
	params: RDD representing a weighted network
	return: RDD of pairs mapping each weighted out-degree of a node
	in the network to the number of nodes having this out-degree
	"""
    out_rdd = get_out_degrees(rdd)
    out_deg_dist = out_rdd.map(lambda x: (x[0],1)) \
                                    .reduceByKey(lambda x,y: x+y) \
                                    .sortBy(lambda x: x[0])
    
    return out_deg_dist


def get_in_degree_dist(rdd):
	"""
	weighted in-degree distributions for a weighted network
	params: RDD representing a weighted network
	return: RDD of pairs mapping each weighted in-degree of a node
	in the network to the number of nodes having this in-degree
	"""
    in_rdd = get_in_degrees(rdd)
    in_deg_dist = in_rdd.map(lambda x: (x[0],1)) \
                        .reduceByKey(lambda x,y: x+y) \
                        .sortBy(lambda x: x[0])
    
    return in_deg_dist
