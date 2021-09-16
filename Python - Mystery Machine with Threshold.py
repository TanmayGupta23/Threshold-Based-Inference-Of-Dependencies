# Mystery Machine With Threshold - https://research.fb.com/wp-content/uploads/2016/11/the-mystery-machine-end-to-end-performance-analysis-of-large-scale-internet-services.pdf 
# Anshul Rastogi
# 9/15/21

import json as js
import numpy as np
from numpy import array
from itertools import combinations

def extract_JSON(js_file):
  with open(js_file) as file:return js.loads(file.read())

def preprocessing(data,extract=False):
  if extract:js_data=list();[js_data.extend(extract_JSON(js_file)["data"]) for js_file in data];data=js_data
  nameIDs,spanIDs=list(),dict()
  [[load_spanN(span,nameIDs,spanIDs,trace["processes"]) for span in trace["spans"]] for trace in data]
  return nameIDs,spanIDs

def load_spanN(span,nameIDs,spanIDs,processes):
  curName=span["operationName"]+"_"+processes[span["processID"]]["serviceName"]
  if curName in nameIDs:
    spanIDs[span["spanID"]]=nameIDs.index(curName)
  else:
    spanIDs[span["spanID"]]=len(nameIDs)
    nameIDs.append(curName)

def alg(data,nameIDs,spanIDs,threshold=1):
  dim=len(nameIDs)*2
  S=np.zeros((dim,dim),dtype="i")
  Q=np.zeros(len(nameIDs)*(dim-1),dtype="i")
  combs=[]
  for trace in data:
    comb=combinations(trace["spans"],2)
    combs.append(list(comb))
    for spanA,spanB in combinations(trace["spans"],2):
      A_Index=spanIDs.get(spanA.get("spanID"))
      t_Astart=spanA.get("startTime")
      t_Aend=t_Astart+spanA.get("duration")
      B_Index=spanIDs.get(spanB.get("spanID"))
      t_Bstart=spanB.get("startTime")
      t_Bend=t_Bstart+spanB.get("duration")
      if t_Aend < t_Bstart:
        S[2*A_Index,2*B_Index]+=1
        S[2*A_Index+1,2*B_Index+1]+=1
        S[2*A_Index,2*B_Index+1]+=1
        S[2*A_Index+1,2*B_Index]+=1
      elif t_Bstart<t_Aend:
        S[2*B_Index,2*A_Index+1]+=1
        if t_Bend<t_Astart:
          S[2*B_Index,2*A_Index]+=1
          S[2*B_Index+1,2*A_Index+1]+=1
          S[2*B_Index,2*A_Index+1]+=1
          S[2*B_Index+1,2*A_Index]+=1
        elif t_Astart<t_Bend:
          S[2*A_Index,2*B_Index+1]+=1
          if t_Bstart<t_Astart:
            S[2*B_Index,2*A_Index]+=1
          elif t_Astart<t_Bstart:
            S[2*A_Index,2*B_Index]+=1
          else:Q[2*A_Index,2*B_Index]+=1
          if t_Bend<t_Aend:S[2*B_Index+1,2*A_Index+1]+=1
          elif t_Aend<t_Bend:S[2*A_Index+1,2*B_Index+1]+=1
          else:Q[get_Q_index(2*A_Index+1,2*B_Index+1)]+=1
        else:
          S[2*B_Index,2*A_Index]+=1
          S[2*B_Index+1,2*A_Index+1]+=1
          S[2*B_Index,2*A_Index+1]+=1
          Q[get_Q_index(2*A_Index,2*B_Index+1)]+=1
      else:
        S[2*A_Index,2*B_Index]+=1
        S[2*A_Index+1,2*B_Index+1]+=1
        S[2*A_Index,2*B_Index+1]+=1
        Q[get_Q_index(2*A_Index+1,2*B_Index)]+=1
  global_model=np.zeros((dim,dim),dtype="i")
  for i,j in combinations(range(dim),2):
    if j-i==1 and i%2==0:
      global_model[i][j]=1
    else:
      denom=S[i][j]+S[j][i]+Q[get_Q_index(i,j)]
      if not denom==0:
        if S[i][j]/denom>threshold:
          global_model[i][j]=1
        if S[j][i]/denom>threshold:
          global_model[j][i]=1
#        global_model[i][j]+=(S[i][j]/denom)//threshold
#        global_model[j][i]+=(S[j][i]/denom)//threshold
  reformatted_traces=np.zeros((len(data),dim,dim),dtype="i")
  for comb_idx in range(len(combs)):
    for spanA,spanB in combs[comb_idx]:
      A_index,B_index=spanIDs.get(spanA.get("spanID")),spanIDs.get(spanB.get("spanID"))
      pos_edges=list(combinations([2*A_index,2*B_index,2*A_index+1,2*B_index+1],2))
      pos_edges+=[el[::-1] for el in pos_edges]
      for i,j in pos_edges:
        if global_model[i][j]>0:
          reformatted_traces[comb_idx][i][j]=1
  return S,Q,global_model,reformatted_traces

def get_Q_index(m,n):
  i,j=sorted((m,n))
  return (i-1)*(i-2)//2+j

test_json="""{
  "data": [
    {
      "traceID": "1",
      "spans": [
        {
          "traceID": "1",
          "spanID": "A1",
          "operationName": "DoA",
          "processID": "p1",
          "startTime": 1,
          "duration": 1000
        },
        {
          "traceID": "1",
          "spanID": "B1",
          "operationName": "DoB",
          "processID": "p1",
          "startTime": 10,
          "duration": 200
        },
        {
          "traceID": "1",
          "spanID": "C1",
          "operationName": "DoC",
          "processID": "p1",
          "startTime": 300,
          "duration": 600
        }
      ],
      "processes": {
      	"p1": {
      		"serviceName":"hello1"
      	}
      }
    },
    {
      "traceID": "2",
      "spans": [
        {
          "traceID": "2",
          "spanID": "B2",
          "operationName": "DoB",
          "processID": "p1",
          "startTime": 700,
          "duration": 600
        },
        {
          "traceID": "2",
          "spanID": "D2",
          "operationName": "DoD",
          "processID": "p1",
          "startTime": 500,
          "duration": 10000
        },
        {
          "traceID": "2",
          "spanID": "C2",
          "operationName": "DoC",
          "processID": "p1",
          "startTime": 503,
          "duration": 200
        }
      ],
      "processes": {
      	"p1": {
      		"serviceName":"hello1"
      	}
      }
    }
  ]
}"""

loaded=js.loads(test_json)["data"]
alged=alg(loaded,*preprocessing(loaded),threshold=0.95)
print("Global Causal Model: ")
print(alged[2])
print('')
print("Reformatted Traces: ")
print(alged[3])
