/*
Mystery Machine With Threshold - https://research.fb.com/wp-content/uploads/2016/11/the-mystery-machine-end-to-end-performance-analysis-of-large-scale-internet-services.pdf
Tanmay Gupta
9/15/21
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

// https://mvnrepository.com/artifact/com.googlecode.json-simple/json-simple
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class Final_Mystery_Machine_With_Threshold {
	
	static double threshold = 0.95; // User-defined Threshold
	
	public static void main(String[] args) throws IOException {
		Scanner s = null;
		try {

			// Reads JSON
			String content = "";
			try( BufferedReader br = new BufferedReader(new FileReader("json_test.in")) ) {
				String line = br.readLine();
				while(line != null) {
					content += line;
					line = br.readLine();
				}
			}

			// Parses JSON
			JSONObject jsonObject = (JSONObject) JSONValue.parse(content);
			JSONArray traces = (JSONArray) jsonObject.get("data");

			int numTraces = traces.size();
			Map<String, Integer> spanIndices = new HashMap<String, Integer>();
			Map<String, Integer> nameIDIndices = new HashMap<String, Integer>();

			// Preprocessing
			for(int i = 0; i < traces.size(); i++) {
				JSONObject curTrace = (JSONObject) traces.get(i);
				JSONArray curSpans = (JSONArray) curTrace.get("spans");
				JSONObject processesMap = (JSONObject) curTrace.get("processes");
				for(int j = 0; j < curSpans.size(); j++) {
					JSONObject jthSpan = (JSONObject) curSpans.get(j);
					String processID = (String) jthSpan.get("processID");
					JSONObject curProcess = (JSONObject) processesMap.get(processID);
					String processKey = (String) curProcess.get("serviceName");
					String curName = (String) jthSpan.get("operationName") + processKey;
					String curSpanID = (String) jthSpan.get("spanID");
					if(nameIDIndices.keySet().contains(curName)) {
						spanIndices.put(curSpanID, nameIDIndices.get(curName));
					}
					else {
						spanIndices.put((String) jthSpan.get("spanID"), nameIDIndices.keySet().size());
						nameIDIndices.put(curName, nameIDIndices.size());
					}
				} 
			}

			long[][] S = new long[nameIDIndices.keySet().size()*2][nameIDIndices.keySet().size()*2];
			long[] Q = new long[nameIDIndices.keySet().size()*(2*nameIDIndices.keySet().size()-1)];
			

			// Loop through traces and update happens-before graph
			for(int i=0; i<numTraces; i++) {
				JSONObject curTrace = (JSONObject) traces.get(i);
				JSONArray curSpans = (JSONArray) curTrace.get("spans");
				for(int j=0; j<curSpans.size()-1; j++) {
					JSONObject spanA = (JSONObject) curSpans.get(j);
					int A_Index = spanIndices.get(spanA.get("spanID"));
					long t_Astart = (long)spanA.get("startTime");
					long t_Aend  = (long)spanA.get("startTime") + (long)spanA.get("duration");
					for(int k=j+1; k<curSpans.size(); k++) {
						JSONObject spanB = (JSONObject) curSpans.get(k);
						int B_Index = spanIndices.get(spanB.get("spanID"));
						long t_Bstart = (long)spanB.get("startTime");
						long t_Bend = (long)spanB.get("startTime") + (long)spanB.get("duration");
						// Automatically known relationships:
						
						// A_End -> A_Start - Always violated
						// A_Start -> A_Start - Always violated
						// A_End -> A_End - Always violated
						// B_End -> B_Start - Always violated
						// B_Start -> B_Start - Always violated
						// B_End -> B_End - Always violated

						// A_Start -> A_End - Always true
						// B_Start -> B_End - Always true


						if (t_Aend < t_Bstart) {
							S[2*A_Index][2*B_Index]++;
							S[2*A_Index+1][2*B_Index+1]++;
							S[2*A_Index][2*B_Index+1]++;
							S[2*A_Index+1][2*B_Index]++;
						}
						else if (t_Bstart < t_Aend ) {
							S[2*B_Index][2*A_Index+1]++;
							if (t_Bend < t_Astart) {
								S[2*B_Index][2*A_Index]++;
								S[2*B_Index+1][2*A_Index+1]++;
								S[2*B_Index][2*A_Index+1]++;
								S[2*B_Index+1][2*A_Index]++;
							}
							else if (t_Astart < t_Bend) {
								S[2*A_Index][2*B_Index+1]++;
								if (t_Bstart < t_Astart) S[2*B_Index][2*A_Index]++;
								else if (t_Astart < t_Bstart) S[2*A_Index][2*B_Index]++;
								else Q[getQIndex(2*A_Index,2*B_Index)]++;

								if (t_Bend < t_Aend) S[2*B_Index+1][2*A_Index+1]++;
								else if (t_Aend < t_Bend) S[2*A_Index+1][2*B_Index+1]++;
								else Q[getQIndex(2*A_Index+1,2*B_Index+1)]++;
							}
							else {
								S[2*B_Index][2*A_Index]++;
								S[2*B_Index+1][2*A_Index+1]++;
								S[2*B_Index][2*A_Index+1]++;
								Q[getQIndex(2*A_Index, 2*B_Index+1)]++;
							}
						}
						else {
							S[2*A_Index][2*B_Index]++;
							S[2*A_Index+1][2*B_Index+1]++;
							S[2*A_Index][2*B_Index+1]++;
							Q[getQIndex(2*A_Index+1,2*B_Index)]++;
						}
					}
				}
			}

			// Global Causal Model generation with threshold application
			long[][] globalCausalModel = new long[nameIDIndices.keySet().size()*2][nameIDIndices.keySet().size()*2];
			for(int i=0; i<globalCausalModel.length; i++) { 
				for(int j=0; j<globalCausalModel.length; j++) {
					 if( S[i][j]!=0 && (double) S[i][j]/( S[i][j] + S[j][i] + Q[getQIndex(i, j)]) > threshold) globalCausalModel[i][j] = 1;
				}
			}
			
			for(int j=0; j<nameIDIndices.keySet().size(); j++) {
				globalCausalModel[2*j][2*j+1]= 1; // Start event happens before end event
			}
			

			// Reformatted arrays have same dimensions as Global Causal Model in order to not have to reindex the spanIDs to integers for each trace
			ArrayList<int[][]> reformattedTraces = new ArrayList<>();
			for(int i=0; i<numTraces; i++) {
				int[][] curReformattedTrace = new int[nameIDIndices.keySet().size()*2][nameIDIndices.keySet().size()*2];
				JSONObject curTrace = (JSONObject) traces.get(i);
				JSONArray curSpans = (JSONArray) curTrace.get("spans");
				for(int j=0; j<curSpans.size(); j++) {
					JSONObject spanA = (JSONObject) curSpans.get(j);
					int A_Index = spanIndices.get(spanA.get("spanID"));
					curReformattedTrace[2*A_Index][2*A_Index+1] = 1;  // Start event happen before end event
					for(int k=j+1; k<curSpans.size(); k++) {
						JSONObject spanB = (JSONObject) curSpans.get(k);
						int B_Index = spanIndices.get(spanB.get("spanID"));
						if(globalCausalModel[2*A_Index][2*B_Index]>0) curReformattedTrace[2*A_Index][2*B_Index] = 1; 
						if(globalCausalModel[2*A_Index+1][2*B_Index]>0) curReformattedTrace[2*A_Index+1][2*B_Index] = 1;
						if(globalCausalModel[2*A_Index][2*B_Index+1]>0) curReformattedTrace[2*A_Index][2*B_Index+1] = 1;
						if(globalCausalModel[2*A_Index+1][2*B_Index+1]>0) curReformattedTrace[2*A_Index+1][2*B_Index+1] = 1;
						if(globalCausalModel[2*B_Index][2*A_Index]>0) curReformattedTrace[2*B_Index][2*A_Index] = 1;
						if(globalCausalModel[2*B_Index+1][2*A_Index]>0) curReformattedTrace[2*B_Index+1][2*A_Index] = 1;
						if(globalCausalModel[2*B_Index][2*A_Index+1]>0) curReformattedTrace[2*B_Index][2*A_Index+1] = 1;
						if(globalCausalModel[2*B_Index+1][2*A_Index+1]>0) curReformattedTrace[2*B_Index+1][2*A_Index+1] = 1;
					}
				}
				reformattedTraces.add(curReformattedTrace);
			}
			
			System.out.println("Global Causal Model: \n" + Arrays.deepToString(globalCausalModel));
			System.out.println();
			System.out.println("Reformatted Traces:");
			for(int[][] rt : reformattedTraces) {
				System.out.println(Arrays.deepToString(rt));
			}
			
		} finally {
			if(!(s==null)) s.close();
		}
	}
	
	public static int getQIndex(int m, int n) {
		int i = Math.max(m,  n);
		return (i-1)*(i-2)/2 + Math.min(m,  n);
	}
}
