package ch.unine.zkpartitioned;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public abstract class Configuration {
	public static String zksConnectStrings;
	public static String zkAdminConnectString;
	public static int flatteningFactor;
	public static int reductionFactor;

    static{
		//zksConnectStrings = new ArrayList<String>();

		try {
			InputStream fstream = ClassLoader.getSystemResourceAsStream("zkpartitioned.config");
			
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

			String line;
			while ((line = br.readLine()) != null) {
				/* ignore comments */
				if (line.contains("#") == false) {
					/*if (line.contains("ZOOKEEPERS")) {
						String regex = "\\s*\\bZOOKEEPERS\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						line = line.replace("[", "");
						line = line.replace("]", "");
						
						StringTokenizer tokenizer = new StringTokenizer(line);
					    while (tokenizer.hasMoreTokens()) {
					    	zksConnectStrings.add(tokenizer.nextToken());
					    }
					} else*/ 
					if (line.contains("ZKADMIN")) {
						String regex = "\\s*\\bZKADMIN\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						zkAdminConnectString = line;
                    } else if (line.contains("ZOOKEEPERS")) {
                        String regex = "\\s*\\bZOOKEEPERS\\b\\s*";
                        line = line.replaceAll(regex, "");
                        line = line.replace("=", "");
                        zksConnectStrings = line;
                    } else if (line.contains("FLATTENING_FACTOR")) {
						String regex = "\\s*\\bFLATTENING_FACTOR\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						flatteningFactor = new Integer(line);
					} else if (line.contains("REDUCTION_FACTOR")) {
						String regex = "\\s*\\bREDUCTION_FACTOR\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						reductionFactor = new Integer(line);
					}
				}
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
