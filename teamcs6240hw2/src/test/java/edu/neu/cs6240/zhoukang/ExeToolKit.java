package edu.neu.cs6240.zhoukang;

import java.io.*;

public class ExeToolKit {

	public static void main(String args[]) {

		try {
			Runtime rt = Runtime.getRuntime();
			// Process pr = rt.exec("cmd /c dir");
			String[] cmds=new String[]{"ls","-al"};
			Process pr = rt.exec(cmds);

			BufferedReader input = new BufferedReader(new InputStreamReader(
					pr.getInputStream()));

			String line = null;

			while ((line = input.readLine()) != null) {
				System.out.println(line);
			}

			int exitVal = pr.waitFor();
			System.out.println("Exited with error code " + exitVal);

		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}

}
