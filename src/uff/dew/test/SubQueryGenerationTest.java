package uff.dew.test;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import uff.dew.avp.AVPConst;
import uff.dew.avp.commons.Utilities;
import uff.dew.svp.Partitioner;
import uff.dew.svp.exceptions.PartitioningException;

public class SubQueryGenerationTest {

	public static void main(String args[]) {
		System.out.println("Generating subqueries in " + AVPConst.SUBQUERIES_FILE_PATH  + " ...");

		String outputFilePathName = AVPConst.SUBQUERIES_FILE_PATH + AVPConst.SUBQUERIES_FILE_NAME;
		String catalogPath = AVPConst.CATALOG_FILE_PATH+AVPConst.TPCH_CATALOG_FILE_NAME;
		//String catalogPath = AVPConst.CATALOG_FILE_PATH+AVPConst.XMARK_CATALOG_FILE_NAME;
		
		//String collectionName = "tpch"; //Para os casos em que a cole��o possui v�rios documentos com conte�do/estrutura diferentes
		String collectionName = "tpch";
		int clusterSize = 3;

		Partitioner partitioner = null;
		List<String> subqueries = null;

		try {
			FileWriter writer = null;
			BufferedWriter buffer = null;
			FileInputStream catalogStream = new FileInputStream(catalogPath);

			for(int i = 11; i < 12; i++) {

				String xquery = Utilities.getFileContent(AVPConst.QUERIES_FILE_PATH, "q" + i + ".xq");
				System.out.println("Q" + i + " = " + xquery);

				partitioner = new Partitioner(catalogStream);
				
				//gera��o de subqueries 
				//Luiz Matos - incluido m�todo que recebe tamb�m o nome da cole��o para os casos em que ela possui v�rios documentos com conte�do/estrutura diferentes
				long startTime = System.nanoTime();
				subqueries = partitioner.executePartitioning(xquery, clusterSize); 
				long elapsedTime = System.nanoTime() - startTime;

				System.out.println("Time elapsed: " + elapsedTime/1000000 + " ms");

				for (int j = 0; j < subqueries.size(); j++) {
					System.out.println(subqueries.get(j)+"\n");
					writer = new FileWriter(outputFilePathName + "_" + j + ".txt");
					buffer = new BufferedWriter(writer);
					buffer.write(subqueries.get(j));
					buffer.close();
				}

				xquery = "";
				writer.close();
			} //fim do for

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (PartitioningException e) {
			e.printStackTrace();
		}
	}
}
