package kafka;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class WriteIntoFile {


		private static final String FILENAME = "C:\\Anant\\Java\\Example\\kafka\\offsetTimestamp.txt";

		//private static BufferedWriter bw;
		
		/*
		public static void main(String[] args) {

			try (BufferedWriter bw = new BufferedWriter(new FileWriter(FILENAME))) {

				String content = "This is the content to write into file\n";

				bw.write(content);

				// no need to close it.
				//bw.close();

				System.out.println("Done");

			} catch (IOException e) {

				e.printStackTrace();

			}

		}*/
		
		/*public WriteIntoFile(){

			try {
				
				//bw = new BufferedWriter(new FileWriter(FILENAME));
				
			//}
			//}(BufferedWriter bw = new BufferedWriter(new FileWriter(FILENAME))) {

			} catch (IOException e) {

				e.printStackTrace();

			}

			
			
		}
		*/
		
		
		void write_in_file(String content){
			
			try(BufferedWriter bw = new BufferedWriter(new FileWriter(FILENAME))) {
			
				bw.write(content);
			}
		
			catch (IOException e) {

				e.printStackTrace();

			}

		}
		
		

	}
	

