package MM1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class MMTool {
	public static int result_row = 0;
	public static int result_col = 0;
	public void findNM(String path) {
		File f = new File(path);
		File[] FolderList = f.listFiles();
		try {
			System.out.println("FolderList[0]: "+ FolderList[0].getPath());
			BufferedReader br0 = new BufferedReader(new FileReader(FolderList[0].getPath()));
            while(br0.readLine()!= null){
                ++result_row;
            }
            BufferedReader br1 = new BufferedReader(new FileReader(FolderList[1].getPath()));
            System.out.println("FolderList[1]: "+ FolderList[1].getPath());
            result_col = br1.readLine().split(" ").length;
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
