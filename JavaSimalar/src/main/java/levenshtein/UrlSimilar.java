package levenshtein;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

public class UrlSimilar {

    public static void main(String[] args) {
        String url = "www.tencent.com";
        String kFile = "url.txt";
        FileInputStream inputStream = null;
        Scanner sc = null;
        try {
            inputStream = new FileInputStream(kFile);
            sc = new Scanner(inputStream, "UTF-8");
            //我们将使用Java.util.Scanner类扫描文件的内容，一行一行连续地读取，允许对每一行进行处理，而不保持对它的引用。总之没有把它们存放在内存中：
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                if (line != null) {
                    Levenshtein.levenshtein(url,line);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (sc != null) {
                sc.close();
            }
        }
        System.out.println("The end!");
    }
}