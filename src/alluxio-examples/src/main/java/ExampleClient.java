import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.util.ConfigurationUtils;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ExampleClient {
    private FileSystem fSystem;
    private String testFilePath;
    private static String confDir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
    private static String PATH_TO_ALLUXIO_SITE_PROPERTIES = confDir + "alluxio-site.properties";
    private static Properties alluxioConf;
    /**
     * init get a FileSystem instance
     * @throws java.io.IOException
     */
    private void init() throws IOException {
        //authentication();
        loadConf();
        instanceBuild();
    }

    /**
     * load configurations from alluxio-site.properties
     * @throws IOException
     */
    private void loadConf() throws IOException {
        InputStream fileInputStream = null;
        alluxioConf = new Properties();
        File propertiesFile = new File(PATH_TO_ALLUXIO_SITE_PROPERTIES);
        try {
            fileInputStream = new FileInputStream(propertiesFile);
            alluxioConf.load(fileInputStream);
        } catch (FileNotFoundException e) {
            System.out.println(PATH_TO_ALLUXIO_SITE_PROPERTIES + "does not exist. Exception: " + e);
        } catch (IOException e) {
            System.out.println("Failed to load configuration file. Exception: " + e);
        } finally{
            close(fileInputStream);
        }
    }

    /**
     * build Alluxio instance
     */
    private void instanceBuild() {
        // get filesystem
        InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
        conf.set(PropertyKey.MASTER_RPC_ADDRESSES, alluxioConf.get("alluxio.master.rpc.addresses"));
        FileSystemContext fsContext = FileSystemContext.create(conf);
        fSystem = FileSystem.Factory.create(fsContext);
    }

    /**
     * create file,write file
     */
    private void write() throws IOException {
        final String content = "hi, I am bigdata. It is successful if you can see me.";
        FileOutStream out = null;
        try {
            AlluxioURI path = new AlluxioURI(testFilePath);
            out = fSystem.createFile(path);
            out.write(content.getBytes());
        }catch (Exception e){
            System.out.println("Failed to write file. Exception:" + e);
        }finally {
            close(out);
        }
    }

    /**
     * read file
     * @throws java.io.IOException
     */
    private void read() throws IOException {
        AlluxioURI path = new AlluxioURI(testFilePath);
        FileInStream in = null;
        try{
            in = fSystem.openFile(path);
            byte[] buffer = new byte[1024];
            int len;
            String content = "";
            while((len = in.read(buffer)) != -1){
                String bufferStr = new String(buffer,0, len);
                content += bufferStr;
            }
            System.out.println(content);
        }catch (Exception e){
            System.out.println("Failed to read file. Exception:" + e);
        }finally {
            close(in);
        }
    }

    private void close(Closeable steam) throws IOException {
        if(steam != null){
            steam.close();
        }
    }

    public static void main(String []args) throws IOException {
        ExampleClient alluxio_client = new ExampleClient();
        alluxio_client.testFilePath = args[0];
        alluxio_client.init();
        alluxio_client.write();
        alluxio_client.read();
    }
}
