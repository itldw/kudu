import org.apache.kudu.client.*;

public class FindRow {

    public static void main(String[] args) {
        //master地址
        String masterAddr = "192.168.0.118";
        //String masterAddr = "192.168.0.122";
        KuduClient client = new KuduClient.KuduClientBuilder(masterAddr)
                .defaultSocketReadTimeoutMs(6000).build();

        try {
            KuduTable table = client.openTable("student");

            //创建scanner扫描
            KuduScanner scanner = client.newScannerBuilder(table).build();

            //遍历数据
            while(scanner.hasMoreRows()){
                RowResultIterator rowResults=scanner.nextRows();
                while (rowResults.hasNext()){
                    RowResult r=rowResults.next();
                    System.out.println("id:"+r.getInt("id")+"_name:"+r.getString("name"));
                }
            }
        } catch (KuduException e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }

    }

}
