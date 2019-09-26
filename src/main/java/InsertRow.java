import org.apache.kudu.client.*;

public class InsertRow {

    public static void main(String[] args) {
        //master地址
        //String masterAddr = "192.168.0.118";
        String masterAddr = "192.168.0.118";

        KuduClient client = new KuduClient.KuduClientBuilder(masterAddr)
                .defaultSocketReadTimeoutMs(6000).build();

        try {
            KuduTable table = client.openTable("student");
            KuduSession kuduSession = client.newSession();

            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            kuduSession.setMutationBufferSpace(3000);
            for (int i = 0; i <10 ; i++) {
                Insert insert = table.newInsert();
                insert.getRow().addInt("id",i);
                insert.getRow().addString("name",i+"号");
                kuduSession.flush();
                kuduSession.apply(insert);
            }
            kuduSession.close();
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
