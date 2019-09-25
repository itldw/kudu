
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.shaded.com.google.common.collect.ImmutableList;
import org.apache.kudu.client.shaded.com.google.common.collect.Lists;

import org.slf4j.LoggerFactory;

public class CreateTable {


    private static final String KUDU_MASTERS = "192.168.0.122";
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CreateTable.class);

    public static void main(String[] args) throws KuduException {

        KuduClient kuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        List<ColumnSchema> columnSchemas = Lists.newArrayList();
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
        Schema schema = new Schema(columnSchemas);
        ImmutableList<String> hashKeys = ImmutableList.of("id");
        CreateTableOptions cto = new CreateTableOptions();
        cto.addHashPartitions(hashKeys, 2);
        cto.setNumReplicas(1);
        kuduClient.createTable("student", schema, cto);
        kuduClient.close();

    }
}
