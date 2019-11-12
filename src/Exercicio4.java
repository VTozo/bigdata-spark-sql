import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio4 {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);

        Dataset<Row> dataset = Join.joinTables();

        dataset = dataset.filter(col("BOLSAFAMILIACASA").equalTo(1));

        long count = dataset.count();

        System.out.println("Total: " + count);

    }
}
