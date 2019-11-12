import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

public class Exercicio2 {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);

        Dataset<Row> dataset = Join.joinTables();

        dataset = dataset.filter(col("QTDEMAIL").notEqual("-9999"));

        dataset.agg(max("QTDEMAIL")).show();
        dataset.agg(min("QTDEMAIL")).show();
    }
}
