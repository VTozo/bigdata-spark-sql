import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

public class Exercicio1 {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);

        Dataset<Row> dataset = Join.joinTables();

        RelationalGroupedDataset orientacao_sexual = dataset.groupBy(col("ORIENTACAO_SEXUAL"));

        orientacao_sexual.count().show();
    }
}
