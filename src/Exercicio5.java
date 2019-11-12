import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio5 {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);

        Dataset<Row> dataset = Join.joinTables();

        Dataset<Row> possui = dataset.filter(col("FUNCIONARIOPUBLICOCASA").$greater$eq(1));

        double porcentagem = possui.count() / (double) dataset.count() * 100;

        System.out.println("Porcentagem: " + porcentagem + "%");
    }
}
