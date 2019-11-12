import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio6 {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);

        Dataset<Row> dataset = Join.joinTables();

        dataset = dataset.withColumn("IDHRANGE", dataset.col("IDHMUNICIPIO") / 10)


        dataset.show(10);
//        double porcentagem = possui.count() / (double) dataset.count() * 100;

//        System.out.println("Porcentagem: " + zeroDez + "%");
    }
}
