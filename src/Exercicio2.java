import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class Exercicio2 {
    public static void main(String[] args) {

        // Imprime apenas prints e erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Cria tabela juntando todos os arquivos
        Dataset<Row> dataset = Join.joinTables();

        // Remove linhas com QTDEMAIL -9999
        dataset = dataset.filter(col("QTDEMAIL").notEqual("-9999"));

        // Imprime o maior
        dataset.agg(max("QTDEMAIL")).show();

        // Imprime o menor
        dataset.agg(min("QTDEMAIL")).show();
    }
}
