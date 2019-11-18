import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio1 {
    public static void main(String[] args) {

        // Imprime apenas prints e erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Cria tabela juntando todos os arquivos
        Dataset<Row> dataset = Join.joinTables();

        // Agrupa por orientacao sexual
        RelationalGroupedDataset orientacao_sexual = dataset.groupBy(col("ORIENTACAO_SEXUAL"));

        // Conta o total de linhas por orientação sexual
        orientacao_sexual.count().show();
    }
}
