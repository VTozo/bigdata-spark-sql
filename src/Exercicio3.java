import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio3 {
    public static void main(String[] args) {

        // Imprime apenas prints e erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Cria tabela juntando todos os arquivos
        Dataset<Row> dataset = Join.joinTables();

        // Filtra apenas linhas com renda acima ou igual a 10000
        dataset = dataset.filter(col("ESTIMATIVARENDA").$greater$eq(10000));

        // Conta o total de linhas restantes
        long count = dataset.count();

        // Imprime o resultado
        System.out.println("Total: " + count);

    }
}
