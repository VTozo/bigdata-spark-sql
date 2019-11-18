import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio4 {
    public static void main(String[] args) {

        // Imprime apenas prints e erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Cria tabela juntando todos os arquivos
        Dataset<Row> dataset = Join.joinTables();

        // Filtra apenas as linhas com bolsa familia igual a 1 (True)
        dataset = dataset.filter(col("BOLSAFAMILIACASA").equalTo(1));

        // Conta o total de linhas restantes
        long count = dataset.count();

        // Imprime o resultado
        System.out.println("Total: " + count);

    }
}
