import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio7 {
    public static void main(String[] args) {

        // Imprime apenas prints e erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Cria tabela juntando todos os arquivos
        Dataset<Row> dataset = Join.joinTables();

        // Filtra apenas linhas com distancias entre 0 e 5000 metros
        dataset = dataset.filter(col("DISTZONARISCO").$less(5000));
        dataset = dataset.filter(col("DISTZONARISCO").$greater$eq(0));

        //Filtra apenas linhas com renda maior que 7000,00 reais
        dataset = dataset.filter(col("ESTIMATIVARENDA").$greater(7000));

        // Conta o total de linhas restantes
        long count = dataset.count();

        // Imprime o resultado
        System.out.println("Total: " + count);

    }
}
