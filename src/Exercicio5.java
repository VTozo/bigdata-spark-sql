import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class Exercicio5 {
    public static void main(String[] args) {

        // Imprime apenas prints e erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Cria tabela juntando todos os arquivos
        Dataset<Row> dataset = Join.joinTables();

        // Filtra apenas as linhas com pelo menos um funcionário público
        Dataset<Row> possui = dataset.filter(col("FUNCIONARIOPUBLICOCASA").$greater$eq(1));

        // Calcula a porcentagem em relação ao total
        double porcentagem = possui.count() / (double) dataset.count() * 100;

        // Imprime o resultado
        System.out.println("Porcentagem: " + porcentagem + "%");
    }
}
